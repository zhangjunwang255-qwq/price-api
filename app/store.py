"""
app/store.py — 价格存储（内存缓存 + PostgreSQL 持久化）
日常模式：存储对齐到固定5分钟时间槽，查询时按固定时间槽返回
"""
import os, threading, time, logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import psycopg2

from .config import DATABASE_URL, SYMBOLS, SAMPLE_INTERVAL_SEC, KEEP_DAYS, DEFAULT_MODE


log = logging.getLogger("price-store")


def _is_trading_time() -> bool:
    """判断当前是否处于贵金属期货交易时段（GFEX，白盘 + 夜盘）"""
    now = datetime.now()
    h = now.hour
    t = h * 60 + now.minute
    if h >= 21 or h < 15:
        return True
    if (540 <= t < 615) or (630 <= t < 690) or (810 <= t < 900):
        return True
    return False


def _nan(v: float) -> float:
    return 0.0 if v != v else v


def _align_to_5min(dt: datetime) -> datetime:
    """对齐到5分钟时间槽（向下取整）"""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def _generate_fixed_slots(end_dt: datetime, interval_min: int, count: int) -> list[datetime]:
    """
    生成固定时间槽列表（从 end_dt 向前推）
    interval_min: 5, 15, 60
    """
    slots = []
    current = _align_to_5min(end_dt)
    # 对齐到 interval 的边界
    if interval_min == 15:
        # 对齐到 0, 15, 30, 45
        current = current.replace(minute=(current.minute // 15) * 15)
    elif interval_min == 60:
        # 对齐到整点
        current = current.replace(minute=0)

    for _ in range(count):
        slots.append(current)
        current -= timedelta(minutes=interval_min)

    return list(reversed(slots))


class PriceStore:
    """
    内存缓存最新行情 + PostgreSQL 持久化
    日常模式：存储对齐到5分钟时间槽
    """

    def __init__(self):
        self._lock     = threading.RLock()
        self._latest:  dict[str, dict] = {}
        self._prev:    dict[str, float] = {}
        self._symbols: list[str] = list(SYMBOLS)

        # 采样模式控制（Railway 重启后自动恢复为 DEFAULT_MODE）
        init_mode     = DEFAULT_MODE
        self._mode    = init_mode
        self._interval = 0 if init_mode == "竞标" else SAMPLE_INTERVAL_SEC
        self._last_sample_time: dict[str, float] = {}

        # PostgreSQL
        self._conn: psycopg2.extensions.connection | None = None
        self._db_ok: bool = True

        self._init_db()

        # 后台写入线程
        self._write_queue: list[tuple] = []
        self._write_lock   = threading.Lock()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

    # ── PostgreSQL ───────────────────────────────────

    def _get_conn(self) -> psycopg2.extensions.connection | None:
        if not self._db_ok:
            return None
        if self._conn is None or self._conn.closed:
            try:
                self._conn = psycopg2.connect(DATABASE_URL)
                log.info("PostgreSQL 连接已建立")
            except Exception as e:
                log.warning("PostgreSQL 连接失败: %s", e)
                self._db_ok = False
                return None
        return self._conn

    def _init_db(self):
        if not DATABASE_URL:
            log.warning("未设置 DATABASE_URL，降级为纯内存模式")
            self._db_ok = False
            return

        conn = self._get_conn()
        if conn is None:
            self._db_ok = False
            return

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS price_history (
                        id         BIGSERIAL PRIMARY KEY,
                        symbol     TEXT        NOT NULL,
                        price      NUMERIC(16,4) NOT NULL,
                        volume     BIGINT      NOT NULL,
                        dt         TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE(symbol, dt)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_lookup
                    ON price_history (symbol, dt DESC)
                """)
            conn.commit()
            log.info("PostgreSQL price_history 表就绪")
        except Exception as e:
            log.warning("PostgreSQL 初始化失败: %s", e)
            self._db_ok = False

    # ── 后台写入 ─────────────────────────────────────

    def _writer_loop(self):
        while True:
            time.sleep(2)
            self._flush()
            self._cleanup()

    def _flush(self):
        if not self._db_ok:
            return
        with self._write_lock:
            batch = self._write_queue
            self._write_queue = []   # 保留引用，连接失败时不清空
        if not batch:
            return
        try:
            conn = self._get_conn()
            if conn is None:
                log.warning("PG 连接失败，%d 条数据暂存队列等待重试", len(batch))
                return   # 连接失败，队列保留，下次重试
            with conn.cursor() as cur:
                # UPSERT: 同一时间槽只保留最新值
                cur.executemany(
                    """
                    INSERT INTO price_history (symbol, price, volume, dt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, dt) DO UPDATE
                    SET price = EXCLUDED.price, volume = EXCLUDED.volume
                    """,
                    batch,
                )
            conn.commit()
            self._flush_ok += len(batch)
        except Exception as e:
            log.warning("PG 批量写入失败: %s", e)
            self._flush_fail += len(batch)

    def _cleanup(self):
        """删除 KEEP_DAYS 天之前的数据"""
        if not self._db_ok:
            return
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM price_history WHERE dt < NOW() - INTERVAL '%s days'",
                    (KEEP_DAYS,),
                )
                deleted = cur.rowcount
                if deleted > 0:
                    log.info("PG 清理 %d 条过期数据（>%d天前）", deleted, KEEP_DAYS)
                conn.commit()
        except Exception as e:
            log.warning("PG 清理失败: %s", e)

    # ── 写入（主线程调用）───────────────────────────────

    # 调试计数器（部署后可删）
    _update_count = 0
    _sample_count = 0
    _flush_ok = 0
    _flush_fail = 0

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        self._update_count += 1

        now = time.time()
        last = self._last_sample_time.get(symbol, 0.0)

        if self._interval > 0 and (now - last) < self._interval:
            return

        with self._lock:
            last = self._last_sample_time.get(symbol, 0.0)
            if self._interval > 0 and (time.time() - last) < self._interval:
                return
            self._last_sample_time[symbol] = time.time()

            prev_price = self._prev.get(symbol)
            change = None
            change_pct = None
            if prev_price is not None and prev_price != 0:
                change = round(price - prev_price, 2)
                change_pct = round(change / prev_price * 100, 4)

            record = {
                "symbol": symbol,
                "instrument_id": instrument_id,
                "price": _nan(price),
                "volume": int(volume),
                "dt": dt,
                "change": change,
                "change_pct": change_pct,
                "is_trading": _is_trading_time(),
            }
            self._latest[symbol] = record
            self._prev[symbol] = _nan(price)

            if self._mode == "竞标":
                return

            # 日常模式：对齐到5分钟时间槽
            dt_parsed = self._parse_dt(dt)
            aligned_dt = _align_to_5min(dt_parsed)

            with self._write_lock:
                self._write_queue.append((symbol, _nan(price), int(volume), aligned_dt))
            self._sample_count += 1

    def _parse_dt(self, dt_str: str) -> datetime:
        if not dt_str:
            return datetime.now(timezone.utc)
        try:
            return datetime.strptime(dt_str[:23], "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            try:
                return datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
            except Exception:
                return datetime.now(timezone.utc)

    # ── 模式控制 ──────────────────────────────────────

    @property
    def mode_info(self) -> dict:
        """获取当前模式信息"""
        with self._lock:
            return {
                "mode": self._mode,
                "interval": self._interval,
            }

    def set_mode(self, mode: str) -> dict:
        if mode not in ("竞标", "日常"):
            return {"ok": False, "error": "无效模式"}
        with self._lock:
            self._mode = mode
            self._interval = 0 if mode == "竞标" else SAMPLE_INTERVAL_SEC
            self._last_sample_time = {}
            log.info("采样模式切换: %s", mode)
        return {"ok": True, "mode": self._mode, "interval": self._interval}

    # ── 查询 ──────────────────────────────────────────

    @property
    def latest(self) -> dict[str, dict]:
        with self._lock:
            return self._latest.copy()

    @property
    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._symbols)

    def get_history(self, symbol: str,
                    interval_: str = "5min",
                    limit: int = 200) -> list[dict]:
        """
        按固定时间槽查询历史数据
        interval_: '5min' | '15min' | '1hour'
        返回固定时间槽，找最接近的数据填充
        """
        if not self._db_ok:
            return []

        interval_min = {"5min": 5, "15min": 15, "1hour": 60}.get(interval_, 5)

        try:
            conn = self._get_conn()
            if conn is None:
                return []

            # 生成固定时间槽（从当前时间向前推）
            now = datetime.now(timezone.utc)
            slots = _generate_fixed_slots(now, interval_min, limit)

            if not slots:
                return []

            # 查询该品种的所有数据（用于填充）
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT price, dt FROM price_history
                       WHERE symbol = %s AND dt >= %s
                       ORDER BY dt ASC""",
                    (symbol, slots[0] - timedelta(minutes=interval_min))
                )
                db_rows = cur.fetchall()

            # 按时间槽填充数据
            result = []
            db_idx = 0
            last_price = None

            for slot in slots:
                # 找最接近该时间槽的数据
                best_price = None
                best_diff = timedelta.max

                while db_idx < len(db_rows):
                    price, dt = db_rows[db_idx]
                    diff = abs(dt - slot)
                    if diff < best_diff and diff <= timedelta(minutes=interval_min / 2):
                        best_diff = diff
                        best_price = float(price)
                        last_price = best_price
                    elif dt > slot + timedelta(minutes=interval_min / 2):
                        break
                    db_idx += 1

                if best_price is None:
                    best_price = last_price

                if best_price is not None:
                    result.append({
                        "datetime": slot.isoformat(),
                        "price": best_price,
                        "symbol": symbol,
                    })

            log.info("%s %s 生成 %d 个固定时间槽", symbol, interval_, len(result))
            return result

        except Exception as e:
            log.warning("查询历史数据失败: %s", e)
            return []

    def debug_slot_matching(self, symbol: str,
                           interval_: str = "5min",
                           limit: int = 20) -> dict:
        """诊断接口：返回每个固定时间槽及匹配详情"""
        interval_map = {"5min": 5, "15min": 15, "1hour": 60}
        interval_min = interval_map.get(interval_, 5)

        if not self._db_ok:
            return {"error": "数据库未连接"}

        try:
            conn = self._get_conn()
            if conn is None:
                return {"error": "数据库连接失败"}

            now = datetime.now(timezone.utc)
            slots = _generate_fixed_slots(now, interval_min, limit)

            # 查询该品种所有原始数据
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, price, dt, created_at
                    FROM price_history
                    WHERE symbol = %s
                    ORDER BY dt ASC
                """, (symbol,))
                raw_rows = cur.fetchall()

            all_prices = [float(r[1]) for r in raw_rows]
            all_dts = [r[2] for r in raw_rows]

            slot_results = []
            matched = 0
            unmatched = 0

            for slot in slots:
                best_idx = -1
                best_diff = timedelta.max

                for i, dt in enumerate(all_dts):
                    diff = abs(dt - slot)
                    if diff < best_diff and diff <= timedelta(minutes=interval_min / 2):
                        best_diff = diff
                        best_idx = i

                if best_idx >= 0:
                    diff_min = best_diff.total_seconds() / 60
                    slot_results.append({
                        "slot": slot.isoformat(),
                        "matched": True,
                        "diff_minutes": round(diff_min, 1),
                        "matched_price": all_prices[best_idx],
                        "matched_dt": all_dts[best_idx].isoformat(),
                        "raw_id": raw_rows[best_idx][0],
                    })
                    matched += 1
                else:
                    slot_results.append({
                        "slot": slot.isoformat(),
                        "matched": False,
                        "diff_minutes": None,
                        "matched_price": None,
                        "matched_dt": None,
                        "raw_id": None,
                    })
                    unmatched += 1

            return {
                "symbol": symbol,
                "interval": interval_,
                "interval_min": interval_min,
                "total_slots": len(slots),
                "matched": matched,
                "unmatched": unmatched,
                "raw_data_count": len(raw_rows),
                "slots": slot_results,
            }

        except Exception as e:
            log.warning("debug_slot_matching 失败: %s", e)
            return {"error": str(e)}
