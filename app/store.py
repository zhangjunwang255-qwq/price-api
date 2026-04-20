"""
app/store.py — 价格存储（内存缓存 + PostgreSQL 持久化）
日常模式：存储对齐到固定5分钟时间槽，查询时按固定时间槽返回

修复记录（887eb69 基础上）：
- 修复 update() 双重 interval 检查导致日常模式采样永不触发的 bug
- 改用 psycopg2.ThreadedConnectionPool（每操作借/还，线程安全）
- writer_loop 顶层 try/except 防止线程崩溃
- 采样/flush 计数器改为实例变量（类变量会导致跨实例共享问题）
"""
import os, threading, time, logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from contextlib import suppress

import psycopg2
from psycopg2 import pool as pg_pool

from .config import DATABASE_URL, SYMBOLS, SAMPLE_INTERVAL_SEC, KEEP_RECORDS, DEFAULT_MODE


log = logging.getLogger("price-store")


# ────────────────────────────────────────────────────
#  工具函数
# ────────────────────────────────────────────────────
def _is_trading_time() -> bool:
    now = datetime.now()
    h, t = now.hour, now.hour * 60 + now.minute
    if h >= 21 or h < 15:
        return True
    if (540 <= t < 615) or (630 <= t < 690) or (810 <= t < 900):
        return True
    return False


def _nan(v: float) -> float:
    return 0.0 if v != v else v


def _align_to_5min(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def _generate_fixed_slots(end_dt: datetime, interval_min: int, count: int):
    slots = []
    current = _align_to_5min(end_dt)
    if interval_min == 15:
        current = current.replace(minute=(current.minute // 15) * 15)
    elif interval_min == 60:
        current = current.replace(minute=0)
    for _ in range(count):
        slots.append(current)
        current -= timedelta(minutes=interval_min)
    return list(reversed(slots))


# ────────────────────────────────────────────────────
#  单例守卫（防止模块被重复 import 创建多个实例）
# ────────────────────────────────────────────────────
_store_instance: "PriceStore | None" = None


# ────────────────────────────────────────────────────
#  PriceStore
# ────────────────────────────────────────────────────
class PriceStore:

    def __init__(self):
        global _store_instance
        if _store_instance is not None:
            log.warning("PriceStore 被重复创建，返回已有实例")
            self.__dict__.update(_store_instance.__dict__)
            return
        _store_instance = self

        self._lock: dict = {}
        self._lock["main"] = threading.RLock()
        self._latest:  dict[str, dict] = {}
        self._prev:    dict[str, float] = {}
        self._symbols: list[str] = list(SYMBOLS)

        # 采样模式
        self._mode    = DEFAULT_MODE
        self._interval = 0 if DEFAULT_MODE == "竞标" else SAMPLE_INTERVAL_SEC
        self._last_sample_time: dict[str, float] = {}

        # PostgreSQL 连接池
        self._cp: pg_pool.ThreadedConnectionPool | None = None
        self._db_ok: bool = True
        self._init_db()

        # 写入队列
        self._write_queue: list[tuple] = []
        self._write_lock   = threading.Lock()

        # 后台 writer 线程
        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name="writer-loop"
        )
        self._writer_thread.start()

        # 统计计数器（实例变量，不是类变量）
        self._update_count = 0
        self._sample_count = 0
        self._flush_ok     = 0
        self._flush_fail   = 0

        log.info("PriceStore 初始化完成，模式=%s，PG=%s", self._mode, self._db_ok)

    # ── PG 连接池 ──────────────────────────────────

    def _pg_get(self):
        """从连接池获取连接，失败返回 None"""
        if not self._db_ok or self._cp is None:
            return None
        try:
            return self._cp.getconn()
        except Exception as e:
            log.warning("从连接池获取连接失败: %s", e)
            return None

    def _pg_put(self, conn):
        """归还连接到连接池"""
        if conn and self._cp:
            with suppress(Exception):
                self._cp.putconn(conn)

    def _init_db(self) -> bool:
        if not DATABASE_URL:
            log.warning("DATABASE_URL 未设置，降级为纯内存模式")
            self._db_ok = False
            return False
        try:
            self._cp = pg_pool.ThreadedConnectionPool(
                minconn=1, maxconn=6, dsn=DATABASE_URL, connect_timeout=5,
            )
            conn = self._cp.getconn()
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
            finally:
                self._cp.putconn(conn)
            self._db_ok = True
            log.info("PostgreSQL 连接池建立，表就绪")
            return True
        except Exception as e:
            log.warning("PostgreSQL 初始化失败: %s", e)
            self._db_ok = False
            return False

    # ── 后台 writer ─────────────────────────────────

    def _writer_loop(self):
        while True:
            time.sleep(2)
            try:
                self._flush()
                self._cleanup()
            except Exception as e:
                log.error("writer_loop 异常: %s", e, exc_info=True)
                time.sleep(5)

    def _flush(self):
        if not self._db_ok:
            return
        with self._write_lock:
            batch = self._write_queue
            self._write_queue = []
        if not batch:
            return
        conn = self._pg_get()
        if conn is None:
            log.warning("PG 连接失败，%d 条数据暂存等待重试", len(batch))
            with self._write_lock:
                self._write_queue = batch + self._write_queue
            return
        try:
            with conn.cursor() as cur:
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
            log.info("PG flush 成功: %d 条", len(batch))
        except Exception as e:
            log.warning("PG 批量写入失败: %s", e)
            with suppress(Exception):
                conn.rollback()
            with self._write_lock:
                self._write_queue = batch + self._write_queue
            self._flush_fail += len(batch)
        finally:
            self._pg_put(conn)

    def _cleanup(self):
        if not self._db_ok:
            return
        conn = self._pg_get()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                for sym in self._symbols:
                    cur.execute(
                        "SELECT COUNT(*) FROM price_history WHERE symbol=%s",
                        (sym,),
                    )
                    count = cur.fetchone()[0]
                    if count > KEEP_RECORDS:
                        excess = count - KEEP_RECORDS
                        cur.execute(
                            """DELETE FROM price_history
                               WHERE id IN (
                                   SELECT id FROM price_history
                                   WHERE symbol=%s
                                   ORDER BY dt ASC
                                   LIMIT %s
                               )""",
                            (sym, excess),
                        )
                        log.info("PG 清理 %s 超量 %d 条", sym, excess)
                conn.commit()
        except Exception as e:
            log.warning("PG 清理失败: %s", e)
        finally:
            self._pg_put(conn)

    # ── 行情更新 ──────────────────────────────────

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        self._update_count += 1

        # 竞标模式直接写内存，不采样
        if self._mode == "竞标":
            with self._lock["main"]:
                prev_price = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev_price, 2) if prev_price is not None else 0,
                    "is_trading": _is_trading_time(),
                }
            return

        # 日常模式：检查采样间隔（只在锁外检查一次）
        now = time.time()
        if self._interval > 0 and (now - self._last_sample_time.get(symbol, 0.0)) < self._interval:
            # 更新内存数据但不采样
            with self._lock["main"]:
                prev_price = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev_price, 2) if prev_price is not None else 0,
                    "is_trading": _is_trading_time(),
                }
            return

        # 需要采样
        with self._lock["main"]:
            # 二次确认（防止锁竞争导致重复采样）
            if self._interval > 0 and (time.time() - self._last_sample_time.get(symbol, 0.0)) < self._interval:
                prev_price = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev_price, 2) if prev_price is not None else 0,
                    "is_trading": _is_trading_time(),
                }
                return
            self._last_sample_time[symbol] = time.time()

            prev_price = self._prev.get(symbol)
            change = None
            if prev_price is not None and prev_price != 0:
                change = round(_nan(price) - prev_price, 2)

            self._latest[symbol] = {
                "symbol": symbol,
                "instrument_id": instrument_id,
                "price": _nan(price),
                "volume": int(volume),
                "dt": dt,
                "change": change,
                "is_trading": _is_trading_time(),
            }
            self._prev[symbol] = _nan(price)

        # 采样数据入队（锁外操作队列）
        dt_parsed = self._parse_dt(dt)
        aligned_dt = _align_to_5min(dt_parsed)
        with self._write_lock:
            self._write_queue.append((symbol, _nan(price), int(volume), aligned_dt))
        self._sample_count += 1

    def _parse_dt(self, dt_str: str) -> datetime:
        if not dt_str:
            return datetime.now(timezone.utc)
        try:
            return datetime.strptime(dt_str[:23], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
        except Exception:
            try:
                return datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                return datetime.now(timezone.utc)

    # ── 模式控制 ──────────────────────────────────

    @property
    def mode_info(self) -> dict:
        return {"mode": self._mode, "interval": self._interval}

    def set_mode(self, mode: str) -> dict:
        if mode not in ("竞标", "日常"):
            return {"ok": False, "error": "无效模式"}
        with self._lock["main"]:
            self._mode = mode
            self._interval = 0 if mode == "竞标" else SAMPLE_INTERVAL_SEC
            self._last_sample_time = {}
        log.info("采样模式切换: %s", mode)
        return {"ok": True, "mode": self._mode, "interval": self._interval}

    # ── 查询 ──────────────────────────────────────

    @property
    def latest(self) -> dict[str, dict]:
        with self._lock["main"]:
            return dict(self._latest)

    @property
    def symbols(self) -> list[str]:
        return list(self._symbols)

    def get_history(self, symbol: str,
                    interval_: str = "5min",
                    limit: int = 200) -> list[dict]:
        if not self._db_ok:
            return []
        interval_min = {"5min": 5, "15min": 15, "1hour": 60}.get(interval_, 5)
        try:
            conn = self._pg_get()
            if conn is None:
                return []
            now = datetime.now(timezone.utc)
            slots = _generate_fixed_slots(now, interval_min, limit)
            if not slots:
                self._pg_put(conn)
                return []
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT price, dt FROM price_history
                       WHERE symbol = %s AND dt >= %s
                       ORDER BY dt ASC""",
                    (symbol, slots[0] - timedelta(minutes=interval_min)),
                )
                db_rows = cur.fetchall()
            self._pg_put(conn)

            result = []
            db_idx = 0
            last_price = None
            for slot in slots:
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
                    result.append({"datetime": slot.isoformat(), "price": best_price, "symbol": symbol})
            return result
        except Exception as e:
            log.warning("查询历史数据失败: %s", e)
            return []

    def debug_slot_matching(self, symbol: str,
                           interval_: str = "5min",
                           limit: int = 20) -> dict:
        interval_min = {"5min": 5, "15min": 15, "1hour": 60}.get(interval_, 5)
        if not self._db_ok:
            return {"error": "数据库未连接"}
        try:
            conn = self._pg_get()
            if conn is None:
                return {"error": "数据库连接失败"}
            now = datetime.now(timezone.utc)
            slots = _generate_fixed_slots(now, interval_min, limit)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, price, dt, created_at FROM price_history WHERE symbol=%s ORDER BY dt ASC",
                    (symbol,),
                )
                raw_rows = cur.fetchall()
            self._pg_put(conn)
            all_prices = [float(r[1]) for r in raw_rows]
            all_dts = [r[2] for r in raw_rows]
            slot_results = []
            for slot in slots:
                best_idx, best_diff = -1, timedelta.max
                for i, dt in enumerate(all_dts):
                    diff = abs(dt - slot)
                    if diff < best_diff and diff <= timedelta(minutes=interval_min / 2):
                        best_diff = diff
                        best_idx = i
                if best_idx >= 0:
                    slot_results.append({
                        "slot": slot.isoformat(), "matched": True,
                        "diff_minutes": round(best_diff.total_seconds() / 60, 1),
                        "matched_price": all_prices[best_idx],
                        "matched_dt": all_dts[best_idx].isoformat(),
                    })
                else:
                    slot_results.append({"slot": slot.isoformat(), "matched": False})
            return {
                "symbol": symbol, "interval": interval_, "raw_data_count": len(raw_rows),
                "slots": slot_results,
            }
        except Exception as e:
            log.warning("debug_slot_matching 失败: %s", e)
            return {"error": str(e)}
