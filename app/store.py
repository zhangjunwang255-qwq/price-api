"""
app/store.py — 价格存储（内存缓存 + PostgreSQL 持久化）
只存原始 5 分钟数据，15min / 1hour 在查询时从 5min 数据聚合得出
"""
import os, threading, time, logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import psycopg2

from .config import DATABASE_URL, SYMBOLS, SAMPLE_INTERVAL_SEC, KEEP_RECORDS


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


class PriceStore:
    """
    内存缓存最新行情 + PostgreSQL 持久化原始 5 分钟数据
    """

    def __init__(self):
        self._lock     = threading.RLock()
        self._latest:  dict[str, dict] = {}
        self._prev:    dict[str, float] = {}
        self._symbols: list[str] = list(SYMBOLS)

        # 采样模式控制
        self._mode             = "竞标"
        self._interval         = 0           # 0=实时, 300=5分钟
        self._last_sample_time = 0.0

        # PostgreSQL
        self._conn: psycopg2.extensions.connection | None = None
        self._db_ok: bool = True

        self._init_db()

        # 后台写入线程（批量写 5min 数据）
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
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_lookup
                    ON price_history (symbol, dt DESC)
                """)
            conn.commit()
            log.info("PostgreSQL price_history 表就绪（原始 5 分钟数据）")
        except Exception as e:
            log.warning("PostgreSQL 初始化失败: %s", e)
            self._db_ok = False

    # ── 后台写入 ─────────────────────────────────────

    def _writer_loop(self):
        """每 2 秒批量写入 + 顺带超量清理"""
        while True:
            time.sleep(2.0)
            with self._write_lock:
                if not self._write_queue:
                    continue
                batch = list(self._write_queue)
                self._write_queue.clear()

            self._flush(batch)
            self._cleanup_if_needed()

    def _flush(self, batch: list[tuple]):
        if not batch or not self._db_ok:
            return
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(
                    cur,
                    """INSERT INTO price_history (symbol, price, volume, dt, created_at)
                       VALUES %s""",
                    [
                        (sym, price, vol, dt, datetime.now(timezone.utc))
                        for sym, price, vol, dt in batch
                    ],
                )
            conn.commit()
            log.debug("PG 写入 %d 条", len(batch))
        except Exception as e:
            log.warning("PG 写入失败: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass

    def _cleanup_if_needed(self):
        """超量时清理最老的 5min 记录"""
        if not self._db_ok:
            return
        try:
            conn = self._get_conn()
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
                        log.info(
                            "PG 清理 %s 超量 %d 条，剩余 %d 条",
                            sym, excess, count - excess,
                        )
                conn.commit()
        except Exception as e:
            log.warning("PG 清理失败: %s", e)

    # ── 写入（主线程调用）───────────────────────────────

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        now = time.time()

        # 日常模式：按 SAMPLE_INTERVAL_SEC 采样
        if self._interval > 0 and (now - self._last_sample_time) < self._interval:
            return

        with self._lock:
            if self._interval > 0 and (time.time() - self._last_sample_time) < self._interval:
                return
            self._last_sample_time = time.time()

            prev_price = self._prev.get(symbol)
            change     = None
            change_pct = None
            if prev_price is not None and prev_price != 0:
                change     = round(price - prev_price, 2)
                change_pct = round(change / prev_price * 100, 4)

            record = {
                "symbol":         symbol,
                "instrument_id":  instrument_id,
                "price":          _nan(price),
                "volume":         int(volume),
                "dt":             dt,
                "change":         change,
                "change_pct":     change_pct,
                "is_trading":     _is_trading_time(),
            }
            self._latest[symbol] = record
            self._prev[symbol]   = _nan(price)

            # 竞标模式 → 不落库
            if self._mode == "竞标":
                return

            # 日常模式 → 原始 5min 数据入队列
            dt_parsed = self._parse_dt(dt)
            with self._write_lock:
                self._write_queue.append((symbol, _nan(price), int(volume), dt_parsed))

    def _parse_dt(self, dt_str: str) -> datetime:
        if not dt_str:
            return datetime.now(timezone.utc)
        try:
            return datetime.strptime(dt_str[:23], "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return datetime.now(timezone.utc)

    # ── 模式控制 ──────────────────────────────────────

    def set_mode(self, mode: str) -> dict:
        if mode not in ("竞标", "日常"):
            return {"ok": False, "error": "无效模式，可用: 竞标, 日常"}
        with self._lock:
            self._mode = mode
            self._interval = 0 if mode == "竞标" else SAMPLE_INTERVAL_SEC
            self._last_sample_time = 0.0
            log.info("采样模式切换: %s (间隔 %d 秒)", mode, self._interval)
        return {"ok": True, "mode": self._mode, "interval": self._interval}

    @property
    def mode_info(self) -> dict:
        with self._lock:
            return {"mode": self._mode, "interval": self._interval}

    # ── 读取 ─────────────────────────────────────────

    @property
    def latest(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._latest)

    @property
    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._symbols)

    def get_history(self, symbol: str,
                    interval_: str = "5min",
                    limit: int = 200) -> list[dict]:
        """
        从 PG 读取历史数据，支持查询时聚合
        interval_: '5min' | '15min' | '1hour'
        5min:    直接返回原始数据
        15min:   每 3 条原始数据聚合为 1 条（3 × 5min = 15min）
        1hour:   每 12 条原始数据聚合为 1 条（12 × 5min = 60min）
        """
        if not self._db_ok:
            return []
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT symbol, price, volume, dt
                       FROM price_history
                       WHERE symbol = %s
                       ORDER BY dt DESC
                       LIMIT %s""",
                    (symbol, limit * 12),   # 多取些，聚合后数量才够
                )
                rows = list(cur.fetchall())

            if not rows:
                return []

            # 5min：直接返回
            if interval_ == "5min":
                return self._format_rows(rows, limit)

            # 聚合
            step = 3 if interval_ == "15min" else 12
            groups = [rows[i:i+step] for i in range(0, len(rows), step)]

            aggregated = []
            for group in groups:
                if not group:
                    continue
                prices  = [float(r[1]) for r in group if r[1] is not None]
                volumes = [int(r[2])   for r in group if r[2] is not None]
                if not prices:
                    continue
                last_row = group[0]  # 取最新那条的时间戳
                aggregated.append((
                    last_row[0],
                    round(sum(prices) / len(prices), 4),   # 平均价
                    max(volumes) if volumes else 0,         # 最大成交量
                    last_row[3],                            # 最新时间戳
                ))

            return self._format_rows(aggregated, limit)

        except Exception as e:
            log.warning("PG 查询失败: %s", e)
            return []

    def _format_rows(self, rows: list, limit: int) -> list[dict]:
        return [
            {
                "symbol":   r[0],
                "price":    float(r[1]),
                "volume":   int(r[2]),
                "datetime": r[3].strftime("%Y-%m-%d %H:%M:%S") if hasattr(r[3], 'strftime') else str(r[3]),
            }
            for r in reversed(rows[:limit])
        ]

    def close(self):
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
