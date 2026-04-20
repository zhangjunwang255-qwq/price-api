"""
app/store.py — 价格存储（内存缓存 + PostgreSQL 持久化）

核心修复：
- update() 采样 bug（双重 interval 检查）
- PG 单连接 → ThreadedConnectionPool
- writer_loop 顶层 try/except
- 统计计数器改为实例变量
- 单例守卫用 __new__
- 时间轴不再依赖数据库时间戳，按固定交易时段规则生成
- 数据库保留 30 日数据（KEEP_DAYS=30）
"""
import os, threading, time, logging
from datetime import datetime, timezone, timedelta
from contextlib import suppress

import psycopg2
from psycopg2 import pool as pg_pool

from .config import DATABASE_URL, SYMBOLS, SAMPLE_INTERVAL_SEC, KEEP_DAYS, DEFAULT_MODE


log = logging.getLogger("price-store")

# ────────────────────────────────────────────────────
#  广期所贵金属交易时段（写死，不依赖数据库时间戳）
# ────────────────────────────────────────────────────
# 白盘：09:00-10:15(10:00起), 10:30-11:30, 13:30-15:00
# 夜盘：21:00-次日 02:30（周一～四）/ 21:00-次日 01:00（周日）
# 5min 槽：10:00 / 10:05 / … / 15:00（共 61 格）
# 15min 槽：10:00 / 10:15 / … / 15:00（共 21 格）
# 1hour  槽：10:00 / 11:00 / … / 15:00（共 6 格）
#
# 图表只展示"今天"（白盘）或"昨夜"（夜盘），即最近一个完整交易时段。
# 查询时用固定时间轴，数据库只提供价格填充，不提供时间。

DAY_SESSION_START = 10  # 白盘起始整点（小时）
DAY_SESSION_END   = 15  # 白盘结束（不含，当日15:00截止）
DAY_FIRST_SLOT    = 10  # 5min 第一个槽

NIGHT_START = 21  # 夜盘起始（小时）
# 夜盘结束：周一～四=02:30（次日），周日=01:00（次日）


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


def _today_day_session_slots(interval_min: int) -> list[datetime]:
    """
    生成今天白盘所有固定时间槽（从 10:00 到 15:00）
    interval_min: 5 | 15 | 60
    """
    now = datetime.now(timezone.utc).astimezone()
    today = now.date()
    slots = []
    current = datetime(today.year, today.month, today.day,
                       DAY_FIRST_SLOT, 0, 0)
    end = datetime(today.year, today.month, today.day,
                   DAY_SESSION_END, 0, 0)
    while current < end:
        slots.append(current)
        current += timedelta(minutes=interval_min)
    return slots


def _last_night_session_slots(interval_min: int) -> list[datetime]:
    """
    生成昨夜（上一个非交易日）夜盘所有固定时间槽（从 21:00 到次日凌晨）
    interval_min: 5 | 15 | 60
    """
    now = datetime.now(timezone.utc).astimezone()
    today = now.date()

    # 找到上一个有夜盘的日期
    # 周一 → 上周五，周二～五 → 前一天，周日 → 上周五
    weekday = today.weekday()
    if weekday == 0:       # 周一
        prev_night = today - timedelta(days=3)
    elif weekday == 6:     # 周日
        prev_night = today - timedelta(days=2)
    else:
        prev_night = today - timedelta(days=1)

    slots = []
    # 夜盘从 prev_night 当天 21:00 开始
    current = datetime(prev_night.year, prev_night.month, prev_night.day,
                       NIGHT_START, 0, 0)
    # 夜盘结束时间（周一～四 次日02:30，周日 次日01:00）
    if weekday == 6:       # 周日
        end_hour, end_min = 1, 0   # 次日 01:00
    else:
        end_hour, end_min = 2, 30  # 次日 02:30
    end = datetime(prev_night.year, prev_night.month, prev_night.day,
                   end_hour, end_min, 0)
    end += timedelta(days=1)

    while current <= end:
        slots.append(current)
        current += timedelta(minutes=interval_min)
    return slots


def _current_session_slots(interval_min: int) -> list[datetime]:
    """返回当前交易日（今天白盘或昨夜夜盘）的所有固定时间槽"""
    now = datetime.now(timezone.utc).astimezone()
    h = now.hour

    if h >= NIGHT_START or h < 15:
        # 夜间段：可能显示昨夜或今天白盘（取决于时间）
        if h >= NIGHT_START:
            # 21点后：显示今天白盘（若已结束）或昨夜（若白盘未开始）
            if h >= 15 and h < NIGHT_START:
                return _last_night_session_slots(interval_min)
            return _today_day_session_slots(interval_min)
        # 凌晨～14:59：显示昨夜
        return _last_night_session_slots(interval_min)
    else:
        # 15:00～20:59：显示今天白盘
        return _today_day_session_slots(interval_min)


# ────────────────────────────────────────────────────
#  PriceStore
# ────────────────────────────────────────────────────
class PriceStore:

    _instance: "PriceStore | None" = None

    def __new__(cls):
        if cls._instance is not None:
            return cls._instance
        return super().__new__(cls)

    def __init__(self):
        if PriceStore._instance is not None:
            return
        PriceStore._instance = self

        self._lock: dict = {}
        self._lock["main"] = threading.RLock()
        self._latest: dict[str, dict] = {}
        self._prev:   dict[str, float] = {}
        self._symbols: list[str] = list(SYMBOLS)

        self._mode     = DEFAULT_MODE
        self._interval = 0 if DEFAULT_MODE == "竞标" else SAMPLE_INTERVAL_SEC
        self._last_sample_time: dict[str, float] = {}

        # PostgreSQL 连接池
        self._cp: pg_pool.ThreadedConnectionPool | None = None
        self._db_ok: bool = True
        self._init_db()

        self._write_queue: list[tuple] = []
        self._write_lock   = threading.Lock()

        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name="writer-loop"
        )
        self._writer_thread.start()

        # 统计（实例变量）
        self._update_count = 0
        self._sample_count = 0
        self._flush_ok     = 0
        self._flush_fail   = 0

        log.info("PriceStore 初始化，模式=%s，PG=%s", self._mode, self._db_ok)

    # ── PG 连接池 ─────────────────────────────────

    def _pg_get(self):
        if not self._db_ok or self._cp is None:
            return None
        try:
            return self._cp.getconn()
        except Exception as e:
            log.warning("从连接池获取连接失败: %s", e)
            return None

    def _pg_put(self, conn):
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

    # ── 后台 writer ───────────────────────────────

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
            log.warning("PG 连接失败，%d 条暂存等待重试", len(batch))
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
                        """DELETE FROM price_history
                           WHERE dt < NOW() - INTERVAL '%d days'""",
                        (KEEP_DAYS,),
                    )
                    log.info("PG 清理 %s 过期数据", sym)
                conn.commit()
        except Exception as e:
            log.warning("PG 清理失败: %s", e)
        finally:
            self._pg_put(conn)

    # ── 行情更新 ─────────────────────────────────

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        self._update_count += 1

        if self._mode == "竞标":
            with self._lock["main"]:
                prev = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev, 2) if prev is not None else 0,
                    "is_trading": _is_trading_time(),
                }
            return

        now = time.time()
        if self._interval > 0 and (now - self._last_sample_time.get(symbol, 0.0)) < self._interval:
            with self._lock["main"]:
                prev = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev, 2) if prev is not None else 0,
                    "is_trading": _is_trading_time(),
                }
            return

        with self._lock["main"]:
            if self._interval > 0 and (time.time() - self._last_sample_time.get(symbol, 0.0)) < self._interval:
                prev = self._prev.get(symbol)
                self._prev[symbol] = _nan(price)
                self._latest[symbol] = {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "price": _nan(price),
                    "volume": int(volume),
                    "dt": dt,
                    "change": round(_nan(price) - prev, 2) if prev is not None else 0,
                    "is_trading": _is_trading_time(),
                }
                return
            self._last_sample_time[symbol] = time.time()

            prev = self._prev.get(symbol)
            change = round(_nan(price) - prev, 2) if prev is not None else None
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

        dt_parsed = self._parse_dt(dt)
        aligned = _align_to_5min(dt_parsed)
        with self._write_lock:
            self._write_queue.append((symbol, _nan(price), int(volume), aligned))
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

    # ── 模式控制 ─────────────────────────────────

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
        return {"ok": True, "mode": self._mode, "interval": self._interval}

    # ── 查询 ─────────────────────────────────────

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
        """
        按固定交易时段时间轴返回数据。
        时间轴与数据库时间戳无关：
          - 5min：10:00→15:00，每5分钟一格
          - 15min：10:00→15:00，每15分钟一格
          - 1hour：10:00→15:00，每小时一格
        每个槽用数据库中离该整点最近的价格填充。
        """
        interval_map = {"5min": 5, "15min": 15, "1hour": 60}
        interval_min = interval_map.get(interval_, 5)

        # 固定时间轴（写死）
        slots = _current_session_slots(interval_min)

        # 截取到 limit
        if len(slots) > limit:
            slots = slots[-limit:]

        if not self._db_ok or not slots:
            return [{"datetime": s.isoformat(), "price": None, "symbol": symbol} for s in slots]

        # 从数据库获取该品种最近30日数据（用于填充）
        conn = self._pg_get()
        if conn is None:
            return [{"datetime": s.isoformat(), "price": None, "symbol": symbol} for s in slots]

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT price, dt FROM price_history
                       WHERE symbol = %s
                       ORDER BY dt ASC""",
                    (symbol,),
                )
                rows = cur.fetchall()
        finally:
            self._pg_put(conn)

        # 用数据库数据填充固定时间轴
        # 找每个槽最近的记录
        result = []
        last_price = None

        for slot in slots:
            best_price = None
            best_diff  = timedelta.max

            for price, dt in rows:
                diff = abs(dt.replace(tzinfo=None) - slot.replace(tzinfo=None))
                if diff < best_diff and diff <= timedelta(minutes=interval_min):
                    best_diff  = diff
                    best_price = float(price)

            if best_price is not None:
                last_price = best_price

            result.append({
                "datetime": slot.strftime("%Y-%m-%d %H:%M"),
                "price":    last_price,
                "symbol":   symbol,
            })

        return result

    def debug_slot_matching(self, symbol: str,
                           interval_: str = "5min",
                           limit: int = 20) -> dict:
        return {"info": "时间轴已改为固定生成，无需此诊断接口"}
