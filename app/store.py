"""
app/store.py — 价格存储（内存缓存 + PostgreSQL 持久化）

修复记录：
- update() 双重 interval 检查 bug → 采样恢复
- writer_loop 顶层 try/except（防崩溃）
- 统计计数器改实例变量
- 单例守卫用 __new__
- 时间轴改为固定交易时段（9:00/10:30/13:30 三节，含间隙），不依赖数据库时间戳
- KEEP_DAYS=30（按时间清理）
- PG 写入：每次新建独立连接（不用池），避免连接被污染导致静默失败
- get_history 支持 days 参数：生成多天固定时间槽
"""
import threading, time, logging
from datetime import datetime, timedelta, date
from contextlib import suppress

import psycopg2

from .config import DATABASE_URL, SYMBOLS, SAMPLE_INTERVAL_SEC, KEEP_DAYS, DEFAULT_MODE


log = logging.getLogger("price-store")


# ────────────────────────────────────────────────────
#  广期所贵金属交易时段（只有白盘，无夜盘）
# ────────────────────────────────────────────────────
# 白盘三节（北京时间）：
#   第一节  09:00 – 10:15
#   第二节  10:30 – 11:30
#   第三节  13:30 – 15:00
DAY_SECTIONS = [
    (9,  0,  10, 15),   # 第一节
    (10, 30, 11, 30),   # 第二节
    (13, 30, 15,  1),   # 第三节（含 15:00，+1 确保 15:00 被包含）
]


# ── 单节时间槽生成 ─────────────────────────────────
def _section_slots(start_h: int, start_m: int,
                   end_h: int, end_m: int,
                   interval_min: int) -> list[tuple]:
    """
    生成单节交易时段的所有 (hour, minute) 时间槽。
    end_h=24 表示跨天到 23:59（用于夜盘 21:00→23:59）。
    """
    slots = []
    ch, cm = start_h, start_m
    while True:
        if ch == 24:
            break
        if ch > end_h or (ch == end_h and cm >= end_m):
            break
        slots.append((ch, cm))
        cm += interval_min
        if cm >= 60:
            ch += cm // 60
            cm %= 60
    return slots


def _day_slots(dt: date, interval_min: int) -> list[datetime]:
    """生成指定日期白盘所有固定时间槽"""
    result = []
    for sh, sm, eh, em in DAY_SECTIONS:
        for h, m in _section_slots(sh, sm, eh, em, interval_min):
            result.append(datetime(dt.year, dt.month, dt.day, h, m, 0))
    return result


def _current_session_slots(interval_min: int) -> list[datetime]:
    """返回今天白盘所有固定时间槽"""
    return _day_slots(datetime.now().date(), interval_min)


# ────────────────────────────────────────────────────
#  工具函数
# ────────────────────────────────────────────────────
def _is_trading_time() -> bool:
    """判断当前是否在交易时段"""
    now = datetime.now()
    h, m = now.hour, now.minute
    t = h * 60 + m
    if (540 <= t < 615) or (630 <= t < 690) or (810 <= t < 900):
        return True
    return False


def _nan(v: float) -> float:
    return 0.0 if v != v else v


def _align_to_5min(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


# ────────────────────────────────────────────────────
#  PriceStore（单例）
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
        self._latest:   dict[str, dict] = {}
        self._prev:     dict[str, float] = {}
        self._symbols:  list[str]        = list(SYMBOLS)

        self._mode     = DEFAULT_MODE
        self._interval = 0 if DEFAULT_MODE == "竞标" else SAMPLE_INTERVAL_SEC
        self._last_sample_time: dict[str, float] = {}

        self._db_ok: bool = bool(DATABASE_URL)
        if not DATABASE_URL:
            log.warning("DATABASE_URL 未设置，降级为纯内存模式")
        else:
            log.info("PriceStore 初始化，模式=%s，PG=%s", self._mode, self._db_ok)
            self._test_pg()

        self._write_queue: list[tuple] = []
        self._write_lock   = threading.Lock()

        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name="writer-loop"
        )
        self._writer_thread.start()

        self._update_count = 0
        self._sample_count = 0
        self._flush_ok     = 0
        self._flush_fail   = 0

    def _test_pg(self) -> bool:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.commit()
            finally:
                conn.close()
            log.info("PG 连接测试成功")
            return True
        except Exception as e:
            log.warning("PG 启动测试失败: %s", e)
            self._db_ok = False
            return False

    def _pg_conn(self):
        return psycopg2.connect(DATABASE_URL, connect_timeout=5)

    # ── 后台 writer ───────────────────────────────

    def _writer_loop(self):
        consecutive_fail = 0
        while True:
            time.sleep(2)
            try:
                self._flush()
                self._cleanup()
                consecutive_fail = 0
            except Exception as e:
                log.error("writer_loop 异常: %s", e, exc_info=True)
                consecutive_fail += 1
                if consecutive_fail >= 5:
                    log.critical("连续失败 %d 次，主动退出让 Railway 重启", consecutive_fail)
                    import os
                    os._exit(1)

    def _flush(self):
        if not self._db_ok:
            return
        with self._write_lock:
            batch = self._write_queue
            self._write_queue = []
        if not batch:
            return

        conn = None
        try:
            conn = self._pg_conn()
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
            log.info("PG 写入成功: %d 条", len(batch))
        except Exception as e:
            log.error("PG 写入失败 [%s]: %s | batch[0]=%s", type(e).__name__, e, batch[:1])
            if conn:
                with suppress(Exception):
                    conn.rollback()
            with self._write_lock:
                self._write_queue = batch + self._write_queue
            self._flush_fail += len(batch)
        finally:
            if conn:
                with suppress(Exception):
                    conn.close()

    def _cleanup(self):
        if not self._db_ok:
            return
        conn = None
        try:
            conn = self._pg_conn()
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM price_history WHERE dt < NOW() - INTERVAL '{KEEP_DAYS} days'")
                deleted = cur.rowcount
            conn.commit()
            if deleted:
                log.info("PG 清理完成: %d 条", deleted)
        except Exception as e:
            log.warning("PG 清理失败: %s", e)
        finally:
            if conn:
                with suppress(Exception):
                    conn.close()

    # ── 行情更新 ─────────────────────────────────

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        self._update_count += 1

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

        now = time.time()
        if self._interval > 0 and (now - self._last_sample_time.get(symbol, 0.0)) < self._interval:
            return

        with self._lock["main"]:
            self._last_sample_time[symbol] = time.time()

        dt_parsed = self._parse_dt(dt)
        aligned = _align_to_5min(dt_parsed)
        with self._write_lock:
            self._write_queue.append((symbol, _nan(price), int(volume), aligned))
        self._sample_count += 1

    def _parse_dt(self, dt_str: str) -> datetime:
        if not dt_str:
            return datetime.now()
        try:
            return datetime.strptime(dt_str[:23], "%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            try:
                return datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return datetime.now()

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
                    limit: int = 200,
                    days: int = 1) -> list[dict]:
        """
        按固定交易时段时间轴返回数据，支持多天历史。

        - days: 查询多少天（含今天），默认 1
        - interval_: 5min | 15min | 1hour
        - limit: 每种间隔的 slot 数量上限（仅限当天，超过则取最近 limit 个）
        """
        interval_map = {"5min": 5, "15min": 15, "1hour": 60}
        interval_min = interval_map.get(interval_, 5)

        today = datetime.now().date()
        # 生成 N 天 slots（今天在最前）
        slots = []
        for d in [today - timedelta(days=i) for i in range(days - 1, -1, -1)]:
            slots.extend(_day_slots(d, interval_min))

        # 当天部分按 limit 截断（老数据舍去，保留最近 limit 条）
        today_slots = _day_slots(today, interval_min)
        if len(today_slots) > limit:
            today_slots = today_slots[-limit:]
        # 重建 slots：今天用截断版，昨天起完整
        if days == 1:
            slots = today_slots
        else:
            past_slots = []
            for d in [today - timedelta(days=i) for i in range(days - 2, -1, -1)]:
                past_slots.extend(_day_slots(d, interval_min))
            slots = past_slots + today_slots

        if not self._db_ok or not slots:
            return [{"datetime": s.strftime("%Y-%m-%d %H:%M"),
                     "price": None, "symbol": symbol} for s in slots]

        conn = None
        try:
            conn = self._pg_conn()
            with conn.cursor() as cur:
                # 查询最近 days 天的数据
                cur.execute(
                    "SELECT price, dt FROM price_history "
                    "WHERE symbol=%s AND dt >= NOW() - INTERVAL '%s days' "
                    "ORDER BY dt ASC",
                    (symbol, days),
                )
                rows = cur.fetchall()
        except Exception as e:
            log.warning("get_history 查询失败: %s", e)
            return [{"datetime": s.strftime("%Y-%m-%d %H:%M"),
                     "price": None, "symbol": symbol} for s in slots]
        finally:
            if conn:
                with suppress(Exception):
                    conn.close()

        # 预建 slot → price 映射（O(n*m) → 优化为 O(n)）
        slot_set = {s: None for s in slots}
        last_price = None
        for price, dt in rows:
            dt_naive = dt.replace(tzinfo=None)
            if dt_naive in slot_set:
                slot_set[dt_naive] = float(price)
        # 前向填充
        result = []
        for slot in slots:
            if slot_set.get(slot) is not None:
                last_price = slot_set[slot]
            result.append({
                "datetime": slot.strftime("%Y-%m-%d %H:%M"),
                "price":    last_price,
                "symbol":   symbol,
            })
        return result
