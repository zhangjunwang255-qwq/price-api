"""
app/store.py — 价格存储（内存缓存 + MySQL 持久化）
"""
import os, threading, time, logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from queue import Queue, Empty
from urllib.parse import urlparse

import pymysql
from pymysql.cursors import DictCursor

from .config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER,
    MYSQL_PASSWORD, MYSQL_DATABASE,
    HISTORY_RETENTION_MIN,
)


def _parse_db_url(url: str):
    """解析 mysql://user:pass@host:port/db → (host, port, user, pass, db)"""
    u = urlparse(url)
    return (
        u.hostname or "localhost",
        u.port or 3306,
        u.username or "root",
        u.password or "",
        u.path.lstrip("/") or "price_history",
    )

log = logging.getLogger("price-store")


def _nan(v: float) -> float:
    return 0.0 if v != v else v  # NaN → 0.0


class PriceStore:
    """
    内存缓存最新行情 + MySQL 持久化历史
    """

    def __init__(self):
        self._lock   = threading.RLock()
        self._latest: dict[str, dict]  = {}  # symbol → 最新行情
        self._prev:   dict[str, float] = {}  # symbol → 上一个价格（算涨跌用）
        self._symbols: list[str]       = []
        self._conn: pymysql.Connection | None = None
        self._db_ok: bool = True  # MySQL 可用标记

        self._write_queue: Queue = Queue()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

        self._init_db()

    # ── 数据库连接 ────────────────────────────────────

    def _get_conn(self) -> pymysql.Connection | None:
        """懒加载连接，断了自动重连；不可用时返回 None"""
        if not self._db_ok:
            return None
        if self._conn is None or not self._conn.open:
            # Railway 内置数据库直接用 DATABASE_URL
            db_url = os.getenv("DATABASE_URL", "")
            if db_url.startswith("mysql://"):
                host, port, user, password, database = _parse_db_url(db_url)
            else:
                host, port, user, password, database = (
                    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
                )
            self._conn = pymysql.connect(
                host     = host,
                port     = port,
                user     = user,
                password = password,
                database = database,
                charset  = "utf8mb4",
                cursorclass = DictCursor,
                autocommit  = False,
            )
        return self._conn

    # ── 数据库初始化 ──────────────────────────────────

    def _init_db(self):
        try:
            db_url = os.getenv("DATABASE_URL", "")
            if db_url.startswith("mysql://"):
                db_host, db_port, db_user, db_pass, db_name = _parse_db_url(db_url)
                # 先建库（如果不存在）
                tmp_conn = pymysql.connect(
                    host     = db_host,
                    port     = db_port,
                    user     = db_user,
                    password = db_pass,
                    charset  = "utf8mb4",
                )
                with tmp_conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                tmp_conn.close()
                log.info("MySQL 数据库 '%s' 就绪", db_name)
            else:
                # 本地 / Hostinger
                tmp_conn = pymysql.connect(
                    host     = MYSQL_HOST,
                    port     = MYSQL_PORT,
                    user     = MYSQL_USER,
                    password = MYSQL_PASSWORD,
                    charset  = "utf8mb4",
                )
                with tmp_conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                tmp_conn.close()
                log.info("MySQL 数据库 '%s' 就绪", MYSQL_DATABASE)

            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS price_history (
                        id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        symbol      VARCHAR(32) NOT NULL,
                        price       DECIMAL(16,4) NOT NULL,
                        volume      BIGINT UNSIGNED NOT NULL,
                        datetime    DATETIME(3) NOT NULL,
                        created_at  DATETIME(3) NOT NULL DEFAULT NOW(3),
                        INDEX idx_symbol_time (symbol, datetime DESC)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            conn.commit()
            log.info("MySQL price_history 表初始化完成")

        except Exception as e:
            log.warning("MySQL 初始化失败（降级为纯内存模式）: %s", e)
            self._db_ok = False

    # ── 异步写入线程 ─────────────────────────────────

    def _writer_loop(self):
        """后台线程：批量从队列取数据，批量 INSERT MySQL"""
        batch: list[tuple] = []
        batch_size = 50
        last_flush = time.time()

        while True:
            try:
                item = self._write_queue.get(timeout=0.5)
                batch.append(item)
                # 凑够 batch_size 或超过 2 秒就刷一次
                if len(batch) >= batch_size or (time.time() - last_flush) > 2.0:
                    self._flush(batch)
                    batch.clear()
                    last_flush = time.time()
            except Empty:
                if batch:
                    self._flush(batch)
                    batch.clear()
                    last_flush = time.time()

    def _flush(self, batch: list[tuple]):
        if not batch or not self._db_ok:
            return
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO price_history
                       (symbol, price, volume, datetime, created_at)
                       VALUES (%s, %s, %s, %s, %s)""",
                    batch,
                )
            conn.commit()
            log.debug("MySQL 批量写入 %d 条", len(batch))
        except Exception as e:
            log.warning("MySQL 批量写入失败: %s", e)
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                pass

    # ── 写入 ──────────────────────────────────────────

    def update(self, symbol: str, instrument_id: str,
               price: float, volume: int, dt: str):
        with self._lock:
            prev_price = self._prev.get(symbol)
            change     = None
            change_pct = None
            if prev_price is not None and prev_price != 0:
                change     = round(price - prev_price, 2)
                change_pct = round(change / prev_price * 100, 4)

            record = {
                "symbol":        symbol,
                "instrument_id": instrument_id,
                "price":         _nan(price),
                "volume":        int(volume),
                "datetime":      dt,
                "change":        change,
                "change_pct":    change_pct,
            }
            self._latest[symbol] = record
            self._prev[symbol]   = _nan(price)
            if symbol not in self._symbols:
                self._symbols.append(symbol)

            # 异步写 MySQL（主线程不阻塞）
            self._write_queue.put((
                symbol,
                _nan(price),
                int(volume),
                dt,
                datetime.now(timezone.utc).isoformat(),
            ))

    # ── 读取 ──────────────────────────────────────────

    @property
    def latest(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._latest)

    @property
    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._symbols)

    def get_history(self, symbol: str,
                     minutes: int = 60,
                     limit: int = 200) -> list[dict]:
        if not self._db_ok:
            return []
        since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, price, volume, datetime
                    FROM price_history
                    WHERE symbol = %s AND datetime >= %s
                    ORDER BY datetime DESC
                    LIMIT %s
                    """,
                    (symbol, since, limit),
                )
                rows = cur.fetchall()
            return [
                {
                    "symbol":   r["symbol"],
                    "price":    float(r["price"]),
                    "volume":   int(r["volume"]),
                    "datetime": r["datetime"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                }
                for r in reversed(rows)
            ]
        except Exception as e:
            log.warning("MySQL 查询失败: %s", e)
            return []

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
