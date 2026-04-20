"""
app/main.py — FastAPI 入口
"""
import logging, os
from contextlib import suppress
import asyncio, threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import PORT
from .store  import PriceStore
from .tqsdk_worker import start_tqsdk

# ── 日志 ──────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("price-api")

# ── 全局状态 ──────────────────────────────────────────
class AppState:
    status:       str = "Starting"
    error:        str | None = None
    connected_at: str = ""
    symbols:      list[str] = []

app_state = AppState()

# ── 存储 ──────────────────────────────────────────────
store = PriceStore()

# ── 启动 TqSdk 采集线程 ───────────────────────────────
start_tqsdk(
    user     = os.getenv("TQ_USER", ""),
    password = os.getenv("TQ_PASS", ""),
    store    = store,
    app_state= app_state,
)

# ── FastAPI 实例 ──────────────────────────────────────
app = FastAPI(
    title       = "SRE Price API",
    description = "铂金/钯金实时行情 API，支持 HTTP 轮询 / WebSocket 推送 / 价格历史查询",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── WebSocket 客户端管理 ──────────────────────────────
ws_clients: set[WebSocket] = set()
_ws_lock    = threading.Lock()


def broadcast(data: dict):
    """线程安全地推送给所有 WS 客户端"""
    global ws_clients
    if not ws_clients:
        return
    dead = set()
    with _ws_lock:
        clients_copy = list(ws_clients)
    for ws in clients_copy:
        try:
            asyncio.run(ws.send_json(data))
        except Exception:
            dead.add(ws)
    if dead:
        with _ws_lock:
            ws_clients -= dead


# 注入广播函数给 tqsdk_worker
import app.tqsdk_worker as tw
tw._ws_broadcast = broadcast

# ── 静态前端页面 ───────────────────────────────────────
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/dashboard")
def dashboard():
    """铂钯金实时行情监控面板"""
    return FileResponse(os.path.join(frontend_dir, "index.html"))


@app.websocket("/ws/quote")
async def ws_quote(ws: WebSocket):
    await ws.accept()
    with _ws_lock:
        ws_clients.add(ws)
    log.info("WS 连接 (当前 %d 个)", len(ws_clients))
    try:
        while True:
            await ws.send_json({
                "status": app_state.status,
                "error":  app_state.error,
                "data":   store.latest,
            })
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            ws_clients.discard(ws)
        log.info("WS 断开 (当前 %d 个)", len(ws_clients))


# ── HTTP 接口 ─────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "SRE Price API",
        "version": "1.0.0",
        "status":  app_state.status,
        "endpoints": {
            "/quote":           "所有品种最新行情",
            "/quote/{symbol}":  "单个品种行情",
            "/history":         "价格历史（?symbol=GFEX.PT&minutes=60）",
            "/symbols":         "已订阅品种列表",
            "/health":          "健康检查",
            "/ws/quote":        "WebSocket 实时推送",
            "/dashboard":       "铂钯金实时监控面板（可视化页面）",
            "/mode":            "获取/设置采样模式 (GET 查询, POST 设置)",
        },
    }


# ── 采样模式控制 ─────────────────────────────────────

@app.get("/mode")
def get_mode():
    """获取当前采样模式"""
    return store.mode_info


@app.post("/mode")
def set_mode(mode: str = Form(...)):
    """设置采样模式: 竞标(实时) 或 日常(5分钟)"""
    result = store.set_mode(mode)
    if not result.get("ok"):
        raise HTTPException(400, result.get("error"))
    return result


@app.get("/quote")
def get_quote():
    return {
        "status":     app_state.status,
        "error":      app_state.error,
        "data":       store.latest,
    }


@app.get("/quote/{symbol}")
def get_quote_one(symbol: str):
    if symbol not in store.latest:
        raise HTTPException(404, f"未知品种: {symbol}")
    return {
        "status": app_state.status,
        "data":   store.latest[symbol],
    }


@app.get("/history")
def get_history(
    symbol:    str  = Query(...,       description="品种代码，如 KQ.m@GFEX.pt"),
    interval_: str  = Query("5min",   description="采样间隔: 5min | 15min | 1hour"),
    limit:     int  = Query(200,      ge=1, le=1000, description="最多返回条数"),
):
    if interval_ not in ("5min", "15min", "1hour"):
        raise HTTPException(400, "无效 interval，可用: 5min | 15min | 1hour")
    records = store.get_history(symbol, interval_=interval_, limit=limit)
    return {
        "symbol":    symbol,
        "interval":  interval_,
        "count":     len(records),
        "records":   records,
    }


@app.get("/debug/stats")
def debug_stats():
    """诊断接口：查看 update/采样/flush 计数器"""
    s = store
    return {
        "update_calls":   s._update_count,
        "samples_queued": s._sample_count,
        "flush_ok":       s._flush_ok,
        "flush_fail":     s._flush_fail,
        "queue_len":      len(s._write_queue),
        "mode":           s._mode,
        "interval_sec":  s._interval,
        "db_ok":          s._db_ok,
        "symbols":        list(s._latest.keys()),
        "store_id":       id(s),
    }

@app.get("/debug/pg_test")
def debug_pg_test():
    """直接测试 PG 读写（绕过 store 队列机制）"""
    import psycopg2
    from .config import DATABASE_URL
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM price_history")
            count = cur.fetchone()[0]
            cur.execute(
                "SELECT symbol, price, dt FROM price_history ORDER BY dt DESC LIMIT 5"
            )
            rows = cur.fetchall()
        conn.close()
        return {"ok": True, "count": count, "latest": [str(r) for r in rows]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/debug/flush_test")
def debug_flush_test():
    """直接测试一次 PG 写入（绕过 writer_loop），返回详细错误"""
    import psycopg2, traceback
    from .config import DATABASE_URL
    from .store import _align_to_5min
    from datetime import datetime

    test_batch = [
        ("KQ.m@GFEX.pt", 1234.5, 10, _align_to_5min(datetime.now())),
        ("KQ.m@GFEX.pd", 567.8,  5,  _align_to_5min(datetime.now())),
    ]
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_history (symbol, price, volume, dt)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol, dt) DO UPDATE
                SET price = EXCLUDED.price, volume = EXCLUDED.volume
                """,
                test_batch,
            )
        conn.commit()
        return {"ok": True, "count": len(test_batch), "batch": [str(t) for t in test_batch]}
    except Exception as e:
        tb = traceback.format_exc()
        if conn:
            with suppress(Exception):
                conn.rollback()
        return {"ok": False, "error": str(e), "type": type(e).__name__,
                "traceback": tb, "batch": [str(t) for t in test_batch]}
    finally:
        if conn:
            with suppress(Exception):
                conn.close()


@app.get("/debug/pg_schema")
def debug_pg_schema():
    """查看 price_history 表结构"""
    import psycopg2
    from .config import DATABASE_URL
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'price_history'
                ORDER BY ordinal_position
            """)
            cols = cur.fetchall()
            cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename='price_history'")
            idxs = cur.fetchall()
        conn.close()
        return {"columns": [dict(zip(["name","type","nullable","default"], r)) for r in cols],
                "indexes": [dict(zip(["name","def"], r)) for r in idxs]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/migrate")
def debug_migrate():
    """一次性迁移：清理重复数据 → 加唯一约束"""
    import psycopg2
    from .config import DATABASE_URL
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                # 1. 删重复：保留每个 (symbol,dt) 的最大 ctid（最新一条）
                cur.execute("""
                    DELETE FROM price_history a
                    USING price_history b
                    WHERE a.ctid < b.ctid
                      AND a.symbol = b.symbol
                      AND a.dt = b.dt
                """)
                deleted = cur.rowcount
                conn.commit()
                log.info("清理重复数据: %d 条", deleted)

                # 2. 加唯一约束
                cur.execute("""
                    ALTER TABLE price_history
                    ADD CONSTRAINT price_history_symbol_dt_key
                    UNIQUE (symbol, dt)
                    DEFERRABLE INITIALLY DEFERRED
                """)
            conn.commit()
            return {"ok": True, "deleted_duplicates": deleted,
                    "message": "约束添加成功，数据已清理"}
        except psycopg2.errors.DuplicateObject:
            conn.rollback()
            return {"ok": True, "deleted_duplicates": 0,
                    "message": "约束已存在，无需操作"}
        finally:
            conn.close()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/symbols")
def list_symbols():
    return {
        "symbols": store.symbols,
        "status":  app_state.status,
    }


@app.get("/health")
def health():
    ok = app_state.status in ("Running", "Connecting", "AuthRequired", "Starting")
    return {
        "ok":     ok,
        "status": app_state.status,
        "error":  app_state.error,
    }


# ── 启动 ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    log.info("启动 Price API → 0.0.0.0:%d", PORT)
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, log_level="info", reload=False)
