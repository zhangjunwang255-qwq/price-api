"""
app/tqsdk_worker.py — TqSdk 后台采集线程
"""
import threading, time, logging, asyncio
from contextlib import suppress

from tqsdk import TqApi, TqAuth

from .config import SYMBOLS, DEADLINE_SEC

log = logging.getLogger("tqsdk-worker")

# 全局引用，shutdown 时关闭
_api_ref = None
_loop_ref = None

# ── 推送给 WebSocket 客户端 ──────────────────────────
_ws_broadcast = None   # 由 main.py 注入


def set_broadcast_fn(fn):
    global _ws_broadcast
    _ws_broadcast = fn


def start_tqsdk(user: str, password: str, store, app_state):
    """在 daemon 线程里启动 TqSdk 连接"""
    if not user or not password:
        app_state.status = "AuthRequired"
        app_state.error  = "TQ_USER / TQ_PASS 环境变量未设置"
        log.error("缺少 TQ_USER 或 TQ_PASS")
        return

    def run():
        global _api_ref
        app_state.status = "Connecting"
        log.info("TqSdk 连接中 ...")

        try:
            api = TqApi(auth=TqAuth(user, password))
            _api_ref = api
            app_state.status      = "Running"
            app_state.connected_at = time.strftime("%Y-%m-%d %H:%M:%S")
            app_state.symbols      = SYMBOLS
            log.info("TqSdk 连接成功，已订阅: %s", SYMBOLS)

            quotes = {sym: api.get_quote(sym) for sym in SYMBOLS}
            last_log = 0.0

            while True:
                # deadline=+2s：休市时最多 2s 返回一次，不死阻塞
                api.wait_update(deadline=time.time() + DEADLINE_SEC)

                for sym, q in quotes.items():
                    store.update(
                        symbol        = sym,
                        instrument_id = q.instrument_id,
                        price         = q.last_price,
                        volume        = q.volume,
                        dt            = q.datetime,
                    )

                # WebSocket 广播
                if _ws_broadcast:
                    _ws_broadcast({
                        "status": app_state.status,
                        "data":   store.latest,
                    })

                # 每 30s 心跳日志
                now = time.time()
                if now - last_log >= 30:
                    latest = store.latest
                    log.info(
                        "行情 | 铂金=%s | 钯金=%s",
                        latest.get("KQ.m@GFEX.pt", {}).get("price", "N/A"),
                        latest.get("KQ.m@GFEX.pd", {}).get("price", "N/A"),
                    )
                    last_log = now

        except Exception as e:
            app_state.status = "Crashed"
            app_state.error  = str(e)
            log.exception("TqSdk 线程异常")
        finally:
            with suppress(Exception):
                _api_ref.close()
            log.info("TqSdk 连接已关闭")


    t = threading.Thread(target=run, daemon=True, name="tqsdk-worker")
    t.start()
    log.info("TqSdk 工作线程已启动")
