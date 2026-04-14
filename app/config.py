"""
app/config.py — 配置（全部通过环境变量）
"""
import os

TQ_USER  = os.getenv("TQ_USER", "")
TQ_PASS  = os.getenv("TQ_PASS", "")
PORT     = int(os.getenv("PORT", os.getenv("RAILWAY_PORT", "7860")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# MySQL（本地 / Hostinger / Railway 内置数据库）
MYSQL_HOST     = os.getenv("MYSQL_HOST", "")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "price_history")

# 要订阅的品种列表（广期所，PT=铂金 PD=钯金，KQ.m@ 主力连续合约自动换月）
SYMBOLS = ["KQ.m@GFEX.PT", "KQ.m@GFEX.PD"]

# wait_update 超时秒数（保证休市时也能正常轮询）
DEADLINE_SEC = 2.0

# 历史数据保留时长（分钟）
HISTORY_RETENTION_MIN = 1440  # 24 小时
