#!/usr/bin/env python3
"""
诊断并修复 price_history 表的时间戳问题
"""
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# 尝试导入 psycopg2
try:
    import psycopg2
except ImportError:
    print("错误: 需要 psycopg2。运行: pip install psycopg2-binary")
    sys.exit(1)

def get_db_url():
    """从环境变量获取数据库 URL"""
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("错误: 未设置 DATABASE_URL 环境变量")
        sys.exit(1)
    return url

def parse_db_url(url):
    """解析 PostgreSQL URL"""
    # 处理 postgres:// 和 postgresql://
    url = url.replace("postgres://", "postgresql://")
    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }

def connect():
    """连接数据库"""
    cfg = parse_db_url(get_db_url())
    return psycopg2.connect(**cfg)

def diagnose():
    """诊断问题"""
    conn = connect()
    cur = conn.cursor()
    
    print("=" * 60)
    print("数据库诊断报告")
    print("=" * 60)
    
    # 1. 查看表结构
    print("\n【表结构】")
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'price_history'
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} (nullable: {row[2]})")
    
    # 2. 查看总记录数
    print("\n【记录统计】")
    cur.execute("SELECT COUNT(*) FROM price_history")
    total = cur.fetchone()[0]
    print(f"  总记录数: {total}")
    
    # 3. 按品种统计
    cur.execute("""
        SELECT symbol, COUNT(*), MIN(dt), MAX(dt)
        FROM price_history
        GROUP BY symbol
        ORDER BY symbol
    """)
    print("\n【按品种统计】")
    for row in cur.fetchall():
        symbol, count, min_dt, max_dt = row
        print(f"  {symbol}:")
        print(f"    记录数: {count}")
        print(f"    最早: {min_dt}")
        print(f"    最晚: {max_dt}")
        if min_dt == max_dt:
            print(f"    ⚠️ 警告: 所有记录时间相同！")
    
    # 4. 查看最近的 10 条记录详情
    print("\n【最近 10 条记录 (GFEX.pt)】")
    cur.execute("""
        SELECT id, symbol, price, dt, created_at
        FROM price_history
        WHERE symbol = 'KQ.m@GFEX.pt'
        ORDER BY dt DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        id_, symbol, price, dt, created_at = row
        print(f"  id={id_}, price={price}, dt={dt}, created_at={created_at}")
    
    print("\n【最近 10 条记录 (GFEX.pd)】")
    cur.execute("""
        SELECT id, symbol, price, dt, created_at
        FROM price_history
        WHERE symbol = 'KQ.m@GFEX.pd'
        ORDER BY dt DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        id_, symbol, price, dt, created_at = row
        print(f"  id={id_}, price={price}, dt={dt}, created_at={created_at}")
    
    cur.close()
    conn.close()
    
    return total > 0

def fix_timestamps():
    """
    修复时间戳问题
    根据 created_at 字段重新生成 dt 字段，假设数据是每 5 分钟一条
    """
    conn = connect()
    cur = conn.cursor()
    
    print("\n" + "=" * 60)
    print("开始修复时间戳")
    print("=" * 60)
    
    # 获取所有品种
    cur.execute("SELECT DISTINCT symbol FROM price_history")
    symbols = [row[0] for row in cur.fetchall()]
    
    for symbol in symbols:
        print(f"\n处理 {symbol}...")
        
        # 获取该品种的所有记录，按 created_at 排序
        cur.execute("""
            SELECT id, created_at
            FROM price_history
            WHERE symbol = %s
            ORDER BY created_at ASC
        """, (symbol,))
        
        rows = cur.fetchall()
        if not rows:
            print(f"  无记录，跳过")
            continue
        
        # 使用当前时间作为基准，向前推算
        # 假设数据是每 5 分钟采集一次
        now = datetime.now(timezone.utc)
        
        updates = []
        for i, (id_, created_at) in enumerate(rows):
            # 从最新记录向前推，每条间隔 5 分钟
            # 或者使用 created_at 的时间
            if created_at:
                new_dt = created_at.replace(tzinfo=timezone.utc) if created_at.tzinfo is None else created_at
            else:
                new_dt = now - timedelta(minutes=5 * (len(rows) - 1 - i))
            
            updates.append((new_dt, id_))
        
        # 批量更新
        cur.executemany(
            "UPDATE price_history SET dt = %s WHERE id = %s",
            updates
        )
        conn.commit()
        print(f"  更新了 {len(updates)} 条记录")
    
    cur.close()
    conn.close()
    print("\n修复完成！")

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print(f"  python {sys.argv[0]} diagnose    # 诊断问题")
        print(f"  python {sys.argv[0]} fix         # 修复时间戳")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "diagnose":
        diagnose()
    elif cmd == "fix":
        confirm = input("确定要修复时间戳吗？这将根据 created_at 重新生成 dt 字段。(yes/no): ")
        if confirm.lower() == "yes":
            fix_timestamps()
        else:
            print("已取消")
    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)

if __name__ == "__main__":
    main()
