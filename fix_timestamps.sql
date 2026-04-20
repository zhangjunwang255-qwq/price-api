-- 修复 price_history 表时间戳问题的 SQL 脚本
-- 在 Railway PostgreSQL 控制台或 psql 中执行

-- 1. 先查看问题
SELECT 
    symbol,
    COUNT(*) as count,
    MIN(dt) as min_dt,
    MAX(dt) as max_dt,
    CASE WHEN MIN(dt) = MAX(dt) THEN '⚠️ 时间相同!' ELSE 'OK' END as status
FROM price_history
GROUP BY symbol;

-- 2. 查看最近 10 条记录的详情（按 created_at 排序）
SELECT id, symbol, price, dt, created_at
FROM price_history
WHERE symbol = 'KQ.m@GFEX.pt'
ORDER BY created_at DESC
LIMIT 10;

-- 3. 修复方案 A: 如果有 created_at 且不为空，用 created_at 替换 dt
-- 先备份（可选）
-- CREATE TABLE price_history_backup AS SELECT * FROM price_history;

-- 更新 pt 品种的时间戳（根据 created_at）
UPDATE price_history
SET dt = created_at
WHERE symbol = 'KQ.m@GFEX.pt' 
  AND created_at IS NOT NULL
  AND (dt IS NULL OR dt = (SELECT MIN(dt) FROM price_history WHERE symbol = 'KQ.m@GFEX.pt'));

-- 更新 pd 品种的时间戳（根据 created_at）
UPDATE price_history
SET dt = created_at
WHERE symbol = 'KQ.m@GFEX.pd' 
  AND created_at IS NOT NULL
  AND (dt IS NULL OR dt = (SELECT MIN(dt) FROM price_history WHERE symbol = 'KQ.m@GFEX.pd'));

-- 4. 修复方案 B: 如果 created_at 也不对，手动生成递增时间戳
-- 这个方案会按 id 顺序，每 5 分钟一条，从当前时间向前推

-- 先创建一个临时表存储新的时间戳
CREATE TEMP TABLE temp_fix AS
SELECT 
    id,
    symbol,
    NOW() - INTERVAL '5 minutes' * (ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY id DESC) - 1) as new_dt
FROM price_history;

-- 查看临时表（确认新时间戳）
SELECT * FROM temp_fix ORDER BY symbol, new_dt DESC LIMIT 20;

-- 应用修复（取消注释执行）
-- UPDATE price_history ph
-- SET dt = tf.new_dt
-- FROM temp_fix tf
-- WHERE ph.id = tf.id;

-- 清理临时表
DROP TABLE IF EXISTS temp_fix;

-- 5. 修复后验证
SELECT 
    symbol,
    COUNT(*) as count,
    MIN(dt) as min_dt,
    MAX(dt) as max_dt,
    CASE WHEN MIN(dt) = MAX(dt) THEN '⚠️ 仍有问题' ELSE '✅ 已修复' END as status
FROM price_history
GROUP BY symbol;
