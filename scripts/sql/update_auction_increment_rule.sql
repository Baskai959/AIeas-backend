-- =============================================================================
-- update_auction_increment_rule.sql
-- 用途：将 seed_live_auction_demo.sql 插入的 5 个拍品
--       （auction_lot.auction_id = 90001 ~ 90005）的 `increment_rule` 字段
--       由旧的「数组 + maxPriceCent/stepCent」形态，改写为新的「ladder」结构，
--       同时**完整保留 seed 中原有的阶梯阈值与加价金额数值**。
--
-- 数据来源：scripts/sql/seed_live_auction_demo.sql 第 4 节 INSERT 的 5 条
--          auction_lot 记录（auction_id 90001 / 90002 / 90003 / 90004 / 90005）。
--
-- 与 seed 脚本的关系：
--   * 本脚本不重新生成种子数据，只在 seed 脚本执行后单独跑一次，覆盖
--     `auction_lot.increment_rule` 一列；
--   * 不修改 seed 脚本本身，不修改 `rule_snapshot`、`bid_record` 等其他字段；
--   * seed 中 5 个拍品的 increment_rule 数值完全一致，因此本脚本用一条 UPDATE
--     批量覆盖，结果保持一致性。
--
-- 旧结构（seed 现存写法，金额单位「分」）：
--   [
--     {"maxPriceCent":  50000, "stepCent":  1000},
--     {"maxPriceCent": 200000, "stepCent":  5000},
--     {"maxPriceCent":1000000, "stepCent": 20000},
--     {                        "stepCent": 50000}
--   ]
--
-- 转换规则（旧 → 新）：
--   * 第一段：    min = 0,                        max = 第一段 maxPriceCent,
--                amount = 第一段 stepCent
--   * 中间段 i：  min = 上一段 maxPriceCent,      max = 当前段 maxPriceCent,
--                amount = 当前段 stepCent
--   * 最后一段：  min = 倒数第二段 maxPriceCent,  （不写 max）,
--                amount = 最后一段 stepCent
--
-- 新结构（写入字段，严格保持 seed 原有数值）：
--   {
--     "type": "ladder",
--     "steps": [
--       { "min":       0, "max":   50000, "amount":  1000 },
--       { "min":   50000, "max":  200000, "amount":  5000 },
--       { "min":  200000, "max": 1000000, "amount": 20000 },
--       { "min": 1000000,                 "amount": 50000 }
--     ]
--   }
--   段连续性：相邻段满足 step[i].max == step[i+1].min。
--
-- 可重放性：
--   * 单条 UPDATE，幂等（多次执行结果一致）。
--   * 末尾 SELECT 打印更新后的 increment_rule，便于人工核对。
--   * 可在 mysql 命令行 `source` 一次性执行（MySQL 8.0+）。
-- =============================================================================

SET NAMES utf8mb4;

-- ---- 1. 更新 increment_rule（仅此一列）----
-- 5 个拍品（90001~90005）seed 阶梯参数完全一致，合并为一条 UPDATE。
UPDATE `auction_lot`
   SET `increment_rule` = JSON_OBJECT(
         'type',  'ladder',
         'steps', JSON_ARRAY(
                    JSON_OBJECT('min',       0, 'max',   50000, 'amount',  1000),
                    JSON_OBJECT('min',   50000, 'max',  200000, 'amount',  5000),
                    JSON_OBJECT('min',  200000, 'max', 1000000, 'amount', 20000),
                    JSON_OBJECT('min', 1000000,                 'amount', 50000)
                  )
       )
 WHERE `auction_id` IN (90001, 90002, 90003, 90004, 90005);

-- ---- 2. 校验：打印更新后的 increment_rule ----
SELECT `auction_id`, `increment_rule`
  FROM `auction_lot`
 WHERE `auction_id` IN (90001, 90002, 90003, 90004, 90005);
