-- =============================================================================
-- update_auction_increment_rule.sql
-- 用途：将 seed_live_auction_demo.sql 插入的 5 个拍品
--       （auction_lot.auction_id = 90001 ~ 90005）的 increment_rule 字段
--       改写为当前统一的 ladder 阶梯加价格式。
--
-- 新结构（金额单位「分」）：
--   {
--     "type": "ladder",
--     "maxBidSteps": 5,
--     "steps": [
--       { "min":       0, "max":   50000, "amount":  1000 },
--       { "min":   50000, "max":  200000, "amount":  5000 },
--       { "min":  200000, "max": 1000000, "amount": 20000 },
--       { "min": 1000000,                 "amount": 50000 }
--     ]
--   }
--
-- 可重放性：
--   * 单条 UPDATE，幂等（多次执行结果一致）。
--   * 末尾 SELECT 打印更新后的 increment_rule，便于人工核对。
-- =============================================================================

SET NAMES utf8mb4;

UPDATE `auction_lot`
   SET `increment_rule` = JSON_OBJECT(
         'type', 'ladder',
         'maxBidSteps', 5,
         'steps', JSON_ARRAY(
                    JSON_OBJECT('min',       0, 'max',   50000, 'amount',  1000),
                    JSON_OBJECT('min',   50000, 'max',  200000, 'amount',  5000),
                    JSON_OBJECT('min',  200000, 'max', 1000000, 'amount', 20000),
                    JSON_OBJECT('min', 1000000,                 'amount', 50000)
                  )
       )
 WHERE `auction_id` IN (90001, 90002, 90003, 90004, 90005);

SELECT `auction_id`, `increment_rule`
  FROM `auction_lot`
 WHERE `auction_id` IN (90001, 90002, 90003, 90004, 90005);
