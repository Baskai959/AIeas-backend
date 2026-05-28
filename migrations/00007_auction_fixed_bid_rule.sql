-- +goose Up
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `cap_price` BIGINT NOT NULL DEFAULT 0 COMMENT '封顶价（分），0 表示无封顶价；达到该价格自动成交' AFTER `reserve_price`;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `cap_price` = CASE
  WHEN `reserve_price` > `start_price`
    THEN `start_price` + CEIL((`reserve_price` - `start_price`) / 100) * 100
  ELSE `start_price` + 100
END
WHERE `cap_price` = 0;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_OBJECT('amount', 100, 'maxBidSteps', 10)
WHERE JSON_EXTRACT(`increment_rule`, '$.amount') IS NULL
   OR JSON_EXTRACT(`increment_rule`, '$.maxBidSteps') IS NULL
   OR JSON_EXTRACT(`increment_rule`, '$.type') IS NOT NULL
   OR JSON_EXTRACT(`increment_rule`, '$.steps') IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `increment_rule` JSON NOT NULL COMMENT '固定加价规则 JSON：amount 固定加价金额（分），maxBidSteps 单次最高加价步数';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `increment_rule` JSON NOT NULL COMMENT '增价规则（阶梯加价）JSON';
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP COLUMN `cap_price`;
-- +goose StatementEnd
