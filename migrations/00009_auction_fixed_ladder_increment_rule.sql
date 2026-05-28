-- +goose Up
-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_OBJECT(
  'type', 'ladder',
  'maxBidSteps', 10,
  'steps', JSON_ARRAY(
    JSON_OBJECT(
      'min', 0,
      'max', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[0].maxPriceCent')) AS UNSIGNED),
      'amount', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[0].stepCent')) AS UNSIGNED)
    ),
    JSON_OBJECT(
      'min', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[0].maxPriceCent')) AS UNSIGNED),
      'max', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[1].maxPriceCent')) AS UNSIGNED),
      'amount', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[1].stepCent')) AS UNSIGNED)
    ),
    JSON_OBJECT(
      'min', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[1].maxPriceCent')) AS UNSIGNED),
      'max', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[2].maxPriceCent')) AS UNSIGNED),
      'amount', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[2].stepCent')) AS UNSIGNED)
    ),
    JSON_OBJECT(
      'min', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[2].maxPriceCent')) AS UNSIGNED),
      'amount', CAST(JSON_UNQUOTE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[3].stepCent')) AS UNSIGNED)
    )
  )
)
WHERE JSON_TYPE(JSON_EXTRACT(`rule_snapshot`, '$.incrementRule')) = 'ARRAY'
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[0].maxPriceCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[0].stepCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[1].maxPriceCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[1].stepCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[2].maxPriceCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[2].stepCent') IS NOT NULL
  AND JSON_EXTRACT(`rule_snapshot`, '$.incrementRule[3].stepCent') IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_SET(`increment_rule`, '$.type', 'fixed')
WHERE JSON_EXTRACT(`increment_rule`, '$.type') IS NULL
  AND JSON_EXTRACT(`increment_rule`, '$.amount') IS NOT NULL
  AND JSON_EXTRACT(`increment_rule`, '$.maxBidSteps') IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_SET(`increment_rule`, '$.maxBidSteps', 10)
WHERE JSON_UNQUOTE(JSON_EXTRACT(`increment_rule`, '$.type')) IN ('fixed', 'ladder')
  AND JSON_EXTRACT(`increment_rule`, '$.maxBidSteps') IS NULL;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_OBJECT('type', 'fixed', 'amount', 100, 'maxBidSteps', 10)
WHERE JSON_EXTRACT(`increment_rule`, '$.type') IS NULL;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `increment_rule` JSON NOT NULL COMMENT '加价规则 JSON：fixed 固定加价；ladder 阶梯加价；maxBidSteps 单次最高加价步数';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_OBJECT(
  'amount',
  COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(`increment_rule`, '$.steps[0].amount')) AS UNSIGNED), 100),
  'maxBidSteps',
  COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(`increment_rule`, '$.maxBidSteps')) AS UNSIGNED), 10)
)
WHERE JSON_UNQUOTE(JSON_EXTRACT(`increment_rule`, '$.type')) = 'ladder';
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `increment_rule` = JSON_REMOVE(`increment_rule`, '$.type')
WHERE JSON_UNQUOTE(JSON_EXTRACT(`increment_rule`, '$.type')) = 'fixed';
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `increment_rule` JSON NOT NULL COMMENT '固定加价规则 JSON：amount 固定加价金额（分），maxBidSteps 单次最高加价步数';
-- +goose StatementEnd
