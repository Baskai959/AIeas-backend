-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS `merchant_follow` (
  `buyer_id` varchar(64) NOT NULL COMMENT '关注用户 ID',
  `merchant_id` varchar(64) NOT NULL COMMENT '被关注商家用户 ID',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '关注时间',
  PRIMARY KEY (`buyer_id`, `merchant_id`),
  KEY `idx_merchant_follow_merchant` (`merchant_id`, `created_at`),
  KEY `idx_merchant_follow_buyer` (`buyer_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户关注商家关系表';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS `merchant_follow`;
-- +goose StatementEnd
