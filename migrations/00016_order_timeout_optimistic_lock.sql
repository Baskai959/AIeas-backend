-- +goose Up
ALTER TABLE `order_deal`
  ADD COLUMN `version` BIGINT NOT NULL DEFAULT 0 COMMENT '行级乐观锁版本号，支付/超时关单 CAS 路径递增' AFTER `closed_at`;

INSERT INTO `config_item` (`config_key`, `config_value`, `description`)
VALUES ('order.pay_timeout_sec', JSON_OBJECT('value', 1200), '订单支付超时（秒）')
ON DUPLICATE KEY UPDATE
  `config_value` = VALUES(`config_value`),
  `description` = VALUES(`description`),
  `updated_at` = CURRENT_TIMESTAMP(3);

-- +goose Down
ALTER TABLE `order_deal`
  DROP COLUMN `version`;

UPDATE `config_item`
SET `config_value` = JSON_OBJECT('value', 1800),
    `description` = '订单支付超时（秒）',
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `config_key` = 'order.pay_timeout_sec';
