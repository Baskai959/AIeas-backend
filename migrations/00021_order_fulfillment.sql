-- +goose Up
ALTER TABLE `order_deal`
  ADD COLUMN `fulfillment_status` VARCHAR(16) NOT NULL DEFAULT 'UNSHIPPED' COMMENT '履约状态：UNSHIPPED/SHIPPED/RECEIVED' AFTER `pay_status`,
  ADD COLUMN `shipped_at` DATETIME(3) DEFAULT NULL COMMENT '发货时间' AFTER `paid_at`,
  ADD COLUMN `received_at` DATETIME(3) DEFAULT NULL COMMENT '收货时间' AFTER `shipped_at`,
  ADD KEY `idx_fulfillment_status` (`fulfillment_status`);

-- +goose Down
ALTER TABLE `order_deal`
  DROP KEY `idx_fulfillment_status`,
  DROP COLUMN `received_at`,
  DROP COLUMN `shipped_at`,
  DROP COLUMN `fulfillment_status`;
