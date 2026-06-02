-- +goose Up
ALTER TABLE `auction_lot`
  ADD COLUMN `audit_task_id` VARCHAR(96) NOT NULL DEFAULT '' COMMENT '当前拍品内容审核任务 ID，用于过滤过期 AI 审核回调' AFTER `rule_snapshot`;

-- +goose Down
ALTER TABLE `auction_lot`
  DROP COLUMN `audit_task_id`;
