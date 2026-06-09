-- +goose Up
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `audit_reject_reason` TEXT NULL COMMENT '拍品内容审核未通过原因' AFTER `audit_task_id`;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP COLUMN `audit_reject_reason`;
-- +goose StatementEnd
