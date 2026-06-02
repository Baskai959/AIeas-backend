-- +goose Up
-- +goose StatementBegin
ALTER TABLE `user`
  ADD COLUMN `ai_permission` VARCHAR(16) NOT NULL DEFAULT 'ASK' COMMENT '商家 AI 控制权限：ASK=执行前询问/ALLOW=自动允许/DENY=自动拒绝' AFTER `status`;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `user`
SET `ai_permission` = 'ASK'
WHERE `ai_permission` = '' OR `ai_permission` IS NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `user`
  DROP COLUMN `ai_permission`;
-- +goose StatementEnd
