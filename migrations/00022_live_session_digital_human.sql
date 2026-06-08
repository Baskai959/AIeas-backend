-- +goose Up
-- +goose StatementBegin
ALTER TABLE `live_session`
  ADD COLUMN `is_digital_human` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否数字人直播间：1=数字人/0=普通直播(recorded)' AFTER `status`;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `live_session`
  DROP COLUMN `is_digital_human`;
-- +goose StatementEnd
