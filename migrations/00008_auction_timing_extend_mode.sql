-- +goose Up
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `anti_extend_mode` VARCHAR(16) NOT NULL DEFAULT 'ADD' COMMENT '反狙击延时模式：ADD=结束时间增加 anti_extend_sec；RESET=倒计时重置为 anti_extend_sec' AFTER `anti_extend_sec`,
  ADD COLUMN `duration_sec` INT NOT NULL DEFAULT 0 COMMENT '拍卖时长（秒），0 表示未预设，可在上架/激活时指定' AFTER `end_time`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `start_time` DATETIME(3) NULL COMMENT '开拍时间；NULL 表示未设置定时开拍',
  MODIFY COLUMN `end_time` DATETIME(3) NULL COMMENT '计划结束时间（可被反狙击延长）；NULL 表示启动时按时长计算';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE `auction_lot`
SET `start_time` = COALESCE(`start_time`, `created_at`),
    `end_time` = COALESCE(
      `end_time`,
      DATE_ADD(COALESCE(`start_time`, `created_at`), INTERVAL IF(`duration_sec` > 0, `duration_sec`, 3600) SECOND)
    );
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  MODIFY COLUMN `start_time` DATETIME(3) NOT NULL COMMENT '开拍时间',
  MODIFY COLUMN `end_time` DATETIME(3) NOT NULL COMMENT '计划结束时间（可被反狙击延长）';
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP COLUMN `duration_sec`,
  DROP COLUMN `anti_extend_mode`;
-- +goose StatementEnd
