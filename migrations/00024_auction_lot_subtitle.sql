-- +goose Up
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `subtitle` VARCHAR(256) NOT NULL DEFAULT '' COMMENT '拍品短简介/副标题，用户端直播清单展示' AFTER `title`;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `subtitle` = COALESCE(NULLIF(`brand`, ''), '')
WHERE `subtitle` = '';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP COLUMN `subtitle`;
-- +goose StatementEnd
