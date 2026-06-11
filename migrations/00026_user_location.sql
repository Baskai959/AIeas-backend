-- +goose Up
ALTER TABLE `user`
  ADD COLUMN `location` VARCHAR(64) DEFAULT NULL COMMENT '所在地' AFTER `avatar_url`;

-- +goose Down
ALTER TABLE `user`
  DROP COLUMN `location`;
