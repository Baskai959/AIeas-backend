-- +goose Up
ALTER TABLE `auction_lot`
  ADD COLUMN `version` BIGINT NOT NULL DEFAULT 0 COMMENT '行级乐观锁版本号，仅由落槌 CAS 路径递增';

-- +goose Down
ALTER TABLE `auction_lot`
  DROP COLUMN `version`;
