-- +goose Up
SET NAMES utf8mb4;
SET time_zone = '+08:00';

CREATE TABLE IF NOT EXISTS `live_room` (
  `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '直播间 ID',
  `merchant_id`       BIGINT UNSIGNED NOT NULL                COMMENT '商家 ID（关联 user.id）',
  `title`             VARCHAR(128)    NOT NULL                COMMENT '直播间标题',
  `description`       VARCHAR(1024)   DEFAULT NULL            COMMENT '直播间描述',
  `cover_url`         VARCHAR(512)    DEFAULT NULL            COMMENT '封面 URL',
  `status`            VARCHAR(16)     NOT NULL DEFAULT 'OFFLINE' COMMENT '状态：OFFLINE/LIVE/CLOSED',
  `active_auction_id` BIGINT          DEFAULT NULL            COMMENT '当前在拍 lot ID（同时只能有一个）',
  `created_at`        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_merchant_status` (`merchant_id`, `status`),
  KEY `idx_active_auction` (`active_auction_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播间（拍卖房间）';

ALTER TABLE `auction_lot`
  ADD COLUMN `live_room_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '所属直播间 ID（0 表示未归属）' AFTER `seller_id`,
  ADD KEY `idx_live_room_status` (`live_room_id`, `status`);

-- +goose Down
ALTER TABLE `auction_lot`
  DROP KEY `idx_live_room_status`,
  DROP COLUMN `live_room_id`;
DROP TABLE IF EXISTS `live_room`;
