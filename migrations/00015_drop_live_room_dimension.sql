-- +goose Up
-- +goose StatementBegin
UPDATE `auction_lot` al
JOIN (
  SELECT ls1.`live_room_id`, ls1.`id`
  FROM `live_session` ls1
  JOIN (
    SELECT `live_room_id`, MAX(`id`) AS `id`
    FROM `live_session`
    WHERE `live_room_id` <> 0
    GROUP BY `live_room_id`
  ) latest ON latest.`id` = ls1.`id`
) pick ON pick.`live_room_id` = al.`live_room_id`
SET al.`live_session_id` = pick.`id`
WHERE al.`live_session_id` IS NULL
  AND al.`live_room_id` <> 0;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `bid_record` br
JOIN `auction_lot` al ON al.`auction_id` = br.`auction_id`
SET br.`live_session_id` = al.`live_session_id`
WHERE br.`live_session_id` IS NULL
  AND al.`live_session_id` IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `order_deal` od
JOIN `auction_lot` al ON al.`auction_id` = od.`auction_id`
SET od.`live_session_id` = al.`live_session_id`
WHERE od.`live_session_id` IS NULL
  AND al.`live_session_id` IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP KEY `idx_live_room_status`,
  DROP COLUMN `live_room_id`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `live_session`
  DROP KEY `idx_room_status`,
  DROP COLUMN `live_room_id`;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS `live_room`;
-- +goose StatementEnd

-- +goose Down
-- 结构级回滚：只恢复 live_room 表和 live_room_id 字段，无法恢复已删除的历史直播间数据。
-- +goose StatementBegin
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
  UNIQUE KEY `uk_live_room_merchant` (`merchant_id`),
  KEY `idx_merchant_status` (`merchant_id`, `status`),
  KEY `idx_active_auction` (`active_auction_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播间（结构级回滚占位）';
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `live_room_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '结构级回滚字段：历史所属直播间 ID' AFTER `seller_id`,
  ADD KEY `idx_live_room_status` (`live_room_id`, `status`);
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `live_session`
  ADD COLUMN `live_room_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '结构级回滚字段：历史所属直播间 ID' AFTER `id`,
  ADD KEY `idx_room_status` (`live_room_id`, `status`);
-- +goose StatementEnd
