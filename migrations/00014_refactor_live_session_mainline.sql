-- +goose Up
-- +goose StatementBegin
ALTER TABLE `live_session`
  MODIFY COLUMN `live_room_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '废弃回滚字段：历史所属直播间 ID',
  MODIFY COLUMN `opened_at` DATETIME(3) DEFAULT NULL COMMENT '实际开播时间',
  MODIFY COLUMN `status` VARCHAR(16) NOT NULL COMMENT '状态：DRAFT/SCHEDULED/LIVE/ENDED/CANCELLED',
  ADD COLUMN `description` TEXT NULL COMMENT '直播场次描述' AFTER `title`,
  ADD COLUMN `cover_url` VARCHAR(1024) DEFAULT NULL COMMENT '直播场次封面 URL' AFTER `description`,
  ADD COLUMN `active_auction_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前讲解/在拍 auction_id，0=无' AFTER `status`,
  ADD COLUMN `scheduled_start_time` DATETIME(3) DEFAULT NULL COMMENT '计划开播时间' AFTER `closed_at`,
  ADD COLUMN `planned_duration_sec` INT NOT NULL DEFAULT 0 COMMENT '计划直播时长（秒）' AFTER `scheduled_start_time`,
  ADD COLUMN `live_merchant_id` VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN `status` = 'LIVE' THEN `merchant_id` ELSE NULL END) STORED COMMENT '同商家同时仅一个 LIVE 的唯一键列' AFTER `merchant_id`,
  ADD KEY `idx_live_session_active_auction` (`active_auction_id`),
  ADD KEY `idx_live_session_status_schedule` (`status`, `scheduled_start_time`);
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `live_session` ls
JOIN `live_room` lr ON lr.`id` = ls.`live_room_id`
SET
  ls.`title` = COALESCE(NULLIF(ls.`title`, ''), lr.`title`),
  ls.`description` = COALESCE(ls.`description`, lr.`description`),
  ls.`cover_url` = COALESCE(ls.`cover_url`, lr.`cover_url`),
  ls.`active_auction_id` = CASE WHEN ls.`status` = 'LIVE' THEN COALESCE(lr.`active_auction_id`, 0) ELSE ls.`active_auction_id` END,
  ls.`updated_at` = NOW(3)
WHERE ls.`live_room_id` <> 0;
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO `live_session` (
  `live_room_id`, `merchant_id`, `title`, `description`, `cover_url`, `status`, `active_auction_id`,
  `opened_at`, `closed_at`, `scheduled_start_time`, `planned_duration_sec`,
  `lots_total`, `lots_sold`, `lots_unsold`, `bid_count`, `gmv_cent`, `viewer_peak`, `viewer_total`,
  `created_at`, `updated_at`
)
SELECT
  lr.`id`, lr.`merchant_id`, lr.`title`, lr.`description`, lr.`cover_url`,
  CASE WHEN lr.`status` = 'LIVE' THEN 'LIVE' ELSE 'ENDED' END,
  CASE WHEN lr.`status` = 'LIVE' THEN COALESCE(lr.`active_auction_id`, 0) ELSE 0 END,
  COALESCE(MIN(al.`start_time`), lr.`created_at`, NOW(3)),
  CASE WHEN lr.`status` = 'LIVE' THEN NULL ELSE COALESCE(MAX(COALESCE(al.`closed_at`, al.`end_time`, al.`updated_at`)), lr.`updated_at`, NOW(3)) END,
  NULL,
  0,
  COUNT(al.`auction_id`),
  SUM(CASE WHEN al.`status` IN ('CLOSED_WON', 'SETTLED') THEN 1 ELSE 0 END),
  SUM(CASE WHEN al.`status` = 'CLOSED_FAILED' THEN 1 ELSE 0 END),
  COALESCE((SELECT COUNT(*) FROM `bid_record` br JOIN `auction_lot` bal ON bal.`auction_id` = br.`auction_id` WHERE bal.`live_room_id` = lr.`id`), 0),
  COALESCE(SUM(CASE WHEN al.`status` IN ('CLOSED_WON', 'SETTLED') THEN COALESCE(al.`deal_price`, 0) ELSE 0 END), 0),
  0,
  0,
  NOW(3),
  NOW(3)
FROM `live_room` lr
LEFT JOIN `auction_lot` al ON al.`live_room_id` = lr.`id`
WHERE NOT EXISTS (SELECT 1 FROM `live_session` existing WHERE existing.`live_room_id` = lr.`id`)
GROUP BY lr.`id`, lr.`merchant_id`, lr.`title`, lr.`description`, lr.`cover_url`, lr.`status`, lr.`active_auction_id`, lr.`created_at`, lr.`updated_at`;
-- +goose StatementEnd

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
UPDATE `live_session` ls
JOIN (
  SELECT `merchant_id`, MAX(`id`) AS `keep_id`
  FROM `live_session`
  WHERE `status` = 'LIVE'
  GROUP BY `merchant_id`
  HAVING COUNT(*) > 1
) dup ON dup.`merchant_id` = ls.`merchant_id`
SET ls.`status` = 'ENDED',
    ls.`active_auction_id` = 0,
    ls.`closed_at` = COALESCE(ls.`closed_at`, NOW(3)),
    ls.`updated_at` = NOW(3)
WHERE ls.`status` = 'LIVE'
  AND ls.`id` <> dup.`keep_id`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `live_session`
  ADD UNIQUE KEY `uk_live_session_one_live_per_merchant` (`live_merchant_id`);
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

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `live_session`
  DROP KEY `idx_live_session_status_schedule`,
  DROP KEY `idx_live_session_active_auction`,
  DROP KEY `uk_live_session_one_live_per_merchant`,
  DROP COLUMN `live_merchant_id`,
  DROP COLUMN `planned_duration_sec`,
  DROP COLUMN `scheduled_start_time`,
  DROP COLUMN `active_auction_id`,
  DROP COLUMN `cover_url`,
  DROP COLUMN `description`,
  MODIFY COLUMN `opened_at` DATETIME(3) NOT NULL COMMENT '开播时间',
  MODIFY COLUMN `live_room_id` BIGINT UNSIGNED NOT NULL COMMENT '所属直播间 ID（live_room.id）',
  MODIFY COLUMN `status` VARCHAR(16) NOT NULL COMMENT '状态：LIVE/ENDED';
-- +goose StatementEnd
