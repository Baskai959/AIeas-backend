-- +goose Up
-- +goose StatementBegin
CREATE TEMPORARY TABLE `tmp_live_session_backfill` AS
SELECT
  lr.`id` AS `live_room_id`,
  lr.`merchant_id` AS `merchant_id`,
  CONCAT('[系统补录]', lr.`title`) AS `session_title`,
  MIN(COALESCE(al.`start_time`, al.`created_at`)) AS `opened_at`,
  COALESCE(MAX(COALESCE(al.`closed_at`, al.`end_time`, al.`updated_at`)), NOW(3)) AS `closed_at`,
  COUNT(*) AS `lots_total`,
  SUM(CASE WHEN al.`status` IN ('CLOSED_WON', 'SETTLED') THEN 1 ELSE 0 END) AS `lots_sold`,
  SUM(CASE WHEN al.`status` = 'CLOSED_FAILED' THEN 1 ELSE 0 END) AS `lots_unsold`,
  (
    SELECT COUNT(*)
    FROM `bid_record` br
    JOIN `auction_lot` bal ON bal.`auction_id` = br.`auction_id`
    WHERE bal.`live_room_id` = lr.`id`
      AND br.`live_session_id` IS NULL
  ) AS `bid_count`,
  SUM(CASE WHEN al.`status` IN ('CLOSED_WON', 'SETTLED') THEN COALESCE(al.`deal_price`, 0) ELSE 0 END) AS `gmv_cent`
FROM `live_room` lr
JOIN `auction_lot` al ON al.`live_room_id` = lr.`id`
WHERE al.`live_room_id` <> 0
  AND al.`live_session_id` IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM `live_session` existing
    WHERE existing.`live_room_id` = lr.`id`
      AND existing.`title` = CONCAT('[系统补录]', lr.`title`)
  )
  AND al.`status` IN ('CLOSED_WON', 'CLOSED_FAILED', 'SETTLED')
GROUP BY lr.`id`, lr.`merchant_id`, lr.`title`;
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO `live_session` (
  `live_room_id`, `merchant_id`, `title`, `status`, `opened_at`, `closed_at`,
  `lots_total`, `lots_sold`, `lots_unsold`, `bid_count`, `gmv_cent`,
  `viewer_peak`, `viewer_total`, `created_at`, `updated_at`
)
SELECT
  t.`live_room_id`, t.`merchant_id`, t.`session_title`, 'ENDED', t.`opened_at`, t.`closed_at`,
  t.`lots_total`, t.`lots_sold`, t.`lots_unsold`, t.`bid_count`, t.`gmv_cent`,
  0, 0, NOW(3), NOW(3)
FROM `tmp_live_session_backfill` t
WHERE NOT EXISTS (
  SELECT 1
  FROM `live_session` ls
  WHERE ls.`live_room_id` = t.`live_room_id`
    AND ls.`title` = t.`session_title`
);
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot` al
JOIN `tmp_live_session_backfill` t ON t.`live_room_id` = al.`live_room_id`
JOIN `live_session` ls ON ls.`live_room_id` = t.`live_room_id` AND ls.`title` = t.`session_title`
SET al.`live_session_id` = ls.`id`
WHERE al.`live_session_id` IS NULL
  AND al.`status` IN ('CLOSED_WON', 'CLOSED_FAILED', 'SETTLED');
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
DROP TEMPORARY TABLE IF EXISTS `tmp_live_session_backfill`;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE `bid_record` br
JOIN `auction_lot` al ON al.`auction_id` = br.`auction_id`
JOIN `live_session` ls ON ls.`id` = al.`live_session_id`
SET br.`live_session_id` = NULL
WHERE ls.`title` LIKE '[系统补录]%';
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `order_deal` od
JOIN `auction_lot` al ON al.`auction_id` = od.`auction_id`
JOIN `live_session` ls ON ls.`id` = al.`live_session_id`
SET od.`live_session_id` = NULL
WHERE ls.`title` LIKE '[系统补录]%';
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot` al
JOIN `live_session` ls ON ls.`id` = al.`live_session_id`
SET al.`live_session_id` = NULL
WHERE ls.`title` LIKE '[系统补录]%';
-- +goose StatementEnd

-- +goose StatementBegin
DELETE FROM `live_session`
WHERE `title` LIKE '[系统补录]%';
-- +goose StatementEnd
