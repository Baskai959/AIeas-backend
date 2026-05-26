-- +goose Up
-- +goose StatementBegin
SET NAMES utf8mb4;
-- +goose StatementEnd

-- ---------------------------------------------------------------------
-- live_session 直播场次表
--   一次"开播-闭播"周期；与 auction_lot/bid_record/order_deal 通过
--   live_session_id 反查关联。字符集/类型与 docs/aieas.sql 保持一致。
-- ---------------------------------------------------------------------
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS `live_session` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '直播场次 ID',
  `live_room_id`  BIGINT UNSIGNED NOT NULL                COMMENT '所属直播间 ID（live_room.id）',
  `merchant_id`   VARCHAR(64)     NOT NULL                COMMENT '商家 ID（冗余便于查询）',
  `title`         VARCHAR(255)    DEFAULT NULL            COMMENT '开播时直播间标题快照',
  `status`        VARCHAR(16)     NOT NULL                COMMENT '状态：LIVE/ENDED',
  `opened_at`     DATETIME(3)     NOT NULL                COMMENT '开播时间',
  `closed_at`     DATETIME(3)     DEFAULT NULL            COMMENT '闭播时间',
  `lots_total`    INT             NOT NULL DEFAULT 0      COMMENT '本场上架/挂载过的拍品数',
  `lots_sold`     INT             NOT NULL DEFAULT 0      COMMENT '本场成交数',
  `lots_unsold`   INT             NOT NULL DEFAULT 0      COMMENT '本场流拍数',
  `bid_count`     INT             NOT NULL DEFAULT 0      COMMENT '本场出价次数',
  `gmv_cent`      BIGINT          NOT NULL DEFAULT 0      COMMENT '本场成交总金额（分）',
  `viewer_peak`   INT             NOT NULL DEFAULT 0      COMMENT '峰值在线',
  `viewer_total`  INT             NOT NULL DEFAULT 0      COMMENT '累计观看人次（去重以 user_id）',
  `created_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_room_status` (`live_room_id`, `status`),
  KEY `idx_merchant_opened` (`merchant_id`, `opened_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播场次（一次开播-闭播）';
-- +goose StatementEnd

-- auction_lot 增加 live_session_id 列与反查索引
-- 列位置紧跟 live_room_id；类型/默认值与 docs/aieas.sql 保持一致
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `live_session_id` BIGINT UNSIGNED DEFAULT NULL COMMENT '所属直播场次 ID（NULL=未关联场次）' AFTER `live_room_id`,
  ADD KEY `idx_live_session` (`live_session_id`);
-- +goose StatementEnd

-- bid_record 增加 live_session_id 列与反查索引
-- 列位置紧跟 auction_id；类型/默认值与 docs/aieas.sql 保持一致
-- +goose StatementBegin
ALTER TABLE `bid_record`
  ADD COLUMN `live_session_id` BIGINT UNSIGNED DEFAULT NULL COMMENT '所属直播场次 ID（NULL=非场次内出价）' AFTER `auction_id`,
  ADD KEY `idx_live_session` (`live_session_id`);
-- +goose StatementEnd

-- order_deal 增加 live_session_id 列与反查索引
-- 列位置紧跟 auction_id；类型/默认值与 docs/aieas.sql 保持一致
-- +goose StatementBegin
ALTER TABLE `order_deal`
  ADD COLUMN `live_session_id` BIGINT UNSIGNED DEFAULT NULL COMMENT '所属直播场次 ID（NULL=非场次内成交）' AFTER `auction_id`,
  ADD KEY `idx_live_session` (`live_session_id`);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `order_deal`
  DROP KEY `idx_live_session`,
  DROP COLUMN `live_session_id`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `bid_record`
  DROP KEY `idx_live_session`,
  DROP COLUMN `live_session_id`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP KEY `idx_live_session`,
  DROP COLUMN `live_session_id`;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS `live_session`;
-- +goose StatementEnd
