-- +goose Up
-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `title` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '拍品标题（原商品标题）' AFTER `live_session_id`,
  ADD COLUMN `description` TEXT NULL COMMENT '拍品描述（原商品描述）' AFTER `title`,
  ADD COLUMN `category` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '拍品类目（原商品类目）' AFTER `description`,
  ADD COLUMN `brand` VARCHAR(64) DEFAULT NULL COMMENT '品牌' AFTER `category`,
  ADD COLUMN `condition_grade` VARCHAR(16) NOT NULL DEFAULT 'GOOD' COMMENT '成色：NEW/LIKE_NEW/GOOD/FAIR' AFTER `brand`,
  ADD COLUMN `image_urls` JSON DEFAULT NULL COMMENT '拍品图片 URL 列表' AFTER `condition_grade`,
  ADD COLUMN `cover_url` VARCHAR(1024) DEFAULT NULL COMMENT '拍品封面 URL' AFTER `image_urls`,
  ADD KEY `idx_lot_category` (`category`),
  ADD KEY `idx_lot_title` (`title`);
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot` al
JOIN `item` i ON al.`item_id` = i.`id`
SET al.`title` = COALESCE(NULLIF(i.`title`, ''), CONCAT('拍品 ', al.`auction_id`)),
    al.`description` = i.`description`,
    al.`category` = COALESCE(NULLIF(i.`category`, ''), '未分类'),
    al.`brand` = i.`brand`,
    al.`condition_grade` = COALESCE(NULLIF(i.`condition_grade`, ''), 'GOOD'),
    al.`image_urls` = i.`images`,
    al.`cover_url` = JSON_UNQUOTE(JSON_EXTRACT(i.`images`, '$[0]'));
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `auction_lot`
SET `title` = IF(`title` = '', CONCAT('拍品 ', `auction_id`), `title`),
    `category` = IF(`category` = '', '未分类', `category`),
    `condition_grade` = IF(`condition_grade` = '', 'GOOD', `condition_grade`),
    `image_urls` = IF(`image_urls` IS NULL, JSON_ARRAY(), `image_urls`),
    `cover_url` = IF(`cover_url` IS NULL OR `cover_url` = '', JSON_UNQUOTE(JSON_EXTRACT(`image_urls`, '$[0]')), `cover_url`);
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `order_deal`
  ADD COLUMN `lot_snapshot` JSON DEFAULT NULL COMMENT '成交时拍品展示与拍卖规则快照' AFTER `live_session_id`;
-- +goose StatementEnd

-- +goose StatementBegin
UPDATE `order_deal` od
JOIN `auction_lot` al ON od.`auction_id` = al.`auction_id`
SET od.`lot_snapshot` = JSON_OBJECT(
  'auctionId', al.`auction_id`,
  'sellerId', al.`seller_id`,
  'liveSessionId', al.`live_session_id`,
  'title', al.`title`,
  'description', al.`description`,
  'category', al.`category`,
  'brand', al.`brand`,
  'condition', al.`condition_grade`,
  'imageUrls', COALESCE(al.`image_urls`, JSON_ARRAY()),
  'coverUrl', al.`cover_url`,
  'startPrice', al.`start_price`,
  'reservePrice', al.`reserve_price`,
  'capPrice', al.`cap_price`,
  'incrementRule', al.`increment_rule`,
  'depositAmount', al.`deposit_amount`,
  'dealPrice', od.`deal_price`,
  'winnerId', od.`winner_id`,
  'closedAt', od.`created_at`
)
WHERE od.`lot_snapshot` IS NULL;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP KEY `idx_item_id`,
  DROP COLUMN `item_id`;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS `item`;
-- +goose StatementEnd

-- +goose Down
-- Down 只恢复旧结构；Up 已物理删除 item 表，历史商品行不可恢复。
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS `item` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '商品 ID',
  `seller_id`       BIGINT UNSIGNED NOT NULL                COMMENT '卖家 ID（关联 user.id）',
  `title`           VARCHAR(128)    NOT NULL                COMMENT '商品标题',
  `category`        VARCHAR(64)     NOT NULL                COMMENT '分类',
  `brand`           VARCHAR(64)     DEFAULT NULL            COMMENT '品牌',
  `condition_grade` VARCHAR(16)     NOT NULL DEFAULT 'NEW'  COMMENT '成色：NEW/LIKE_NEW/GOOD/FAIR',
  `images`          JSON            NOT NULL                COMMENT '图片 URL 数组（JSON）',
  `description`     TEXT            DEFAULT NULL            COMMENT '商品描述',
  `status`          VARCHAR(16)     NOT NULL DEFAULT 'DRAFT' COMMENT '状态：DRAFT/READY/LISTED/OFFLINE',
  `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_seller_status` (`seller_id`, `status`),
  KEY `idx_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品表';
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  ADD COLUMN `item_id` BIGINT NOT NULL DEFAULT 0 COMMENT '关联商品 ID（回滚兼容）' AFTER `auction_id`,
  ADD KEY `idx_item_id` (`item_id`);
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `order_deal`
  DROP COLUMN `lot_snapshot`;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE `auction_lot`
  DROP KEY `idx_lot_title`,
  DROP KEY `idx_lot_category`,
  DROP COLUMN `cover_url`,
  DROP COLUMN `image_urls`,
  DROP COLUMN `condition_grade`,
  DROP COLUMN `brand`,
  DROP COLUMN `category`,
  DROP COLUMN `description`,
  DROP COLUMN `title`;
-- +goose StatementEnd
