-- +goose Up
SET NAMES utf8mb4;
SET time_zone = '+08:00';

CREATE TABLE IF NOT EXISTS `user` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '用户 ID',
  `account`       VARCHAR(64)     NOT NULL                COMMENT '登录账号',
  `phone`         VARCHAR(32)     DEFAULT NULL            COMMENT '手机号',
  `nickname`      VARCHAR(64)     NOT NULL                COMMENT '昵称',
  `password_hash` CHAR(64)        NOT NULL                COMMENT '密码哈希',
  `avatar_url`    VARCHAR(512)    DEFAULT NULL            COMMENT '头像 URL',
  `role`          VARCHAR(16)     NOT NULL DEFAULT 'buyer' COMMENT '角色：buyer/merchant/admin',
  `status`        VARCHAR(16)     NOT NULL DEFAULT 'ACTIVE' COMMENT '账号状态：ACTIVE/DISABLED',
  `last_login_at` DATETIME(3)     DEFAULT NULL            COMMENT '最近登录时间',
  `created_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  UNIQUE KEY `uk_phone` (`phone`),
  KEY `idx_role_status` (`role`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户表';

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

CREATE TABLE IF NOT EXISTS `auction_lot` (
  `auction_id`       BIGINT          NOT NULL                  COMMENT '拍品 ID（业务侧分配，非自增）',
  `item_id`          BIGINT          NOT NULL                  COMMENT '关联商品 ID（item.id）',
  `seller_id`        BIGINT          NOT NULL                  COMMENT '卖家 ID（user.id）',
  `auction_type`     VARCHAR(16)     NOT NULL DEFAULT 'ENGLISH' COMMENT '拍卖类型，MVP 仅支持 ENGLISH',
  `start_price`      BIGINT          NOT NULL                  COMMENT '起拍价（分），允许为 0（0 元起拍）',
  `reserve_price`    BIGINT          NOT NULL DEFAULT 0        COMMENT '保留价（分），0 表示无保留价',
  `increment_rule`   JSON            NOT NULL                  COMMENT '增价规则（阶梯加价）JSON',
  `anti_sniping_sec` INT             NOT NULL DEFAULT 15       COMMENT '反狙击触发窗口（秒）',
  `anti_extend_sec`  INT             NOT NULL DEFAULT 30       COMMENT '反狙击延长时长（秒）',
  `deposit_amount`   BIGINT          NOT NULL                  COMMENT '保证金金额（分）',
  `status`           VARCHAR(32)     NOT NULL                  COMMENT '状态：DRAFT/PENDING_AUDIT/READY/WARMING_UP/RUNNING/EXTENDED/HAMMER_PENDING/CLOSED_WON/CLOSED_FAILED/SETTLED',
  `rule_snapshot`    JSON            NOT NULL                  COMMENT '规则快照（不可变）JSON',
  `start_time`       DATETIME(3)     NOT NULL                  COMMENT '开拍时间',
  `end_time`         DATETIME(3)     NOT NULL                  COMMENT '计划结束时间（可被反狙击延长）',
  `winner_id`        BIGINT UNSIGNED DEFAULT NULL              COMMENT '中拍用户 ID（user.id）',
  `deal_price`       BIGINT          DEFAULT NULL              COMMENT '成交价（分）',
  `closed_at`        DATETIME(3)     DEFAULT NULL              COMMENT '实际落锤时间',
  `closed_by`        VARCHAR(32)     DEFAULT NULL              COMMENT '落锤来源：AUTO/ADMIN/RISK_TERMINATE',
  `created_at`       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`auction_id`),
  KEY `idx_item_id` (`item_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_end_time` (`status`, `end_time`),
  KEY `idx_winner_id` (`winner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='拍品（拍卖实例）表';

CREATE TABLE IF NOT EXISTS `bid_record` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '出价记录 ID',
  `request_id`    VARCHAR(64)     NOT NULL                COMMENT '客户端幂等键（UUID）',
  `auction_id`    BIGINT          NOT NULL                COMMENT '拍品 ID（auction_lot.auction_id）',
  `bidder_id`     BIGINT UNSIGNED NOT NULL                COMMENT '出价人 ID（user.id）',
  `bid_price`     BIGINT          NOT NULL                COMMENT '出价金额（分）',
  `bid_ts_ms`     BIGINT          NOT NULL                COMMENT '服务端受理时间戳（毫秒）',
  `source`        VARCHAR(32)     NOT NULL DEFAULT 'live_ws' COMMENT '出价来源：live_ws/http/admin_proxy',
  `risk_result`   VARCHAR(32)     NOT NULL DEFAULT 'ALLOW' COMMENT '风控结果：ALLOW/REJECT/REVIEW',
  `reject_reason` VARCHAR(64)     DEFAULT NULL            COMMENT '拒绝原因，如 BELOW_MIN_INC / BLACKLIST',
  `created_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_request_id` (`request_id`),
  KEY `idx_auction_bid_ts` (`auction_id`, `bid_ts_ms`),
  KEY `idx_bidder_id` (`bidder_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='出价记录表';

CREATE TABLE IF NOT EXISTS `order_deal` (
  `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '订单 ID',
  `auction_id`     BIGINT          NOT NULL                COMMENT '拍品 ID（auction_lot.auction_id）',
  `winner_id`      BIGINT UNSIGNED NOT NULL                COMMENT '中拍人 ID（user.id）',
  `seller_id`      BIGINT UNSIGNED NOT NULL                COMMENT '卖家 ID（user.id）',
  `deal_price`     BIGINT          NOT NULL                COMMENT '成交价（分）',
  `deposit_amount` BIGINT          NOT NULL DEFAULT 0      COMMENT '已冻结保证金金额（分）',
  `status`         VARCHAR(16)     NOT NULL DEFAULT 'CREATED' COMMENT '订单状态：CREATED/PAID/TIMEOUT/CANCELLED',
  `pay_status`     VARCHAR(16)     NOT NULL DEFAULT 'UNPAID' COMMENT '支付状态：UNPAID/PAID/REFUNDED',
  `pay_deadline`   DATETIME(3)     DEFAULT NULL            COMMENT '支付截止时间',
  `paid_at`        DATETIME(3)     DEFAULT NULL            COMMENT '支付完成时间',
  `closed_at`      DATETIME(3)     DEFAULT NULL            COMMENT '订单关闭时间',
  `created_at`     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_auction_id` (`auction_id`),
  KEY `idx_winner_id` (`winner_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_pay_deadline` (`status`, `pay_deadline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='成交订单表';

CREATE TABLE IF NOT EXISTS `deposit_ledger` (
  `id`               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '账本 ID',
  `auction_id`       BIGINT          NOT NULL                COMMENT '拍品 ID（auction_lot.auction_id）',
  `user_id`          BIGINT UNSIGNED NOT NULL                COMMENT '用户 ID（user.id）',
  `amount`           BIGINT          NOT NULL                COMMENT '保证金金额（分）',
  `status`           VARCHAR(16)     NOT NULL                COMMENT '状态：PENDING/READY/CAPTURED/RELEASED/FAILED',
  `related_order_id` BIGINT UNSIGNED DEFAULT NULL            COMMENT '关联订单 ID（order_deal.id），CAPTURED 时绑定',
  `remark`           VARCHAR(256)    DEFAULT NULL            COMMENT '备注',
  `created_at`       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_auction_user` (`auction_id`, `user_id`),
  KEY `idx_user_status` (`user_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='保证金账本';

CREATE TABLE IF NOT EXISTS `audit_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '审计日志 ID',
  `operator_id`   BIGINT UNSIGNED NOT NULL                COMMENT '操作者 ID（user.id）',
  `operator_role` VARCHAR(16)     NOT NULL                COMMENT '操作者角色：user/merchant/admin',
  `action`        VARCHAR(64)     NOT NULL                COMMENT '动作枚举，如 AUCTION_AUDIT_PASS/HAMMER/BLACKLIST_ADD',
  `target_type`   VARCHAR(32)     NOT NULL                COMMENT '目标类型：AUCTION/ORDER/USER/ITEM',
  `target_id`     VARCHAR(64)     NOT NULL                COMMENT '目标 ID（字符串以兼容业务态主键）',
  `payload`       JSON            DEFAULT NULL            COMMENT '请求/业务上下文 JSON',
  `ip`            VARCHAR(64)     DEFAULT NULL            COMMENT '操作 IP',
  `ua`            VARCHAR(256)    DEFAULT NULL            COMMENT 'User-Agent',
  `created_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_operator_created` (`operator_id`, `created_at`),
  KEY `idx_target` (`target_type`, `target_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='审计日志';

CREATE TABLE IF NOT EXISTS `blacklist` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '黑名单 ID',
  `user_id`    BIGINT UNSIGNED NOT NULL                COMMENT '被拉黑用户 ID（user.id）',
  `reason`     VARCHAR(256)    NOT NULL                COMMENT '拉黑原因',
  `created_by` BIGINT UNSIGNED NOT NULL                COMMENT '操作者 ID（user.id）',
  `created_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `expires_at` DATETIME(3)     DEFAULT NULL            COMMENT '过期时间，NULL 表示永久',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_id` (`user_id`),
  KEY `idx_expires_at` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='黑名单';

CREATE TABLE IF NOT EXISTS `risk_event` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '风险事件 ID',
  `event_type`  VARCHAR(32)     NOT NULL                COMMENT '事件类型：BID_FREQ/SHILL_BIDDING/ABUSE_RETRY 等',
  `user_id`     BIGINT UNSIGNED DEFAULT NULL            COMMENT '涉及用户 ID（user.id）',
  `auction_id`  BIGINT          DEFAULT NULL            COMMENT '涉及拍品 ID（auction_lot.auction_id）',
  `severity`    VARCHAR(8)      NOT NULL DEFAULT 'LOW'  COMMENT '严重度：LOW/MID/HIGH',
  `payload`     JSON            DEFAULT NULL            COMMENT '命中详情 JSON',
  `status`      VARCHAR(16)     NOT NULL DEFAULT 'PENDING' COMMENT '处理状态：PENDING/REVIEWED/IGNORED',
  `reviewed_by` BIGINT UNSIGNED DEFAULT NULL            COMMENT '复核人 ID（user.id）',
  `reviewed_at` DATETIME(3)     DEFAULT NULL            COMMENT '复核时间',
  `created_at`  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_status_created` (`status`, `created_at`),
  KEY `idx_auction_user` (`auction_id`, `user_id`),
  KEY `idx_event_type` (`event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='风险事件表';

CREATE TABLE IF NOT EXISTS `config_item` (
  `config_key`   VARCHAR(64)     NOT NULL                COMMENT '配置键（如 default.deposit_ratio）',
  `config_value` JSON            NOT NULL                COMMENT '配置值（JSON）',
  `description`  VARCHAR(256)    DEFAULT NULL            COMMENT '配置项说明',
  `updated_by`   BIGINT UNSIGNED DEFAULT NULL            COMMENT '最近修改者 ID（user.id）',
  `updated_at`   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`config_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='平台级配置项';

-- +goose Down
DROP TABLE IF EXISTS `config_item`;
DROP TABLE IF EXISTS `risk_event`;
DROP TABLE IF EXISTS `blacklist`;
DROP TABLE IF EXISTS `audit_log`;
DROP TABLE IF EXISTS `deposit_ledger`;
DROP TABLE IF EXISTS `order_deal`;
DROP TABLE IF EXISTS `bid_record`;
DROP TABLE IF EXISTS `auction_lot`;
DROP TABLE IF EXISTS `item`;
DROP TABLE IF EXISTS `user`;
