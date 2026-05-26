-- =====================================================================
-- 实时竞拍大师 · MVP 初始化 DDL（MySQL 8.0）
-- 字符集: utf8mb4 / utf8mb4_0900_ai_ci
-- 引擎  : InnoDB
-- 时间  : DATETIME(3) 毫秒精度
-- 金额  : BIGINT，单位「分」
-- 主键  : 业务表统一 BIGINT UNSIGNED AUTO_INCREMENT
--         （auction_lot 沿用 auction_id BIGINT，由后端雪花 ID 生成）
-- 外键  : MVP 阶段不强制启用，关联关系在 COMMENT 中标注
-- =====================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';

-- ---------------------------------------------------------------------
-- 1. user 用户表
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 2. item 商品表
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `item` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '商品 ID',
  `seller_id`       BIGINT UNSIGNED NOT NULL                COMMENT '卖家 ID（关联 user.id）',
  `title`           VARCHAR(128)    NOT NULL                COMMENT '商品标题',
  `category`        VARCHAR(64)     NOT NULL                COMMENT '分类',
  `brand`           VARCHAR(64)     DEFAULT NULL            COMMENT '品牌',
  `condition_grade` VARCHAR(16)     NOT NULL DEFAULT 'NEW'  COMMENT '成色：NEW/LIKE_NEW/GOOD/FAIR',
  `images`          JSON            NOT NULL                COMMENT '图片 URL 数组（JSON）',
  `description`     TEXT            DEFAULT NULL            COMMENT '商品描述',
  `status`          VARCHAR(16)     NOT NULL DEFAULT 'PENDING_AUDIT' COMMENT '状态：DRAFT/PENDING_AUDIT/READY/REJECTED/LISTED/OFFLINE',
  `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_seller_status` (`seller_id`, `status`),
  KEY `idx_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品表';

-- ---------------------------------------------------------------------
-- 3. auction_lot 拍品表
--    与《直播竞拍系统技术设计方案.md》第 10.2 节对齐
--    补充字段: winner_id / deal_price / closed_at / closed_by
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `auction_lot` (
  `auction_id`       BIGINT          NOT NULL                  COMMENT '拍品 ID（后端雪花 ID，非自增）',
  `item_id`          BIGINT          NOT NULL                  COMMENT '关联商品 ID（item.id）',
  `seller_id`        BIGINT          NOT NULL                  COMMENT '卖家 ID（user.id）',
  `live_room_id`     BIGINT UNSIGNED NOT NULL DEFAULT 0        COMMENT '所属直播间 ID（0=未归属）',
  `live_session_id`  BIGINT UNSIGNED DEFAULT NULL              COMMENT '所属直播场次 ID（NULL=未关联场次）',
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
  KEY `idx_live_room_status` (`live_room_id`, `status`),
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_status_end_time` (`status`, `end_time`),
  KEY `idx_winner_id` (`winner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='拍品（拍卖实例）表';

-- ---------------------------------------------------------------------
-- 4. bid_record 出价记录表
--    uk_request_id 提供出价幂等保障
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `bid_record` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '出价记录 ID',
  `request_id`    VARCHAR(64)     NOT NULL                COMMENT '客户端幂等键（UUID）',
  `auction_id`    BIGINT          NOT NULL                COMMENT '拍品 ID（auction_lot.auction_id）',
  `live_session_id` BIGINT UNSIGNED DEFAULT NULL          COMMENT '所属直播场次 ID（NULL=非场次内出价）',
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
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_bidder_id` (`bidder_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='出价记录表';

-- ---------------------------------------------------------------------
-- 5. order_deal 成交订单表
--    uk_auction_id 保证一拍一单，防止重复成交
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `order_deal` (
  `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '订单 ID（成交订单由后端雪花 ID 写入，保留自增兜底）',
  `auction_id`     BIGINT          NOT NULL                COMMENT '拍品 ID（auction_lot.auction_id）',
  `live_session_id` BIGINT UNSIGNED DEFAULT NULL           COMMENT '所属直播场次 ID（NULL=非场次内成交）',
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
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_status_pay_deadline` (`status`, `pay_deadline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='成交订单表';

-- ---------------------------------------------------------------------
-- 6. deposit_ledger 保证金账本
--    uk_auction_user 保证同用户在同拍场仅一条有效记录
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 7. audit_log 审计日志
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 8. blacklist 黑名单
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 9. risk_event 风险事件
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 10. config_item 平台级配置
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `config_item` (
  `config_key`   VARCHAR(64)     NOT NULL                COMMENT '配置键（如 default.deposit_ratio）',
  `config_value` JSON            NOT NULL                COMMENT '配置值（JSON）',
  `description`  VARCHAR(256)    DEFAULT NULL            COMMENT '配置项说明',
  `updated_by`   BIGINT UNSIGNED DEFAULT NULL            COMMENT '最近修改者 ID（user.id）',
  `updated_at`   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`config_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='平台级配置项';

-- ---------------------------------------------------------------------
-- 11. live_session 直播场次表
--     一次 "开播-闭播" 周期；与 auction_lot/bid_record/order_deal 通过 live_session_id 关联
-- ---------------------------------------------------------------------
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

-- =====================================================================
-- 演示数据（Demo Seed），仅供本地启动验证使用
-- =====================================================================

-- 演示账号：管理员 / 商家 / 买家
INSERT INTO `user` (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`) VALUES
  (1, 'admin001',    '13800000001', '系统管理员', '1349e037dcc317dcbf97759f0df4b566f748b399227a1e4f5686fbe3b231ffe8', 'admin',    'ACTIVE'),
  (2, 'merchant001', '13800000002', '演示商家',   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', 'merchant', 'ACTIVE'),
  (3, 'buyer001',    '13800000003', '买家小明',   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', 'buyer',    'ACTIVE'),
  (4, 'disabled001', '13800000004', '停用买家',   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', 'buyer',    'DISABLED');

-- 演示商品
INSERT INTO `item` (`id`, `seller_id`, `title`, `category`, `brand`, `condition_grade`, `images`, `description`, `status`) VALUES
  (1001, 2, '【演示】限量版机械键盘', '数码配件', 'DemoBrand', 'NEW',
   JSON_ARRAY('https://example.com/demo1-1.jpg','https://example.com/demo1-2.jpg'),
   '演示用商品，限量版机械键盘，全新未拆封。', 'LISTED');

-- 演示拍品（0 元起拍 + 反狙击 15s/30s + 保证金 100 元）
INSERT INTO `auction_lot`
  (`auction_id`, `item_id`, `seller_id`, `auction_type`, `start_price`, `reserve_price`,
   `increment_rule`, `anti_sniping_sec`, `anti_extend_sec`, `deposit_amount`,
   `status`, `rule_snapshot`, `start_time`, `end_time`)
VALUES
  (90001, 1001, 2, 'ENGLISH', 0, 0,
   JSON_OBJECT('type','STEP','steps', JSON_ARRAY(
     JSON_OBJECT('lt', 10000, 'inc', 100),
     JSON_OBJECT('lt', 100000, 'inc', 500),
     JSON_OBJECT('lt', 1000000, 'inc', 2000)
   )),
   15, 30, 10000,
   'READY',
   JSON_OBJECT(
     'auctionType','ENGLISH',
     'startPrice', 0,
     'reservePrice', 0,
     'incrementStrategy', JSON_OBJECT('type','STEP'),
     'antiSniping', JSON_OBJECT('triggerSec',15,'extendSec',30),
     'depositPolicy', JSON_OBJECT('amount',10000)
   ),
   '2026-05-22 20:00:00.000',
   '2026-05-22 20:30:00.000');

-- 演示平台配置
INSERT INTO `config_item` (`config_key`, `config_value`, `description`) VALUES
  ('default.deposit_ratio',   JSON_OBJECT('ratio', 0.1, 'min', 1000, 'max', 100000), '默认保证金比例（按起拍价 10%，最小 10 元最大 1000 元）'),
  ('default.anti_sniping',    JSON_OBJECT('triggerSec', 15, 'extendSec', 30),         '默认反狙击参数'),
  ('order.pay_timeout_sec',   JSON_OBJECT('value', 1800),                              '订单支付超时（秒）');
