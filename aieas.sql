/*
 Navicat MySQL Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 80200
 Source Host           : localhost:3306
 Source Schema         : aieas

 Target Server Type    : MySQL
 Target Server Version : 80200
 File Encoding         : 65001

 Date: 10/06/2026 18:33:36
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for auction_lot
-- ----------------------------
DROP TABLE IF EXISTS `auction_lot`;
CREATE TABLE `auction_lot` (
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（后端雪花 ID，非自增）',
  `seller_id` bigint NOT NULL COMMENT '卖家 ID（user.id）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '所属直播场次 ID（NULL=未关联场次）',
  `title` varchar(128) NOT NULL DEFAULT '' COMMENT '拍品标题（原商品标题）',
  `subtitle` varchar(256) NOT NULL DEFAULT '' COMMENT '拍品短简介/副标题，用户端直播清单展示',
  `description` text COMMENT '拍品描述（原商品描述）',
  `category` varchar(64) NOT NULL DEFAULT '' COMMENT '拍品类目（原商品类目）',
  `brand` varchar(64) DEFAULT NULL COMMENT '品牌',
  `condition_grade` varchar(16) NOT NULL DEFAULT 'GOOD' COMMENT '成色：NEW/LIKE_NEW/GOOD/FAIR',
  `image_urls` json DEFAULT NULL COMMENT '拍品图片 URL 列表',
  `cover_url` varchar(1024) DEFAULT NULL COMMENT '拍品封面 URL',
  `auction_type` varchar(16) NOT NULL DEFAULT 'ENGLISH' COMMENT '拍卖类型，MVP 仅支持 ENGLISH',
  `start_price` bigint NOT NULL COMMENT '起拍价（分），允许为 0（0 元起拍）',
  `reserve_price` bigint NOT NULL DEFAULT '0' COMMENT '保留价（分），0 表示无保留价',
  `cap_price` bigint NOT NULL DEFAULT '0' COMMENT '封顶价（分），0 表示无封顶价；达到该价格自动成交',
  `increment_rule` json NOT NULL COMMENT '加价规则 JSON：fixed 固定加价；ladder 阶梯加价；maxBidSteps 单次最高加价步数',
  `anti_sniping_sec` int NOT NULL DEFAULT '15' COMMENT '反狙击触发窗口（秒）',
  `anti_extend_sec` int NOT NULL DEFAULT '30' COMMENT '反狙击延长时长（秒）',
  `anti_extend_mode` varchar(16) NOT NULL DEFAULT 'ADD' COMMENT '反狙击延时模式：ADD=结束时间增加 anti_extend_sec；RESET=倒计时重置为 anti_extend_sec',
  `deposit_amount` bigint NOT NULL COMMENT '保证金金额（分）',
  `status` varchar(32) NOT NULL COMMENT '状态：DRAFT/PENDING_AUDIT/READY/WARMING_UP/RUNNING/EXTENDED/HAMMER_PENDING/CLOSED_WON/CLOSED_FAILED/SETTLED',
  `rule_snapshot` json NOT NULL COMMENT '规则快照（不可变）JSON',
  `audit_task_id` varchar(96) NOT NULL DEFAULT '' COMMENT '当前拍品内容审核任务 ID，用于过滤过期 AI 审核回调',
  `audit_reject_reason` text COMMENT '拍品内容审核未通过原因',
  `start_time` datetime(3) DEFAULT NULL COMMENT '开拍时间；NULL 表示未设置定时开拍',
  `end_time` datetime(3) DEFAULT NULL COMMENT '计划结束时间（可被反狙击延长）；NULL 表示启动时按时长计算',
  `duration_sec` int NOT NULL DEFAULT '0' COMMENT '拍卖时长（秒），0 表示未预设，可在上架/激活时指定',
  `winner_id` bigint unsigned DEFAULT NULL COMMENT '中拍用户 ID（user.id）',
  `deal_price` bigint DEFAULT NULL COMMENT '成交价（分）',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '实际落锤时间',
  `closed_by` varchar(32) DEFAULT NULL COMMENT '落锤来源：AUTO/ADMIN/RISK_TERMINATE',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `version` bigint NOT NULL DEFAULT '0' COMMENT '行级乐观锁版本号，仅由落槌 CAS 路径递增',
  PRIMARY KEY (`auction_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_end_time` (`status`,`end_time`),
  KEY `idx_winner_id` (`winner_id`),
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_lot_category` (`category`),
  KEY `idx_lot_title` (`title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='拍品（拍卖实例）表';

-- ----------------------------
-- Table structure for audit_log
-- ----------------------------
DROP TABLE IF EXISTS `audit_log`;
CREATE TABLE `audit_log` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '审计日志 ID',
  `operator_id` bigint unsigned NOT NULL COMMENT '操作者 ID（user.id）',
  `operator_role` varchar(16) NOT NULL COMMENT '操作者角色：user/merchant/admin',
  `action` varchar(64) NOT NULL COMMENT '动作枚举，如 AUCTION_AUDIT_PASS/HAMMER/BLACKLIST_ADD',
  `target_type` varchar(32) NOT NULL COMMENT '目标类型：AUCTION/ORDER/USER/ITEM',
  `target_id` varchar(64) NOT NULL COMMENT '目标 ID（字符串以兼容业务态主键）',
  `payload` json DEFAULT NULL COMMENT '请求/业务上下文 JSON',
  `ip` varchar(64) DEFAULT NULL COMMENT '操作 IP',
  `ua` varchar(256) DEFAULT NULL COMMENT 'User-Agent',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_operator_created` (`operator_id`,`created_at`),
  KEY `idx_target` (`target_type`,`target_id`)
) ENGINE=InnoDB AUTO_INCREMENT=90110049 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='审计日志';

-- ----------------------------
-- Table structure for bid_record
-- ----------------------------
DROP TABLE IF EXISTS `bid_record`;
CREATE TABLE `bid_record` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '出价记录 ID',
  `request_id` varchar(64) NOT NULL COMMENT '客户端幂等键（UUID）',
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（auction_lot.auction_id）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '所属直播场次 ID（NULL=非场次内出价）',
  `bidder_id` bigint unsigned NOT NULL COMMENT '出价人 ID（user.id）',
  `bid_price` bigint NOT NULL COMMENT '出价金额（分）',
  `bid_ts_ms` bigint NOT NULL COMMENT '服务端受理时间戳（毫秒）',
  `source` varchar(32) NOT NULL DEFAULT 'live_ws' COMMENT '出价来源：live_ws/http/admin_proxy',
  `risk_result` varchar(32) NOT NULL DEFAULT 'ALLOW' COMMENT '风控结果：ALLOW/REJECT/REVIEW',
  `reject_reason` varchar(64) DEFAULT NULL COMMENT '拒绝原因，如 BELOW_MIN_INC / BLACKLIST',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_request_id` (`request_id`),
  KEY `idx_auction_bid_ts` (`auction_id`,`bid_ts_ms`),
  KEY `idx_bidder_id` (`bidder_id`),
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_bid_record_auction_allow_price_time` (`auction_id`,`risk_result`,`reject_reason`,`bid_price` DESC,`bid_ts_ms`),
  KEY `idx_bid_record_auction_allow_time` (`auction_id`,`risk_result`,`reject_reason`,`bid_ts_ms`)
) ENGINE=InnoDB AUTO_INCREMENT=91484625 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='出价记录表';

-- ----------------------------
-- Table structure for blacklist
-- ----------------------------
DROP TABLE IF EXISTS `blacklist`;
CREATE TABLE `blacklist` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '黑名单 ID',
  `user_id` bigint unsigned NOT NULL COMMENT '被拉黑用户 ID（user.id）',
  `reason` varchar(256) NOT NULL COMMENT '拉黑原因',
  `created_by` bigint unsigned NOT NULL COMMENT '操作者 ID（user.id）',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `expires_at` datetime(3) DEFAULT NULL COMMENT '过期时间，NULL 表示永久',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_id` (`user_id`),
  KEY `idx_expires_at` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='黑名单';

-- ----------------------------
-- Table structure for config_item
-- ----------------------------
DROP TABLE IF EXISTS `config_item`;
CREATE TABLE `config_item` (
  `config_key` varchar(64) NOT NULL COMMENT '配置键（如 default.deposit_ratio）',
  `config_value` json NOT NULL COMMENT '配置值（JSON）',
  `description` varchar(256) DEFAULT NULL COMMENT '配置项说明',
  `updated_by` bigint unsigned DEFAULT NULL COMMENT '最近修改者 ID（user.id）',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`config_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='平台级配置项';

-- ----------------------------
-- Table structure for deposit_ledger
-- ----------------------------
DROP TABLE IF EXISTS `deposit_ledger`;
CREATE TABLE `deposit_ledger` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '账本 ID',
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（auction_lot.auction_id）',
  `user_id` bigint unsigned NOT NULL COMMENT '用户 ID（user.id）',
  `amount` bigint NOT NULL COMMENT '保证金金额（分）',
  `status` varchar(16) NOT NULL COMMENT '状态：PENDING/READY/CAPTURED/RELEASED/FAILED',
  `related_order_id` bigint unsigned DEFAULT NULL COMMENT '关联订单 ID（order_deal.id），CAPTURED 时绑定',
  `remark` varchar(256) DEFAULT NULL COMMENT '备注',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_auction_user` (`auction_id`,`user_id`),
  KEY `idx_user_status` (`user_id`,`status`)
) ENGINE=InnoDB AUTO_INCREMENT=90037213 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='保证金账本';

-- ----------------------------
-- Table structure for goose_db_version
-- ----------------------------
DROP TABLE IF EXISTS `goose_db_version`;
CREATE TABLE `goose_db_version` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `version_id` bigint NOT NULL,
  `is_applied` tinyint(1) NOT NULL,
  `tstamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Table structure for live_analysis_report
-- ----------------------------
DROP TABLE IF EXISTS `live_analysis_report`;
CREATE TABLE `live_analysis_report` (
  `task_id` varchar(64) NOT NULL COMMENT '报告生成任务 ID（lar_xxx）',
  `agent_request_id` varchar(128) DEFAULT NULL COMMENT 'Agent 异步任务 ID，用于回调兜底定位',
  `merchant_id` varchar(64) NOT NULL COMMENT '商家 ID（冗余便于按商家查询）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '直播场次 ID，与 live_session.id 对应',
  `status` varchar(16) NOT NULL COMMENT '任务状态：PENDING/RUNNING/SUCCEEDED/FAILED',
  `attempt_count` int NOT NULL DEFAULT '0' COMMENT '已请求 Agent 生成次数，最多 3 次',
  `prompt` text NOT NULL COMMENT '发送给 Agent 的提示词',
  `report` mediumtext COMMENT 'AI 生成的直播总结内容',
  `error_message` varchar(1024) DEFAULT NULL COMMENT '失败原因',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`task_id`),
  UNIQUE KEY `uk_live_session` (`live_session_id`),
  KEY `idx_merchant_created` (`merchant_id`,`created_at`),
  KEY `idx_status_updated` (`status`,`updated_at`),
  KEY `idx_agent_request` (`agent_request_id`),
  KEY `idx_session_status` (`live_session_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播 AI 总结报告';

-- ----------------------------
-- Table structure for live_session
-- ----------------------------
DROP TABLE IF EXISTS `live_session`;
CREATE TABLE `live_session` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '直播场次 ID',
  `merchant_id` varchar(64) NOT NULL COMMENT '商家 ID（冗余便于查询）',
  `live_merchant_id` varchar(64) GENERATED ALWAYS AS ((case when (`status` = _utf8mb4'LIVE') then `merchant_id` else NULL end)) STORED COMMENT '同商家同时仅一个 LIVE 的唯一键列',
  `title` varchar(255) DEFAULT NULL COMMENT '开播时直播间标题快照',
  `description` text COMMENT '直播场次描述',
  `cover_url` varchar(1024) DEFAULT NULL COMMENT '直播场次封面 URL',
  `status` varchar(16) NOT NULL COMMENT '状态：DRAFT/SCHEDULED/LIVE/ENDED/CANCELLED',
  `is_digital_human` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否数字人直播间：1=数字人/0=普通直播(recorded)',
  `active_auction_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '当前讲解/在拍 auction_id，0=无',
  `opened_at` datetime(3) DEFAULT NULL COMMENT '实际开播时间',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '闭播时间',
  `scheduled_start_time` datetime(3) DEFAULT NULL COMMENT '计划开播时间',
  `planned_duration_sec` int NOT NULL DEFAULT '0' COMMENT '计划直播时长（秒）',
  `lots_total` int NOT NULL DEFAULT '0' COMMENT '本场上架/挂载过的拍品数',
  `lots_sold` int NOT NULL DEFAULT '0' COMMENT '本场成交数',
  `lots_unsold` int NOT NULL DEFAULT '0' COMMENT '本场流拍数',
  `bid_count` int NOT NULL DEFAULT '0' COMMENT '本场出价次数',
  `gmv_cent` bigint NOT NULL DEFAULT '0' COMMENT '本场成交总金额（分）',
  `viewer_peak` int NOT NULL DEFAULT '0' COMMENT '峰值在线',
  `viewer_total` int NOT NULL DEFAULT '0' COMMENT '累计观看人次（去重以 user_id）',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_live_session_one_live_per_merchant` (`live_merchant_id`),
  KEY `idx_merchant_opened` (`merchant_id`,`opened_at`),
  KEY `idx_live_session_active_auction` (`active_auction_id`),
  KEY `idx_live_session_status_schedule` (`status`,`scheduled_start_time`)
) ENGINE=InnoDB AUTO_INCREMENT=90000029 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播场次（一次开播-闭播）';

-- ----------------------------
-- Table structure for order_deal
-- ----------------------------
DROP TABLE IF EXISTS `order_deal`;
CREATE TABLE `order_deal` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '订单 ID（成交订单由后端雪花 ID 写入，保留自增兜底）',
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（auction_lot.auction_id）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '所属直播场次 ID（NULL=非场次内成交）',
  `lot_snapshot` json DEFAULT NULL COMMENT '成交时拍品展示与拍卖规则快照',
  `winner_id` bigint unsigned NOT NULL COMMENT '中拍人 ID（user.id）',
  `seller_id` bigint unsigned NOT NULL COMMENT '卖家 ID（user.id）',
  `deal_price` bigint NOT NULL COMMENT '成交价（分）',
  `deposit_amount` bigint NOT NULL DEFAULT '0' COMMENT '已冻结保证金金额（分）',
  `status` varchar(16) NOT NULL DEFAULT 'CREATED' COMMENT '订单状态：CREATED/PAID/TIMEOUT/CANCELLED',
  `pay_status` varchar(16) NOT NULL DEFAULT 'UNPAID' COMMENT '支付状态：UNPAID/PAID/REFUNDED',
  `fulfillment_status` varchar(16) NOT NULL DEFAULT 'UNSHIPPED' COMMENT '履约状态：UNSHIPPED/SHIPPED/RECEIVED',
  `pay_deadline` datetime(3) DEFAULT NULL COMMENT '支付截止时间',
  `paid_at` datetime(3) DEFAULT NULL COMMENT '支付完成时间',
  `shipped_at` datetime(3) DEFAULT NULL COMMENT '发货时间',
  `received_at` datetime(3) DEFAULT NULL COMMENT '收货时间',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '订单关闭时间',
  `version` bigint NOT NULL DEFAULT '0' COMMENT '行级乐观锁版本号，支付/超时关单 CAS 路径递增',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_auction_id` (`auction_id`),
  KEY `idx_winner_id` (`winner_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_pay_deadline` (`status`,`pay_deadline`),
  KEY `idx_live_session` (`live_session_id`),
  KEY `idx_fulfillment_status` (`fulfillment_status`)
) ENGINE=InnoDB AUTO_INCREMENT=57837820969473 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='成交订单表';

-- ----------------------------
-- Table structure for risk_event
-- ----------------------------
DROP TABLE IF EXISTS `risk_event`;
CREATE TABLE `risk_event` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '风险事件 ID',
  `event_type` varchar(32) NOT NULL COMMENT '事件类型：BID_FREQ/SHILL_BIDDING/ABUSE_RETRY 等',
  `user_id` bigint unsigned DEFAULT NULL COMMENT '涉及用户 ID（user.id）',
  `auction_id` bigint DEFAULT NULL COMMENT '涉及拍品 ID（auction_lot.auction_id）',
  `severity` varchar(8) NOT NULL DEFAULT 'LOW' COMMENT '严重度：LOW/MID/HIGH',
  `payload` json DEFAULT NULL COMMENT '命中详情 JSON',
  `status` varchar(16) NOT NULL DEFAULT 'PENDING' COMMENT '处理状态：PENDING/REVIEWED/IGNORED',
  `reviewed_by` bigint unsigned DEFAULT NULL COMMENT '复核人 ID（user.id）',
  `reviewed_at` datetime(3) DEFAULT NULL COMMENT '复核时间',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_status_created` (`status`,`created_at`),
  KEY `idx_auction_user` (`auction_id`,`user_id`),
  KEY `idx_event_type` (`event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='风险事件表';

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '用户 ID',
  `account` varchar(64) NOT NULL COMMENT '登录账号',
  `phone` varchar(32) DEFAULT NULL COMMENT '手机号',
  `nickname` varchar(64) NOT NULL COMMENT '昵称',
  `password_hash` char(64) NOT NULL COMMENT '密码哈希',
  `avatar_url` varchar(512) DEFAULT NULL COMMENT '头像 URL',
  `role` varchar(16) NOT NULL DEFAULT 'buyer' COMMENT '角色：buyer/merchant/admin',
  `status` varchar(16) NOT NULL DEFAULT 'ACTIVE' COMMENT '账号状态：ACTIVE/DISABLED',
  `ai_permission` varchar(16) NOT NULL DEFAULT 'ASK' COMMENT '商家 AI 控制权限：ASK=执行前询问/ALLOW=自动允许/DENY=自动拒绝',
  `last_login_at` datetime(3) DEFAULT NULL COMMENT '最近登录时间',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  UNIQUE KEY `uk_phone` (`phone`),
  KEY `idx_role_status` (`role`,`status`)
) ENGINE=InnoDB AUTO_INCREMENT=92000101 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户表';

SET FOREIGN_KEY_CHECKS = 1;
