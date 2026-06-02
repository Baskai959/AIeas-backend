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

 Date: 25/05/2026 20:57:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for auction_lot
-- ----------------------------
DROP TABLE IF EXISTS `auction_lot`;
CREATE TABLE `auction_lot` (
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（后端雪花 ID，非自增）',
  `item_id` bigint NOT NULL COMMENT '关联商品 ID（item.id）',
  `seller_id` bigint NOT NULL COMMENT '卖家 ID（user.id）',
  `live_room_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '所属直播间 ID（0 表示未归属）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '所属直播场次 ID（NULL=未关联场次）',
  `auction_type` varchar(16) NOT NULL DEFAULT 'ENGLISH' COMMENT '拍卖类型，MVP 仅支持 ENGLISH',
  `start_price` bigint NOT NULL COMMENT '起拍价（分），允许为 0（0 元起拍）',
  `reserve_price` bigint NOT NULL DEFAULT '0' COMMENT '保留价（分），0 表示无保留价',
  `increment_rule` json NOT NULL COMMENT '加价规则 JSON：fixed 固定加价；ladder 阶梯加价；maxBidSteps 单次最高加价步数',
  `anti_sniping_sec` int NOT NULL DEFAULT '15' COMMENT '反狙击触发窗口（秒）',
  `anti_extend_sec` int NOT NULL DEFAULT '30' COMMENT '反狙击延长时长（秒）',
  `deposit_amount` bigint NOT NULL COMMENT '保证金金额（分）',
  `status` varchar(32) NOT NULL COMMENT '状态：DRAFT/PENDING_AUDIT/AUDIT_REJECTED/READY/WARMING_UP/RUNNING/EXTENDED/HAMMER_PENDING/CLOSED_WON/CLOSED_FAILED/SETTLED',
  `rule_snapshot` json NOT NULL COMMENT '规则快照（不可变）JSON',
  `start_time` datetime(3) NOT NULL COMMENT '开拍时间',
  `end_time` datetime(3) NOT NULL COMMENT '计划结束时间（可被反狙击延长）',
  `winner_id` bigint unsigned DEFAULT NULL COMMENT '中拍用户 ID（user.id）',
  `deal_price` bigint DEFAULT NULL COMMENT '成交价（分）',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '实际落锤时间',
  `closed_by` varchar(32) DEFAULT NULL COMMENT '落锤来源：AUTO/ADMIN/RISK_TERMINATE',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`auction_id`),
  KEY `idx_item_id` (`item_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_end_time` (`status`,`end_time`),
  KEY `idx_winner_id` (`winner_id`),
  KEY `idx_live_room_status` (`live_room_id`,`status`),
  KEY `idx_live_session` (`live_session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='拍品（拍卖实例）表';

-- ----------------------------
-- Records of auction_lot
-- ----------------------------
BEGIN;
INSERT INTO `auction_lot` VALUES (90001, 90001, 2001, 90001, NULL, 'ENGLISH', 10000, 20000, '{\"type\": \"ladder\", \"maxBidSteps\": 5, \"steps\": [{\"max\": 50000, \"min\": 0, \"amount\": 1000}, {\"max\": 200000, \"min\": 50000, \"amount\": 5000}, {\"max\": 1000000, \"min\": 200000, \"amount\": 20000}, {\"min\": 1000000, \"amount\": 50000}]}', 15, 30, 8000, 'CLOSED_WON', '{\"antiExtendSec\": 30, \"incrementRule\": [{\"stepCent\": 1000, \"maxPriceCent\": 50000}, {\"stepCent\": 5000, \"maxPriceCent\": 200000}, {\"stepCent\": 20000, \"maxPriceCent\": 1000000}, {\"stepCent\": 50000}], \"antiSnipingSec\": 15, \"startPriceCent\": 10000, \"reservePriceCent\": 20000, \"depositAmountCent\": 8000}', '2025-11-01 20:00:30.000', '2025-11-01 20:10:30.000', 1001, 50000, '2025-11-01 20:10:30.000', 'AUTO', '2025-10-31 18:00:00.000', '2026-05-25 17:23:52.696');
INSERT INTO `auction_lot` VALUES (90002, 90002, 2001, 90001, NULL, 'ENGLISH', 30000, 80000, '{\"type\": \"ladder\", \"maxBidSteps\": 5, \"steps\": [{\"max\": 50000, \"min\": 0, \"amount\": 1000}, {\"max\": 200000, \"min\": 50000, \"amount\": 5000}, {\"max\": 1000000, \"min\": 200000, \"amount\": 20000}, {\"min\": 1000000, \"amount\": 50000}]}', 15, 30, 15000, 'CLOSED_WON', '{\"antiExtendSec\": 30, \"incrementRule\": [{\"stepCent\": 1000, \"maxPriceCent\": 50000}, {\"stepCent\": 5000, \"maxPriceCent\": 200000}, {\"stepCent\": 20000, \"maxPriceCent\": 1000000}, {\"stepCent\": 50000}], \"antiSnipingSec\": 15, \"startPriceCent\": 30000, \"reservePriceCent\": 80000, \"depositAmountCent\": 15000}', '2025-11-01 20:11:00.000', '2025-11-01 20:21:00.000', 1003, 145000, '2025-11-01 20:21:00.000', 'AUTO', '2025-10-31 18:05:00.000', '2026-05-25 17:23:52.696');
INSERT INTO `auction_lot` VALUES (90003, 90003, 2001, 90001, NULL, 'ENGLISH', 300000, 800000, '{\"type\": \"ladder\", \"maxBidSteps\": 5, \"steps\": [{\"max\": 50000, \"min\": 0, \"amount\": 1000}, {\"max\": 200000, \"min\": 50000, \"amount\": 5000}, {\"max\": 1000000, \"min\": 200000, \"amount\": 20000}, {\"min\": 1000000, \"amount\": 50000}]}', 15, 30, 50000, 'CLOSED_FAILED', '{\"antiExtendSec\": 30, \"incrementRule\": [{\"stepCent\": 1000, \"maxPriceCent\": 50000}, {\"stepCent\": 5000, \"maxPriceCent\": 200000}, {\"stepCent\": 20000, \"maxPriceCent\": 1000000}, {\"stepCent\": 50000}], \"antiSnipingSec\": 15, \"startPriceCent\": 300000, \"reservePriceCent\": 800000, \"depositAmountCent\": 50000}', '2025-11-01 20:21:30.000', '2025-11-01 20:31:30.000', NULL, NULL, '2025-11-01 20:31:30.000', 'AUTO', '2025-10-31 18:10:00.000', '2026-05-25 17:23:52.696');
INSERT INTO `auction_lot` VALUES (90004, 90004, 2001, 90001, NULL, 'ENGLISH', 80000, 200000, '{\"type\": \"ladder\", \"maxBidSteps\": 5, \"steps\": [{\"max\": 50000, \"min\": 0, \"amount\": 1000}, {\"max\": 200000, \"min\": 50000, \"amount\": 5000}, {\"max\": 1000000, \"min\": 200000, \"amount\": 20000}, {\"min\": 1000000, \"amount\": 50000}]}', 15, 30, 40000, 'CLOSED_WON', '{\"antiExtendSec\": 30, \"incrementRule\": [{\"stepCent\": 1000, \"maxPriceCent\": 50000}, {\"stepCent\": 5000, \"maxPriceCent\": 200000}, {\"stepCent\": 20000, \"maxPriceCent\": 1000000}, {\"stepCent\": 50000}], \"antiSnipingSec\": 15, \"startPriceCent\": 80000, \"reservePriceCent\": 200000, \"depositAmountCent\": 40000}', '2025-11-01 20:32:00.000', '2025-11-01 20:42:00.000', 1004, 320000, '2025-11-01 20:42:00.000', 'AUTO', '2025-10-31 18:15:00.000', '2026-05-25 17:23:52.696');
INSERT INTO `auction_lot` VALUES (90005, 90005, 2001, 90001, NULL, 'ENGLISH', 200000, 400000, '{\"type\": \"ladder\", \"maxBidSteps\": 5, \"steps\": [{\"max\": 50000, \"min\": 0, \"amount\": 1000}, {\"max\": 200000, \"min\": 50000, \"amount\": 5000}, {\"max\": 1000000, \"min\": 200000, \"amount\": 20000}, {\"min\": 1000000, \"amount\": 50000}]}', 15, 30, 60000, 'CLOSED_WON', '{\"antiExtendSec\": 30, \"incrementRule\": [{\"stepCent\": 1000, \"maxPriceCent\": 50000}, {\"stepCent\": 5000, \"maxPriceCent\": 200000}, {\"stepCent\": 20000, \"maxPriceCent\": 1000000}, {\"stepCent\": 50000}], \"antiSnipingSec\": 15, \"startPriceCent\": 200000, \"reservePriceCent\": 400000, \"depositAmountCent\": 60000}', '2025-11-01 20:42:30.000', '2025-11-01 20:52:30.000', 1005, 720000, '2025-11-01 20:52:30.000', 'AUTO', '2025-10-31 18:20:00.000', '2026-05-25 17:23:52.696');
COMMIT;

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
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='审计日志';

-- ----------------------------
-- Records of audit_log
-- ----------------------------
BEGIN;
INSERT INTO `audit_log` VALUES (1, 2, 'merchant', 'POST /api/v1/live-rooms', 'HTTP', '/api/v1/live-rooms', '{\"path\": \"/api/v1/live-rooms\", \"method\": \"POST\", \"status\": 200, \"request_id\": \"req_1779690574940651000\"}', '127.0.0.1:55852', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 14:29:34.943');
INSERT INTO `audit_log` VALUES (2, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 200, \"request_id\": \"req_1779690592167620000\"}', '127.0.0.1:55964', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 14:29:52.866');
INSERT INTO `audit_log` VALUES (3, 2, 'merchant', 'POST /api/v1/items/description/optimize', 'HTTP', '/api/v1/items/description/optimize', '{\"path\": \"/api/v1/items/description/optimize\", \"method\": \"POST\", \"status\": 500, \"request_id\": \"req_1779693217119837000\"}', '127.0.0.1:59373', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 15:13:38.579');
INSERT INTO `audit_log` VALUES (4, 2, 'merchant', 'POST /api/v1/items/description/optimize', 'HTTP', '/api/v1/items/description/optimize', '{\"path\": \"/api/v1/items/description/optimize\", \"method\": \"POST\", \"status\": 500, \"request_id\": \"req_1779693273193542000\"}', '127.0.0.1:59894', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 15:14:33.298');
INSERT INTO `audit_log` VALUES (5, 2, 'merchant', 'POST /api/v1/items/description/optimize', 'HTTP', '/api/v1/items/description/optimize', '{\"path\": \"/api/v1/items/description/optimize\", \"method\": \"POST\", \"status\": 500, \"request_id\": \"req_1779693439754541000\"}', '127.0.0.1:61431', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 15:17:20.564');
INSERT INTO `audit_log` VALUES (6, 2, 'merchant', 'POST /api/v1/items/description/optimize', 'HTTP', '/api/v1/items/description/optimize', '{\"path\": \"/api/v1/items/description/optimize\", \"method\": \"POST\", \"status\": 200, \"request_id\": \"req_1779693589351808000\"}', '127.0.0.1:62904', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 15:19:57.981');
INSERT INTO `audit_log` VALUES (7, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 500, \"request_id\": \"req_1779696348981306000\"}', '127.0.0.1:55023', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:05:49.015');
INSERT INTO `audit_log` VALUES (8, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 500, \"request_id\": \"req_1779696354626470000\"}', '127.0.0.1:55075', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:05:57.950');
INSERT INTO `audit_log` VALUES (9, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 500, \"request_id\": \"req_1779696382474294000\"}', '127.0.0.1:55329', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:06:25.797');
INSERT INTO `audit_log` VALUES (10, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 500, \"request_id\": \"req_1779696473753439000\"}', '127.0.0.1:56235', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:07:53.791');
INSERT INTO `audit_log` VALUES (11, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 500, \"request_id\": \"req_1779696502982808000\"}', '127.0.0.1:56497', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:08:26.307');
INSERT INTO `audit_log` VALUES (12, 2, 'merchant', 'PATCH /api/v1/items/1001', 'HTTP', '/api/v1/items/1001', '{\"path\": \"/api/v1/items/1001\", \"method\": \"PATCH\", \"status\": 200, \"request_id\": \"req_1779696600790625000\"}', '127.0.0.1:57352', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 16:10:01.240');
INSERT INTO `audit_log` VALUES (13, 2, 'merchant', 'POST /api/v1/auth/logout', 'HTTP', '/api/v1/auth/logout', '{\"path\": \"/api/v1/auth/logout\", \"method\": \"POST\", \"status\": 200, \"request_id\": \"req_1779700063243729000\"}', '127.0.0.1:55358', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36', '2026-05-25 17:07:43.246');
COMMIT;

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
  KEY `idx_live_session` (`live_session_id`)
) ENGINE=InnoDB AUTO_INCREMENT=54 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='出价记录表';

-- ----------------------------
-- Records of bid_record
-- ----------------------------
BEGIN;
INSERT INTO `bid_record` VALUES (1, 'seed-demo-90001-01', 90001, NULL, 1003, 11000, 1761998460000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:01:00.000');
INSERT INTO `bid_record` VALUES (2, 'seed-demo-90001-02', 90001, NULL, 1004, 13000, 1761998510000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:01:50.000');
INSERT INTO `bid_record` VALUES (3, 'seed-demo-90001-03', 90001, NULL, 1005, 17000, 1761998560000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:02:40.000');
INSERT INTO `bid_record` VALUES (4, 'seed-demo-90001-04', 90001, NULL, 1006, 22000, 1761998610000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:03:30.000');
INSERT INTO `bid_record` VALUES (5, 'seed-demo-90001-05', 90001, NULL, 1001, 27000, 1761998660000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:04:20.000');
INSERT INTO `bid_record` VALUES (6, 'seed-demo-90001-06', 90001, NULL, 1003, 32000, 1761998710000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:05:10.000');
INSERT INTO `bid_record` VALUES (7, 'seed-demo-90001-07', 90001, NULL, 1004, 37000, 1761998760000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:06:00.000');
INSERT INTO `bid_record` VALUES (8, 'seed-demo-90001-08', 90001, NULL, 1005, 42000, 1761998810000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:06:50.000');
INSERT INTO `bid_record` VALUES (9, 'seed-demo-90001-09', 90001, NULL, 1006, 47000, 1761998860000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:07:40.000');
INSERT INTO `bid_record` VALUES (10, 'seed-demo-90001-10', 90001, NULL, 1001, 50000, 1761998910000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:08:30.000');
INSERT INTO `bid_record` VALUES (11, 'seed-demo-90002-01', 90002, NULL, 1001, 32000, 1761999090000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:11:30.000');
INSERT INTO `bid_record` VALUES (12, 'seed-demo-90002-02', 90002, NULL, 1004, 36000, 1761999140000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:12:20.000');
INSERT INTO `bid_record` VALUES (13, 'seed-demo-90002-03', 90002, NULL, 1005, 41000, 1761999190000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:13:10.000');
INSERT INTO `bid_record` VALUES (14, 'seed-demo-90002-04', 90002, NULL, 1006, 46000, 1761999240000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:14:00.000');
INSERT INTO `bid_record` VALUES (15, 'seed-demo-90002-05', 90002, NULL, 1003, 50000, 1761999290000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:14:50.000');
INSERT INTO `bid_record` VALUES (16, 'seed-demo-90002-06', 90002, NULL, 1001, 55000, 1761999340000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:15:40.000');
INSERT INTO `bid_record` VALUES (17, 'seed-demo-90002-07', 90002, NULL, 1004, 65000, 1761999390000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:16:30.000');
INSERT INTO `bid_record` VALUES (18, 'seed-demo-90002-08', 90002, NULL, 1005, 80000, 1761999440000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:17:20.000');
INSERT INTO `bid_record` VALUES (19, 'seed-demo-90002-09', 90002, NULL, 1006, 100000, 1761999490000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:18:10.000');
INSERT INTO `bid_record` VALUES (20, 'seed-demo-90002-10', 90002, NULL, 1001, 120000, 1761999540000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:19:00.000');
INSERT INTO `bid_record` VALUES (21, 'seed-demo-90002-11', 90002, NULL, 1003, 145000, 1761999590000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:19:50.000');
INSERT INTO `bid_record` VALUES (22, 'seed-demo-90003-01', 90003, NULL, 1001, 320000, 1761999720000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:22:00.000');
INSERT INTO `bid_record` VALUES (23, 'seed-demo-90003-02', 90003, NULL, 1003, 340000, 1761999770000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:22:50.000');
INSERT INTO `bid_record` VALUES (24, 'seed-demo-90003-03', 90003, NULL, 1004, 360000, 1761999820000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:23:40.000');
INSERT INTO `bid_record` VALUES (25, 'seed-demo-90003-04', 90003, NULL, 1005, 400000, 1761999870000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:24:30.000');
INSERT INTO `bid_record` VALUES (26, 'seed-demo-90003-05', 90003, NULL, 1006, 440000, 1761999920000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:25:20.000');
INSERT INTO `bid_record` VALUES (27, 'seed-demo-90003-06', 90003, NULL, 1001, 480000, 1761999970000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:26:10.000');
INSERT INTO `bid_record` VALUES (28, 'seed-demo-90003-07', 90003, NULL, 1003, 520000, 1762000020000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:27:00.000');
INSERT INTO `bid_record` VALUES (29, 'seed-demo-90003-08', 90003, NULL, 1004, 580000, 1762000070000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:27:50.000');
INSERT INTO `bid_record` VALUES (30, 'seed-demo-90003-09', 90003, NULL, 1005, 640000, 1762000120000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:28:40.000');
INSERT INTO `bid_record` VALUES (31, 'seed-demo-90003-10', 90003, NULL, 1006, 700000, 1762000170000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:29:30.000');
INSERT INTO `bid_record` VALUES (32, 'seed-demo-90003-11', 90003, NULL, 1001, 750000, 1762000220000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:30:20.000');
INSERT INTO `bid_record` VALUES (33, 'seed-demo-90004-01', 90004, NULL, 1001, 85000, 1762000350000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:32:30.000');
INSERT INTO `bid_record` VALUES (34, 'seed-demo-90004-02', 90004, NULL, 1003, 95000, 1762000400000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:33:20.000');
INSERT INTO `bid_record` VALUES (35, 'seed-demo-90004-03', 90004, NULL, 1005, 110000, 1762000450000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:34:10.000');
INSERT INTO `bid_record` VALUES (36, 'seed-demo-90004-04', 90004, NULL, 1006, 130000, 1762000500000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:35:00.000');
INSERT INTO `bid_record` VALUES (37, 'seed-demo-90004-05', 90004, NULL, 1004, 150000, 1762000550000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:35:50.000');
INSERT INTO `bid_record` VALUES (38, 'seed-demo-90004-06', 90004, NULL, 1001, 170000, 1762000600000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:36:40.000');
INSERT INTO `bid_record` VALUES (39, 'seed-demo-90004-07', 90004, NULL, 1003, 195000, 1762000650000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:37:30.000');
INSERT INTO `bid_record` VALUES (40, 'seed-demo-90004-08', 90004, NULL, 1005, 215000, 1762000700000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:38:20.000');
INSERT INTO `bid_record` VALUES (41, 'seed-demo-90004-09', 90004, NULL, 1006, 240000, 1762000750000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:39:10.000');
INSERT INTO `bid_record` VALUES (42, 'seed-demo-90004-10', 90004, NULL, 1001, 280000, 1762000800000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:40:00.000');
INSERT INTO `bid_record` VALUES (43, 'seed-demo-90004-11', 90004, NULL, 1004, 320000, 1762000850000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:40:50.000');
INSERT INTO `bid_record` VALUES (44, 'seed-demo-90005-01', 90005, NULL, 1001, 220000, 1762000980000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:43:00.000');
INSERT INTO `bid_record` VALUES (45, 'seed-demo-90005-02', 90005, NULL, 1003, 250000, 1762001030000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:43:50.000');
INSERT INTO `bid_record` VALUES (46, 'seed-demo-90005-03', 90005, NULL, 1004, 290000, 1762001080000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:44:40.000');
INSERT INTO `bid_record` VALUES (47, 'seed-demo-90005-04', 90005, NULL, 1006, 340000, 1762001130000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:45:30.000');
INSERT INTO `bid_record` VALUES (48, 'seed-demo-90005-05', 90005, NULL, 1005, 400000, 1762001180000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:46:20.000');
INSERT INTO `bid_record` VALUES (49, 'seed-demo-90005-06', 90005, NULL, 1001, 450000, 1762001230000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:47:10.000');
INSERT INTO `bid_record` VALUES (50, 'seed-demo-90005-07', 90005, NULL, 1003, 500000, 1762001280000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:48:00.000');
INSERT INTO `bid_record` VALUES (51, 'seed-demo-90005-08', 90005, NULL, 1004, 580000, 1762001330000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:48:50.000');
INSERT INTO `bid_record` VALUES (52, 'seed-demo-90005-09', 90005, NULL, 1006, 650000, 1762001380000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:49:40.000');
INSERT INTO `bid_record` VALUES (53, 'seed-demo-90005-10', 90005, NULL, 1005, 720000, 1762001430000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:50:30.000');
COMMIT;

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
-- Records of blacklist
-- ----------------------------
BEGIN;
COMMIT;

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
-- Records of config_item
-- ----------------------------
BEGIN;
INSERT INTO `config_item` VALUES ('default.anti_sniping', '{\"extendSec\": 30, \"triggerSec\": 15}', '默认反狙击参数', NULL, '2026-05-25 14:04:00.486');
INSERT INTO `config_item` VALUES ('default.deposit_ratio', '{\"max\": 100000, \"min\": 1000, \"ratio\": 0.1}', '默认保证金比例（按起拍价 10%，最小 10 元最大 1000 元）', NULL, '2026-05-25 14:04:00.486');
INSERT INTO `config_item` VALUES ('order.pay_timeout_sec', '{\"value\": 1200}', '订单支付超时（秒）', NULL, '2026-05-25 14:04:00.486');
COMMIT;

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
) ENGINE=InnoDB AUTO_INCREMENT=90026 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='保证金账本';

-- ----------------------------
-- Records of deposit_ledger
-- ----------------------------
BEGIN;
INSERT INTO `deposit_ledger` VALUES (90001, 90001, 1001, 8000, 'CAPTURED', 90001, '中标，保证金抵扣订单', '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');
INSERT INTO `deposit_ledger` VALUES (90002, 90001, 1003, 8000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');
INSERT INTO `deposit_ledger` VALUES (90003, 90001, 1004, 8000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');
INSERT INTO `deposit_ledger` VALUES (90004, 90001, 1005, 8000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');
INSERT INTO `deposit_ledger` VALUES (90005, 90001, 1006, 8000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');
INSERT INTO `deposit_ledger` VALUES (90006, 90002, 1001, 15000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `deposit_ledger` VALUES (90007, 90002, 1003, 15000, 'CAPTURED', 90002, '中标，保证金抵扣订单', '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `deposit_ledger` VALUES (90008, 90002, 1004, 15000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `deposit_ledger` VALUES (90009, 90002, 1005, 15000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `deposit_ledger` VALUES (90010, 90002, 1006, 15000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `deposit_ledger` VALUES (90011, 90003, 1001, 50000, 'RELEASED', NULL, '流拍，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');
INSERT INTO `deposit_ledger` VALUES (90012, 90003, 1003, 50000, 'RELEASED', NULL, '流拍，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');
INSERT INTO `deposit_ledger` VALUES (90013, 90003, 1004, 50000, 'RELEASED', NULL, '流拍，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');
INSERT INTO `deposit_ledger` VALUES (90014, 90003, 1005, 50000, 'RELEASED', NULL, '流拍，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');
INSERT INTO `deposit_ledger` VALUES (90015, 90003, 1006, 50000, 'RELEASED', NULL, '流拍，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');
INSERT INTO `deposit_ledger` VALUES (90016, 90004, 1001, 40000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `deposit_ledger` VALUES (90017, 90004, 1003, 40000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `deposit_ledger` VALUES (90018, 90004, 1004, 40000, 'CAPTURED', 90003, '中标，保证金抵扣订单', '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `deposit_ledger` VALUES (90019, 90004, 1005, 40000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `deposit_ledger` VALUES (90020, 90004, 1006, 40000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `deposit_ledger` VALUES (90021, 90005, 1001, 60000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');
INSERT INTO `deposit_ledger` VALUES (90022, 90005, 1003, 60000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');
INSERT INTO `deposit_ledger` VALUES (90023, 90005, 1004, 60000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');
INSERT INTO `deposit_ledger` VALUES (90024, 90005, 1005, 60000, 'CAPTURED', 90004, '中标，保证金抵扣订单', '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');
INSERT INTO `deposit_ledger` VALUES (90025, 90005, 1006, 60000, 'RELEASED', NULL, '未中标，保证金已释放', '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');
COMMIT;

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
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of goose_db_version
-- ----------------------------
BEGIN;
INSERT INTO `goose_db_version` VALUES (1, 0, 1, '2026-05-25 20:09:22');
COMMIT;

-- ----------------------------
-- Table structure for item
-- ----------------------------
DROP TABLE IF EXISTS `item`;
CREATE TABLE `item` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '商品 ID',
  `seller_id` bigint unsigned NOT NULL COMMENT '卖家 ID（关联 user.id）',
  `title` varchar(128) NOT NULL COMMENT '商品标题',
  `category` varchar(64) NOT NULL COMMENT '分类',
  `brand` varchar(64) DEFAULT NULL COMMENT '品牌',
  `condition_grade` varchar(16) NOT NULL DEFAULT 'NEW' COMMENT '成色：NEW/LIKE_NEW/GOOD/FAIR',
  `images` json NOT NULL COMMENT '图片 URL 数组（JSON）',
  `description` text COMMENT '商品描述',
  `status` varchar(16) NOT NULL DEFAULT 'PENDING_AUDIT' COMMENT '状态：DRAFT/PENDING_AUDIT/READY/REJECTED/LISTED/OFFLINE',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_seller_status` (`seller_id`,`status`),
  KEY `idx_category` (`category`)
) ENGINE=InnoDB AUTO_INCREMENT=90006 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品表';

-- ----------------------------
-- Records of item
-- ----------------------------
BEGIN;
INSERT INTO `item` VALUES (1001, 2001, '【演示】限量版机械键盘', '数码配件', 'DemoBrand', 'NEW', '[\"/api/v1/images/10d5e3c0a25ec49909ee5f8c20101974.jpg\"]', '演示用商品，限量版机械键盘，全新未拆封。', 'LISTED', '2026-05-25 14:03:52.637', '2026-05-25 17:08:34.436');
INSERT INTO `item` VALUES (90001, 2001, '清代青花瓷小碗', '瓷器', NULL, 'GOOD', '[\"https://cdn.example.com/item/90001-1.jpg\"]', '清中期民窑青花，口径 12cm', 'LISTED', '2025-10-30 10:00:00.000', '2025-11-01 19:55:00.000');
INSERT INTO `item` VALUES (90002, 2001, '老坑端砚一方', '文房', '老坑', 'GOOD', '[\"https://cdn.example.com/item/90002-1.jpg\"]', '端砚长方，纹理细腻', 'LISTED', '2025-10-30 10:05:00.000', '2025-11-01 19:55:00.000');
INSERT INTO `item` VALUES (90003, 2001, '民国和田玉牌', '玉石', NULL, 'LIKE_NEW', '[\"https://cdn.example.com/item/90003-1.jpg\"]', '和田白玉牌，正反双面工', 'LISTED', '2025-10-30 10:10:00.000', '2025-11-01 19:55:00.000');
INSERT INTO `item` VALUES (90004, 2001, '宋代影青刻花碗', '瓷器', NULL, 'FAIR', '[\"https://cdn.example.com/item/90004-1.jpg\"]', '影青刻花，包浆自然', 'LISTED', '2025-10-30 10:15:00.000', '2025-11-01 19:55:00.000');
INSERT INTO `item` VALUES (90005, 2001, '清乾隆掐丝珐琅香炉', '杂项', NULL, 'GOOD', '[\"https://cdn.example.com/item/90005-1.jpg\"]', '掐丝珐琅三足香炉，整器完整', 'LISTED', '2025-10-30 10:20:00.000', '2025-11-01 19:55:00.000');
COMMIT;

-- ----------------------------
-- Table structure for live_room
-- ----------------------------
DROP TABLE IF EXISTS `live_room`;
CREATE TABLE `live_room` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '直播间 ID',
  `merchant_id` bigint unsigned NOT NULL COMMENT '商家 ID（关联 user.id）',
  `title` varchar(128) NOT NULL COMMENT '直播间标题',
  `description` varchar(1024) DEFAULT NULL COMMENT '直播间描述',
  `cover_url` varchar(512) DEFAULT NULL COMMENT '封面 URL',
  `status` varchar(16) NOT NULL DEFAULT 'OFFLINE' COMMENT '状态：OFFLINE/LIVE/CLOSED',
  `active_auction_id` bigint DEFAULT NULL COMMENT '当前在拍 lot ID（同时只能有一个）',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_merchant` (`merchant_id`),
  KEY `idx_merchant_status` (`merchant_id`,`status`),
  KEY `idx_active_auction` (`active_auction_id`)
) ENGINE=InnoDB AUTO_INCREMENT=90002 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播间（拍卖房间）';

-- ----------------------------
-- Records of live_room
-- ----------------------------
BEGIN;
INSERT INTO `live_room` VALUES (1, 2, '演示商家 的直播间', '', '', 'OFFLINE', NULL, '2026-05-25 14:29:34.942', '2026-05-25 14:29:34.942');
INSERT INTO `live_room` VALUES (90001, 2001, '【演示】古玩珠宝拍卖夜场', '5 件珍品轮番上拍，欢迎围观', 'https://cdn.example.com/live/90001.jpg', 'CLOSED', NULL, '2025-11-01 19:30:00.000', '2025-11-01 20:53:00.000');
COMMIT;

-- ----------------------------
-- Table structure for live_session
-- ----------------------------
DROP TABLE IF EXISTS `live_session`;
CREATE TABLE `live_session` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '直播场次 ID',
  `live_room_id` bigint unsigned NOT NULL COMMENT '所属直播间 ID（live_room.id）',
  `merchant_id` varchar(64) NOT NULL COMMENT '商家 ID（冗余便于查询）',
  `title` varchar(255) DEFAULT NULL COMMENT '开播时直播间标题快照',
  `status` varchar(16) NOT NULL COMMENT '状态：LIVE/ENDED',
  `opened_at` datetime(3) NOT NULL COMMENT '开播时间',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '闭播时间',
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
  KEY `idx_room_status` (`live_room_id`,`status`),
  KEY `idx_merchant_opened` (`merchant_id`,`opened_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播场次（一次开播-闭播）';

-- ----------------------------
-- Records of live_session
-- ----------------------------
BEGIN;
COMMIT;

-- ----------------------------
-- Table structure for order_deal
-- ----------------------------
DROP TABLE IF EXISTS `order_deal`;
CREATE TABLE `order_deal` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '订单 ID（成交订单由后端雪花 ID 写入，保留自增兜底）',
  `auction_id` bigint NOT NULL COMMENT '拍品 ID（auction_lot.auction_id）',
  `live_session_id` bigint unsigned DEFAULT NULL COMMENT '所属直播场次 ID（NULL=非场次内成交）',
  `winner_id` bigint unsigned NOT NULL COMMENT '中拍人 ID（user.id）',
  `seller_id` bigint unsigned NOT NULL COMMENT '卖家 ID（user.id）',
  `deal_price` bigint NOT NULL COMMENT '成交价（分）',
  `deposit_amount` bigint NOT NULL DEFAULT '0' COMMENT '已冻结保证金金额（分）',
  `status` varchar(16) NOT NULL DEFAULT 'CREATED' COMMENT '订单状态：CREATED/PAID/TIMEOUT/CANCELLED',
  `pay_status` varchar(16) NOT NULL DEFAULT 'UNPAID' COMMENT '支付状态：UNPAID/PAID/REFUNDED',
  `pay_deadline` datetime(3) DEFAULT NULL COMMENT '支付截止时间',
  `paid_at` datetime(3) DEFAULT NULL COMMENT '支付完成时间',
  `closed_at` datetime(3) DEFAULT NULL COMMENT '订单关闭时间',
  `version` bigint NOT NULL DEFAULT '0' COMMENT '行级乐观锁版本号，支付/超时关单 CAS 路径递增',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_auction_id` (`auction_id`),
  KEY `idx_winner_id` (`winner_id`),
  KEY `idx_seller_id` (`seller_id`),
  KEY `idx_status_pay_deadline` (`status`,`pay_deadline`),
  KEY `idx_live_session` (`live_session_id`)
) ENGINE=InnoDB AUTO_INCREMENT=90005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='成交订单表';

-- ----------------------------
-- Records of order_deal
-- ----------------------------
BEGIN;
INSERT INTO `order_deal` VALUES (90001, 90001, NULL, 1001, 2001, 50000, 8000, 'PAID', 'PAID', '2025-11-02 20:10:30.000', '2025-11-01 20:15:30.000', '2025-11-01 20:15:30.000', 0, '2025-11-01 20:10:30.000', '2025-11-01 20:15:30.000');
INSERT INTO `order_deal` VALUES (90002, 90002, NULL, 1003, 2001, 145000, 15000, 'CREATED', 'UNPAID', '2025-11-02 20:21:00.000', NULL, NULL, 0, '2025-11-01 20:21:00.000', '2025-11-01 20:21:00.000');
INSERT INTO `order_deal` VALUES (90003, 90004, NULL, 1004, 2001, 320000, 40000, 'CREATED', 'UNPAID', '2025-11-02 20:42:00.000', NULL, NULL, 0, '2025-11-01 20:42:00.000', '2025-11-01 20:42:00.000');
INSERT INTO `order_deal` VALUES (90004, 90005, NULL, 1005, 2001, 720000, 60000, 'CREATED', 'UNPAID', '2025-11-02 20:52:30.000', NULL, NULL, 0, '2025-11-01 20:52:30.000', '2025-11-01 20:52:30.000');
COMMIT;

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
-- Records of risk_event
-- ----------------------------
BEGIN;
COMMIT;

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
  `last_login_at` datetime(3) DEFAULT NULL COMMENT '最近登录时间',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  UNIQUE KEY `uk_phone` (`phone`),
  KEY `idx_role_status` (`role`,`status`)
) ENGINE=InnoDB AUTO_INCREMENT=9002 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户表';

-- ----------------------------
-- Records of user
-- ----------------------------
BEGIN;
INSERT INTO `user` VALUES (1001, 'buyer001', '13800000003', '竞拍用户001', 'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL, 'buyer', 'ACTIVE', NULL, '2026-05-25 14:03:50.022', '2026-05-25 16:43:47.005');
INSERT INTO `user` VALUES (1002, 'disabled001', '13800000004', '停用用户001', 'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL, 'buyer', 'DISABLED', NULL, '2026-05-25 14:03:50.022', '2026-05-25 16:43:47.008');
INSERT INTO `user` VALUES (1003, 'buyer003', '13800000103', '竞拍用户003', '0000000000000000000000000000000000000000000000000000000000000000', NULL, 'buyer', 'ACTIVE', NULL, '2026-05-25 16:43:56.328', '2026-05-25 16:43:56.328');
INSERT INTO `user` VALUES (1004, 'buyer004', '13800000104', '竞拍用户004', '0000000000000000000000000000000000000000000000000000000000000000', NULL, 'buyer', 'ACTIVE', NULL, '2026-05-25 16:43:56.328', '2026-05-25 16:43:56.328');
INSERT INTO `user` VALUES (1005, 'buyer005', '13800000105', '竞拍用户005', '0000000000000000000000000000000000000000000000000000000000000000', NULL, 'buyer', 'ACTIVE', NULL, '2026-05-25 16:43:56.328', '2026-05-25 16:43:56.328');
INSERT INTO `user` VALUES (1006, 'buyer006', '13800000106', '竞拍用户006', '0000000000000000000000000000000000000000000000000000000000000000', NULL, 'buyer', 'ACTIVE', NULL, '2026-05-25 16:43:56.328', '2026-05-25 16:43:56.328');
INSERT INTO `user` VALUES (2001, 'merchant001', '13800000002', '商家001', 'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL, 'merchant', 'ACTIVE', NULL, '2026-05-25 14:03:50.022', '2026-05-25 16:43:47.006');
INSERT INTO `user` VALUES (9001, 'admin001', '13800000001', '管理员001', '1349e037dcc317dcbf97759f0df4b566f748b399227a1e4f5686fbe3b231ffe8', NULL, 'admin', 'ACTIVE', NULL, '2026-05-25 14:03:50.022', '2026-05-25 16:43:47.007');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
