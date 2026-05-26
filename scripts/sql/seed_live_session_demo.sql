-- =============================================================================
-- seed_live_session_demo.sql
-- 模拟一场完整的「直播拍卖场次（live_session）」真实数据，覆盖：
--   开播 → 上架拍品 → 用户报名/缴纳保证金 → 出价多轮 → 落槌 →
--   成交订单生成 → 付款 → 关播 → live_session 计数收口。
--
-- 覆盖表：user / live_room / live_session / item / auction_lot /
--         deposit_ledger / bid_record / order_deal / audit_log
--
-- ID 段约定（远离已用区间 1-9001、90001-90025，避免主键/UNIQUE 冲突）：
--   user.id               : 90000001（商家）/ 90000011~90000014（买家）
--   live_room.id          : 90000001
--   live_session.id       : 90000001
--   item.id               : 90000001 ~ 90000003
--   auction_lot.auction_id: 90000001 ~ 90000003
--   deposit_ledger.id     : 90000101 ~ 90000111（11 条）
--   bid_record.id         : 90000201 ~ 90000208（8 条）
--   order_deal.id         : 90000301 ~ 90000302（2 条）
--   audit_log.id          : 90000401 ~ 90000402（2 条）
--
-- 拍卖场景：
--   * Lot 1（明代铜鎏金弥勒佛坐像）：成交（SETTLED），5 轮出价，deal=18000分
--   * Lot 2（清代翡翠手镯）：流拍（CLOSED_FAILED），1 条出价但未达保留价
--   * Lot 3（民国粉彩花鸟瓶）：成交（SETTLED），2 轮出价，deal=55000分
--   bid_count=8, lots_total=3, lots_sold=2, lots_unsold=1, gmv_cent=73000
--
-- 时间轴（2026-05-26，CST +08:00）：
--   14:00:00  开播 → live_session.opened_at
--   14:05:00  Lot 1 开拍
--   14:05:30~14:11:30  Lot 1 出价（5 条）
--   14:15:00  Lot 1 落槌 → CLOSED_WON → SETTLED
--   14:16:00  Lot 2 开拍
--   14:18:00  Lot 2 仅 1 条出价
--   14:26:00  Lot 2 落槌 → CLOSED_FAILED
--   14:27:00  Lot 3 开拍
--   14:28:00~14:30:00  Lot 3 出价（2 条）
--   14:37:00  Lot 3 落槌 → CLOSED_WON → SETTLED
--   14:40:00  关播 → live_session.closed_at
-- =============================================================================

START TRANSACTION;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) 用户：1 商家 + 4 买家
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `user` (`id`, `account`, `phone`, `nickname`, `password_hash`, `avatar_url`, `role`, `status`, `last_login_at`, `created_at`, `updated_at`)
VALUES
  (90000001, 'merchant_qingyaxuan', '13900000001', '晴雅轩拍卖行',
   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL,
   'merchant', 'ACTIVE', '2026-05-26 13:55:00.000', '2026-05-20 10:00:00.000', '2026-05-26 13:55:00.000'),
  (90000011, 'buyer_zhang', '13900000011', '张先生',
   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL,
   'buyer', 'ACTIVE', '2026-05-26 13:58:00.000', '2026-05-20 10:01:00.000', '2026-05-26 13:58:00.000'),
  (90000012, 'buyer_li', '13900000012', '李女士',
   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL,
   'buyer', 'ACTIVE', '2026-05-26 13:59:00.000', '2026-05-20 10:02:00.000', '2026-05-26 13:59:00.000'),
  (90000013, 'buyer_wangdc', '13900000013', '王大成',
   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL,
   'buyer', 'ACTIVE', '2026-05-26 13:57:00.000', '2026-05-20 10:03:00.000', '2026-05-26 13:57:00.000'),
  (90000014, 'buyer_zhaoxt', '13900000014', '赵小棠',
   'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca', NULL,
   'buyer', 'ACTIVE', '2026-05-26 14:01:00.000', '2026-05-20 10:04:00.000', '2026-05-26 14:01:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) 直播间
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `live_room` (`id`, `merchant_id`, `title`, `description`, `cover_url`, `status`, `active_auction_id`, `created_at`, `updated_at`)
VALUES
  (90000001, 90000001, '晴雅轩·周一拍卖专场',
   '每周一下午两点开拍，精品瓷器、玉器、杂项轮番上阵',
   'https://cdn.example.com/live/90000001-cover.jpg',
   'CLOSED', NULL,
   '2026-05-20 12:00:00.000', '2026-05-26 14:40:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) 直播场次（初始状态 LIVE，脚本末尾 UPDATE 为 ENDED 并收口计数）
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `live_session` (`id`, `live_room_id`, `merchant_id`, `title`, `status`, `opened_at`, `closed_at`,
                            `lots_total`, `lots_sold`, `lots_unsold`, `bid_count`, `gmv_cent`,
                            `viewer_peak`, `viewer_total`, `created_at`, `updated_at`)
VALUES
  (90000001, 90000001, '90000001', '晴雅轩·周一拍卖专场',
   'LIVE', '2026-05-26 14:00:00.000', NULL,
   0, 0, 0, 0, 0, 0, 0,
   '2026-05-26 14:00:00.000', '2026-05-26 14:00:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) 商品（3 件）
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `item` (`id`, `seller_id`, `title`, `category`, `brand`, `condition_grade`, `images`, `description`, `status`, `created_at`, `updated_at`)
VALUES
  (90000001, 90000001, '明代铜鎏金弥勒佛坐像', '杂项', NULL, 'GOOD',
   '[\"https://cdn.example.com/item/90000001-1.jpg\",\"https://cdn.example.com/item/90000001-2.jpg\"]',
   '明中期铜鎏金弥勒佛坐像，高约 18cm，开脸端庄，鎏金大部分保留',
   'LISTED', '2026-05-22 10:00:00.000', '2026-05-26 14:05:00.000'),
  (90000002, 90000001, '清代翡翠手镯（满绿）', '玉石', NULL, 'LIKE_NEW',
   '[\"https://cdn.example.com/item/90000002-1.jpg\"]',
   '清晚期老坑翡翠圆条手镯，内径 56mm，满绿通透',
   'LISTED', '2026-05-22 10:05:00.000', '2026-05-26 14:16:00.000'),
  (90000003, 90000001, '民国粉彩花鸟瓶', '瓷器', NULL, 'GOOD',
   '[\"https://cdn.example.com/item/90000003-1.jpg\"]',
   '民国粉彩花鸟纹赏瓶，高 35cm，全品无伤，底款"江西珍品"',
   'LISTED', '2026-05-22 10:10:00.000', '2026-05-26 14:27:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) 拍品（3 件 auction_lot，均挂载到 live_session）
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `auction_lot` (`auction_id`, `item_id`, `seller_id`, `live_room_id`, `live_session_id`,
                           `auction_type`, `start_price`, `reserve_price`, `increment_rule`,
                           `anti_sniping_sec`, `anti_extend_sec`, `deposit_amount`,
                           `status`, `rule_snapshot`,
                           `start_time`, `end_time`,
                           `winner_id`, `deal_price`, `closed_at`, `closed_by`,
                           `created_at`, `updated_at`)
VALUES
  -- Lot 1: 成交 → SETTLED
  (90000001, 90000001, 90000001, 90000001, 90000001,
   'ENGLISH', 10000, 15000,
   '{"type":"ladder","steps":[{"min":0,"max":50000,"amount":1000},{"min":50000,"max":200000,"amount":5000},{"min":200000,"max":1000000,"amount":20000},{"min":1000000,"amount":50000}]}',
   15, 30, 5000,
   'SETTLED',
   '{"startPriceCent":10000,"reservePriceCent":15000,"depositAmountCent":5000,"antiSnipingSec":15,"antiExtendSec":30,"incrementRule":[{"stepCent":1000,"maxPriceCent":50000},{"stepCent":5000,"maxPriceCent":200000},{"stepCent":20000,"maxPriceCent":1000000},{"stepCent":50000}]}',
   '2026-05-26 14:05:00.000', '2026-05-26 14:15:00.000',
   90000012, 18000, '2026-05-26 14:15:00.000', 'AUTO',
   '2026-05-25 10:00:00.000', '2026-05-26 14:15:00.000'),

  -- Lot 2: 流拍 → CLOSED_FAILED
  (90000002, 90000002, 90000001, 90000001, 90000001,
   'ENGLISH', 50000, 80000,
   '{"type":"ladder","steps":[{"min":0,"max":50000,"amount":1000},{"min":50000,"max":200000,"amount":5000},{"min":200000,"max":1000000,"amount":20000},{"min":1000000,"amount":50000}]}',
   15, 30, 20000,
   'CLOSED_FAILED',
   '{"startPriceCent":50000,"reservePriceCent":80000,"depositAmountCent":20000,"antiSnipingSec":15,"antiExtendSec":30,"incrementRule":[{"stepCent":1000,"maxPriceCent":50000},{"stepCent":5000,"maxPriceCent":200000},{"stepCent":20000,"maxPriceCent":1000000},{"stepCent":50000}]}',
   '2026-05-26 14:16:00.000', '2026-05-26 14:26:00.000',
   NULL, NULL, '2026-05-26 14:26:00.000', 'AUTO',
   '2026-05-25 10:05:00.000', '2026-05-26 14:26:00.000'),

  -- Lot 3: 成交 → SETTLED
  (90000003, 90000003, 90000001, 90000001, 90000001,
   'ENGLISH', 30000, 40000,
   '{"type":"ladder","steps":[{"min":0,"max":50000,"amount":1000},{"min":50000,"max":200000,"amount":5000},{"min":200000,"max":1000000,"amount":20000},{"min":1000000,"amount":50000}]}',
   15, 30, 10000,
   'SETTLED',
   '{"startPriceCent":30000,"reservePriceCent":40000,"depositAmountCent":10000,"antiSnipingSec":15,"antiExtendSec":30,"incrementRule":[{"stepCent":1000,"maxPriceCent":50000},{"stepCent":5000,"maxPriceCent":200000},{"stepCent":20000,"maxPriceCent":1000000},{"stepCent":50000}]}',
   '2026-05-26 14:27:00.000', '2026-05-26 14:37:00.000',
   90000014, 55000, '2026-05-26 14:37:00.000', 'AUTO',
   '2026-05-25 10:10:00.000', '2026-05-26 14:37:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 6) 保证金（deposit_ledger）：11 条
--    Lot 1: 4 买家各 5000 分
--    Lot 2: 3 买家各 20000 分
--    Lot 3: 3 买家各 10000 分
--    中标者 CAPTURED，其余 RELEASED
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `deposit_ledger` (`id`, `auction_id`, `user_id`, `amount`, `status`, `related_order_id`, `remark`, `created_at`, `updated_at`)
VALUES
  -- Lot 1 deposits
  (90000101, 90000001, 90000011, 5000, 'RELEASED', NULL,   '未中标，保证金已释放', '2026-05-26 13:50:00.000', '2026-05-26 14:15:00.000'),
  (90000102, 90000001, 90000012, 5000, 'CAPTURED', 90000301, '中标，保证金抵扣订单', '2026-05-26 13:51:00.000', '2026-05-26 14:15:00.000'),
  (90000103, 90000001, 90000013, 5000, 'RELEASED', NULL,   '未中标，保证金已释放', '2026-05-26 13:52:00.000', '2026-05-26 14:15:00.000'),
  (90000104, 90000001, 90000014, 5000, 'RELEASED', NULL,   '未中标，保证金已释放', '2026-05-26 13:53:00.000', '2026-05-26 14:15:00.000'),
  -- Lot 2 deposits
  (90000105, 90000002, 90000011, 20000, 'RELEASED', NULL, '流拍，保证金已释放', '2026-05-26 14:10:00.000', '2026-05-26 14:26:00.000'),
  (90000106, 90000002, 90000013, 20000, 'RELEASED', NULL, '流拍，保证金已释放', '2026-05-26 14:10:30.000', '2026-05-26 14:26:00.000'),
  (90000107, 90000002, 90000014, 20000, 'RELEASED', NULL, '流拍，保证金已释放', '2026-05-26 14:11:00.000', '2026-05-26 14:26:00.000'),
  -- Lot 3 deposits
  (90000108, 90000003, 90000011, 10000, 'RELEASED', NULL,   '未中标，保证金已释放', '2026-05-26 14:20:00.000', '2026-05-26 14:37:00.000'),
  (90000109, 90000003, 90000013, 10000, 'RELEASED', NULL,   '未中标，保证金已释放', '2026-05-26 14:20:30.000', '2026-05-26 14:37:00.000'),
  (90000110, 90000003, 90000014, 10000, 'CAPTURED', 90000302, '中标，保证金抵扣订单', '2026-05-26 14:21:00.000', '2026-05-26 14:37:00.000'),
  -- 额外：buyer_li(90000012) 也报名了 Lot 3 但未出价
  (90000111, 90000003, 90000012, 10000, 'RELEASED', NULL, '未出价，保证金已释放', '2026-05-26 14:21:30.000', '2026-05-26 14:37:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 7) 出价记录（bid_record）：8 条
--    Lot 1: 5 条（张→李→王→张→李*赢）
--    Lot 2: 1 条（赵，未达保留价）
--    Lot 3: 2 条（王→赵*赢）
--    所有 bid_record.live_session_id = 90000001
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `bid_record` (`id`, `request_id`, `auction_id`, `live_session_id`, `bidder_id`,
                          `bid_price`, `bid_ts_ms`, `source`, `risk_result`, `reject_reason`, `created_at`)
VALUES
  -- Lot 1 bids (start_price=10000, increment=1000 in 0~50000 range)
  (90000201, 'sess-demo-90000001-b01', 90000001, 90000001, 90000011,
   11000, 1779775530000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:05:30.000'),
  (90000202, 'sess-demo-90000001-b02', 90000001, 90000001, 90000012,
   12000, 1779775620000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:07:00.000'),
  (90000203, 'sess-demo-90000001-b03', 90000001, 90000001, 90000013,
   13000, 1779775710000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:08:30.000'),
  (90000204, 'sess-demo-90000001-b04', 90000001, 90000001, 90000011,
   14000, 1779775800000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:10:00.000'),
  (90000205, 'sess-demo-90000001-b05', 90000001, 90000001, 90000012,
   18000, 1779775890000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:11:30.000'),

  -- Lot 2 bids (start_price=50000, reserve=80000; 仅 1 条出价)
  (90000206, 'sess-demo-90000002-b01', 90000002, 90000001, 90000014,
   55000, 1779776280000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:18:00.000'),

  -- Lot 3 bids (start_price=30000, reserve=40000)
  (90000207, 'sess-demo-90000003-b01', 90000003, 90000001, 90000013,
   35000, 1779776880000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:28:00.000'),
  (90000208, 'sess-demo-90000003-b02', 90000003, 90000001, 90000014,
   55000, 1779777000000, 'live_ws', 'ALLOW', NULL, '2026-05-26 14:30:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 8) 成交订单（order_deal）：2 条（Lot1 + Lot3），均关联 live_session
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `order_deal` (`id`, `auction_id`, `live_session_id`, `winner_id`, `seller_id`,
                          `deal_price`, `deposit_amount`, `status`, `pay_status`,
                          `pay_deadline`, `paid_at`, `closed_at`, `created_at`, `updated_at`)
VALUES
  -- Lot 1 → 买家李女士(90000012) 中标，deal_price=18000
  (90000301, 90000001, 90000001, 90000012, 90000001,
   18000, 5000, 'CREATED', 'UNPAID',
   '2026-05-27 14:15:00.000', NULL, NULL,
   '2026-05-26 14:15:00.000', '2026-05-26 14:15:00.000'),
  -- Lot 3 → 买家赵小棠(90000014) 中标，deal_price=55000
  (90000302, 90000003, 90000001, 90000014, 90000001,
   55000, 10000, 'CREATED', 'UNPAID',
   '2026-05-27 14:37:00.000', NULL, NULL,
   '2026-05-26 14:37:00.000', '2026-05-26 14:37:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 9) 模拟付款：将两笔订单 UPDATE 为已支付状态
-- ─────────────────────────────────────────────────────────────────────────────
UPDATE `order_deal`
SET `status`     = 'PAID',
    `pay_status` = 'PAID',
    `paid_at`    = '2026-05-26 14:45:00.000',
    `closed_at`  = '2026-05-26 14:45:00.000',
    `updated_at` = '2026-05-26 14:45:00.000'
WHERE `id` = 90000301;

UPDATE `order_deal`
SET `status`     = 'PAID',
    `pay_status` = 'PAID',
    `paid_at`    = '2026-05-26 14:50:00.000',
    `closed_at`  = '2026-05-26 14:50:00.000',
    `updated_at` = '2026-05-26 14:50:00.000'
WHERE `id` = 90000302;

-- ─────────────────────────────────────────────────────────────────────────────
-- 10) 审计日志（点缀）：开播 + 关播
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO `audit_log` (`id`, `operator_id`, `operator_role`, `action`, `target_type`, `target_id`, `payload`, `ip`, `ua`, `created_at`)
VALUES
  (90000401, 90000001, 'merchant', 'LIVE_SESSION_OPEN', 'LIVE_SESSION', '90000001',
   '{"live_room_id":90000001,"title":"晴雅轩·周一拍卖专场"}',
   '10.0.0.1', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '2026-05-26 14:00:00.000'),
  (90000402, 90000001, 'merchant', 'LIVE_SESSION_CLOSE', 'LIVE_SESSION', '90000001',
   '{"live_room_id":90000001,"lots_total":3,"lots_sold":2,"gmv_cent":73000}',
   '10.0.0.1', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '2026-05-26 14:40:00.000');

-- ─────────────────────────────────────────────────────────────────────────────
-- 11) 关播：live_session 状态 → ENDED，收口计数列
--     lots_total  = 3
--     lots_sold   = 2  (Lot1 + Lot3)
--     lots_unsold = 1  (Lot2)
--     bid_count   = 8  (5 + 1 + 2)
--     gmv_cent    = 73000 (18000 + 55000)
--     viewer_peak = 128（模拟峰值在线）
--     viewer_total= 356（模拟累计观看人次）
-- ─────────────────────────────────────────────────────────────────────────────
UPDATE `live_session`
SET `status`       = 'ENDED',
    `closed_at`    = '2026-05-26 14:40:00.000',
    `lots_total`   = 3,
    `lots_sold`    = 2,
    `lots_unsold`  = 1,
    `bid_count`    = 8,
    `gmv_cent`     = 73000,
    `viewer_peak`  = 128,
    `viewer_total` = 356,
    `updated_at`   = '2026-05-26 14:40:00.000'
WHERE `id` = 90000001;

COMMIT;

-- =============================================================================
-- 验证查询（人工核对用，不在事务内）
-- =============================================================================

-- Q1: 确认 live_session 最终状态及计数
SELECT id, status, opened_at, closed_at,
       lots_total, lots_sold, lots_unsold, bid_count, gmv_cent,
       viewer_peak, viewer_total
FROM `live_session`
WHERE id = 90000001;

-- Q2: 确认各 auction_lot 状态、winner、deal_price 及 live_session_id 关联
SELECT auction_id, status, winner_id, deal_price, live_session_id, closed_at
FROM `auction_lot`
WHERE live_session_id = 90000001
ORDER BY start_time;

-- Q3: 确认 bid_count 与实际 bid_record 行数一致
SELECT COUNT(*) AS actual_bid_count
FROM `bid_record`
WHERE live_session_id = 90000001;

-- Q4: 确认 GMV = SUM(order_deal.deal_price) for this session
SELECT SUM(deal_price) AS actual_gmv_cent
FROM `order_deal`
WHERE live_session_id = 90000001 AND status = 'PAID';

-- Q5: 订单状态检查（全部已付款）
SELECT id, auction_id, winner_id, deal_price, status, pay_status, paid_at
FROM `order_deal`
WHERE live_session_id = 90000001;
