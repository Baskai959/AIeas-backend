-- =============================================================================
-- seed_live_auction_demo.sql
-- 模拟一场真实「拍卖直播」的全部数据库数据。
--
-- 表覆盖：user / live_room / item / auction_lot / bid_record / deposit_ledger
--         / order_deal
--
-- 数据规模：
--   * 1 个商家（user.id=2001，复用 seed-dev）
--   * 5 个买家：1001（复用 seed-dev）+ 1003 / 1004 / 1005 / 1006（本脚本插入）
--   * 1 个直播间（live_room.id=90001，CLOSED）
--   * 5 个商品（item.id=90001~90005）
--   * 5 个拍品（auction_lot.auction_id=90001~90005，串行拍卖）
--     - 90001 / 90002 / 90004 / 90005 ：CLOSED_WON
--     - 90003                          ：CLOSED_FAILED（最高价 < reserve_price）
--   * 53 条 bid_record（每拍 10 ~ 11 条）
--   * 25 条 deposit_ledger（每拍 5 个参与买家各一条）
--   * 4  条 order_deal（90001 已支付，其余 CREATED/UNPAID）
--
-- ID 段约定（避免与现网/dev 冲突）：
--   user.id            : 1001 / 1003 / 1004 / 1005 / 1006 / 2001
--   live_room.id       : 90001
--   item.id            : 90001 ~ 90005
--   auction_lot.auction_id : 90001 ~ 90005
--   order_deal.id      : 90001 ~ 90004
--   deposit_ledger.id  : 90001 ~ 90025
--   bid_record.id      : 数据库自增（不指定 id）
--   bid_record.request_id : "seed-demo-<auction_id>-<NN>"，UNIQUE 索引保证幂等
--
-- 时间轴（Asia/Shanghai，+08:00）：
--   直播开始：2025-11-01 20:00:00
--   每个拍品 warmup 30s + 拍卖 10min
--     90001 : 20:00:30 ~ 20:10:30
--     90002 : 20:11:00 ~ 20:21:00
--     90003 : 20:21:30 ~ 20:31:30
--     90004 : 20:32:00 ~ 20:42:00
--     90005 : 20:42:30 ~ 20:52:30
--
-- 阶梯加价规则（写入 auction_lot.increment_rule）：
--   {"type":"ladder","maxBidSteps":5,
--    "steps":[{"min":0,"max":50000,"amount":1000},
--             {"min":50000,"max":200000,"amount":5000},
--             {"min":200000,"max":1000000,"amount":20000},
--             {"min":1000000,"amount":50000}]}
--   语义：以「当前价」落在哪个阶梯决定 amount；
--   合法新价区间为 prev + step ≤ new ≤ prev + 5*step。
--
-- 字段说明 / 已省略字段：
--   * 全部 INSERT 严格按 migrations/00001_init_schema.sql 与
--     00002_live_room.sql、00003_live_room_merchant_unique.sql 中真实建表语句对齐。
--   * user 的 avatar_url / last_login_at 为可空字段，本脚本不写。
--   * item 的 brand / description 可空，部分商品仅给 brand。
--   * auction_lot.closed_by 给 'AUTO'。
--   * order_deal.closed_at 仅在 PAID 单据上写入（其它保持 NULL）。
--   * deposit_ledger.remark 给文字说明便于人工核对。
--
-- 可重放性：
--   * 头部关闭外键检查，尾部恢复
--   * 全部 INSERT 用 INSERT IGNORE（依赖各表的 PK / UNIQUE 索引去重）
--   * 末尾打印 SELECT 摘要
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';
SET FOREIGN_KEY_CHECKS = 0;

-- ---- 0. 前置检查：确认 seed-dev 用户存在 ----
-- 如果下面查询的 cnt < 2，说明 seed-dev 还没跑过，请先执行 `go run ./cmd/db seed-dev`
SELECT 'precheck_seed_dev_users' AS step,
       COUNT(*)                  AS cnt
  FROM `user`
 WHERE `id` IN (1001, 2001);

-- ---- 1. 用户：补齐演示用买家（1003 / 1004 / 1005 / 1006）----
-- 复用 seed-dev 的 1001（buyer001）和 2001（merchant001）。
-- password_hash 给 64 位占位 hex；演示数据不参与登录鉴权。
INSERT IGNORE INTO `user`
  (`id`, `account`,    `phone`,        `nickname`,      `password_hash`,                                                     `role`,    `status`)
VALUES
  (1003, 'buyer003',   '13800000103',  '竞拍用户003',    '0000000000000000000000000000000000000000000000000000000000000000', 'buyer',   'ACTIVE'),
  (1004, 'buyer004',   '13800000104',  '竞拍用户004',    '0000000000000000000000000000000000000000000000000000000000000000', 'buyer',   'ACTIVE'),
  (1005, 'buyer005',   '13800000105',  '竞拍用户005',    '0000000000000000000000000000000000000000000000000000000000000000', 'buyer',   'ACTIVE'),
  (1006, 'buyer006',   '13800000106',  '竞拍用户006',    '0000000000000000000000000000000000000000000000000000000000000000', 'buyer',   'ACTIVE');

-- ---- 2. 直播间（live_room）----
-- 一个直播间，归属商家 2001，本脚本演示「直播已结束」状态。
INSERT IGNORE INTO `live_room`
  (`id`,    `merchant_id`, `title`,                  `description`,                  `cover_url`,                                `status`,  `active_auction_id`, `created_at`,              `updated_at`)
VALUES
  (90001,   2001,          '【演示】古玩珠宝拍卖夜场', '5 件珍品轮番上拍，欢迎围观', 'https://cdn.example.com/live/90001.jpg', 'CLOSED',  NULL,                '2025-11-01 19:30:00.000', '2025-11-01 20:53:00.000');

-- ---- 3. 商品（item）----
-- 5 件商品，全部归属商家 2001。
INSERT IGNORE INTO `item`
  (`id`,   `seller_id`, `title`,                    `category`, `brand`,    `condition_grade`, `images`,                                                              `description`,                  `status`,  `created_at`,              `updated_at`)
VALUES
  (90001,  2001,        '清代青花瓷小碗',            '瓷器',     NULL,       'GOOD',            JSON_ARRAY('https://cdn.example.com/item/90001-1.jpg'),                '清中期民窑青花，口径 12cm',     'LISTED',  '2025-10-30 10:00:00.000', '2025-11-01 19:55:00.000'),
  (90002,  2001,        '老坑端砚一方',              '文房',     '老坑',     'GOOD',            JSON_ARRAY('https://cdn.example.com/item/90002-1.jpg'),                '端砚长方，纹理细腻',            'LISTED',  '2025-10-30 10:05:00.000', '2025-11-01 19:55:00.000'),
  (90003,  2001,        '民国和田玉牌',              '玉石',     NULL,       'LIKE_NEW',        JSON_ARRAY('https://cdn.example.com/item/90003-1.jpg'),                '和田白玉牌，正反双面工',        'LISTED',  '2025-10-30 10:10:00.000', '2025-11-01 19:55:00.000'),
  (90004,  2001,        '宋代影青刻花碗',            '瓷器',     NULL,       'FAIR',            JSON_ARRAY('https://cdn.example.com/item/90004-1.jpg'),                '影青刻花，包浆自然',            'LISTED',  '2025-10-30 10:15:00.000', '2025-11-01 19:55:00.000'),
  (90005,  2001,        '清乾隆掐丝珐琅香炉',        '杂项',     NULL,       'GOOD',            JSON_ARRAY('https://cdn.example.com/item/90005-1.jpg'),                '掐丝珐琅三足香炉，整器完整',    'LISTED',  '2025-10-30 10:20:00.000', '2025-11-01 19:55:00.000');

-- ---- 4. 拍品（auction_lot）----
-- 5 个拍品按时间串行拍卖。
-- 阶梯加价规则与 rule_snapshot 写入 JSON 字段。
-- 90003 为 CLOSED_FAILED：reserve_price=800000，最高出价 750000，未达保留价。
-- 其余四个均为 CLOSED_WON，winner_id / deal_price 与最后一条 bid 的出价人 / 出价金额一致。
INSERT IGNORE INTO `auction_lot`
  (`auction_id`, `item_id`, `seller_id`, `live_room_id`, `auction_type`, `start_price`, `reserve_price`, `increment_rule`,
   `anti_sniping_sec`, `anti_extend_sec`, `deposit_amount`, `status`,         `rule_snapshot`,
   `start_time`,              `end_time`,                `winner_id`, `deal_price`, `closed_at`,               `closed_by`,
   `created_at`,              `updated_at`)
VALUES
  -- Lot 1: 起拍 100 元，CLOSED_WON，winner=1001，成交 500 元
  (90001, 90001, 2001, 90001, 'ENGLISH',  10000,   20000,
   JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000))),
   15, 30, 8000, 'CLOSED_WON',
   JSON_OBJECT('startPriceCent',10000,'reservePriceCent',20000,'depositAmountCent',8000,
               'antiSnipingSec',15,'antiExtendSec',30,
               'incrementRule',JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000)))),
   '2025-11-01 20:00:30.000', '2025-11-01 20:10:30.000', 1001, 50000, '2025-11-01 20:10:30.000', 'AUTO',
   '2025-10-31 18:00:00.000', '2025-11-01 20:10:30.000'),

  -- Lot 2: 起拍 300 元，CLOSED_WON，winner=1003，成交 1450 元
  (90002, 90002, 2001, 90001, 'ENGLISH',  30000,   80000,
   JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000))),
   15, 30, 15000, 'CLOSED_WON',
   JSON_OBJECT('startPriceCent',30000,'reservePriceCent',80000,'depositAmountCent',15000,
               'antiSnipingSec',15,'antiExtendSec',30,
               'incrementRule',JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000)))),
   '2025-11-01 20:11:00.000', '2025-11-01 20:21:00.000', 1003, 145000, '2025-11-01 20:21:00.000', 'AUTO',
   '2025-10-31 18:05:00.000', '2025-11-01 20:21:00.000'),

  -- Lot 3: 起拍 3000 元，CLOSED_FAILED（最高 7500 元 < reserve 8000 元）
  (90003, 90003, 2001, 90001, 'ENGLISH',  300000,  800000,
   JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000))),
   15, 30, 50000, 'CLOSED_FAILED',
   JSON_OBJECT('startPriceCent',300000,'reservePriceCent',800000,'depositAmountCent',50000,
               'antiSnipingSec',15,'antiExtendSec',30,
               'incrementRule',JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000)))),
   '2025-11-01 20:21:30.000', '2025-11-01 20:31:30.000', NULL, NULL, '2025-11-01 20:31:30.000', 'AUTO',
   '2025-10-31 18:10:00.000', '2025-11-01 20:31:30.000'),

  -- Lot 4: 起拍 800 元，CLOSED_WON，winner=1004，成交 3200 元
  (90004, 90004, 2001, 90001, 'ENGLISH',  80000,   200000,
   JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000))),
   15, 30, 40000, 'CLOSED_WON',
   JSON_OBJECT('startPriceCent',80000,'reservePriceCent',200000,'depositAmountCent',40000,
               'antiSnipingSec',15,'antiExtendSec',30,
               'incrementRule',JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000)))),
   '2025-11-01 20:32:00.000', '2025-11-01 20:42:00.000', 1004, 320000, '2025-11-01 20:42:00.000', 'AUTO',
   '2025-10-31 18:15:00.000', '2025-11-01 20:42:00.000'),

  -- Lot 5: 起拍 2000 元，CLOSED_WON，winner=1005，成交 7200 元
  (90005, 90005, 2001, 90001, 'ENGLISH',  200000,  400000,
   JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000))),
   15, 30, 60000, 'CLOSED_WON',
   JSON_OBJECT('startPriceCent',200000,'reservePriceCent',400000,'depositAmountCent',60000,
               'antiSnipingSec',15,'antiExtendSec',30,
               'incrementRule',JSON_OBJECT('type','ladder','maxBidSteps',5,'steps',
              JSON_ARRAY(JSON_OBJECT('min',0,'max',50000,'amount',1000),
                         JSON_OBJECT('min',50000,'max',200000,'amount',5000),
                         JSON_OBJECT('min',200000,'max',1000000,'amount',20000),
                         JSON_OBJECT('min',1000000,'amount',50000)))),
   '2025-11-01 20:42:30.000', '2025-11-01 20:52:30.000', 1005, 720000, '2025-11-01 20:52:30.000', 'AUTO',
   '2025-10-31 18:20:00.000', '2025-11-01 20:52:30.000');

-- ---- 5. 出价记录（bid_record）----
-- 每条出价均满足：
--   * bidder 已在该拍品的 deposit_ledger 中（见第 6 节）
--   * 出价金额 >= 上一价 + 阶梯 step，且 <= 上一价 + 5 * 阶梯 step
--   * created_at 落在 [start_time, end_time] 内，按时间递增
--   * bid_ts_ms = UNIX_TIMESTAMP(created_at) * 1000
--
-- request_id 用 "seed-demo-<auction_id>-<NN>" 保证幂等。

-- ---- 5.1 Lot 90001（10 条；start=10000，winner=1001，成交 50000）----
INSERT IGNORE INTO `bid_record`
  (`request_id`,             `auction_id`, `bidder_id`, `bid_price`, `bid_ts_ms`,    `source`,   `risk_result`, `reject_reason`, `created_at`)
VALUES
  ('seed-demo-90001-01', 90001, 1003, 11000, 1761998460000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:01:00.000'),
  ('seed-demo-90001-02', 90001, 1004, 13000, 1761998510000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:01:50.000'),
  ('seed-demo-90001-03', 90001, 1005, 17000, 1761998560000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:02:40.000'),
  ('seed-demo-90001-04', 90001, 1006, 22000, 1761998610000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:03:30.000'),
  ('seed-demo-90001-05', 90001, 1001, 27000, 1761998660000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:04:20.000'),
  ('seed-demo-90001-06', 90001, 1003, 32000, 1761998710000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:05:10.000'),
  ('seed-demo-90001-07', 90001, 1004, 37000, 1761998760000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:06:00.000'),
  ('seed-demo-90001-08', 90001, 1005, 42000, 1761998810000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:06:50.000'),
  ('seed-demo-90001-09', 90001, 1006, 47000, 1761998860000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:07:40.000'),
  ('seed-demo-90001-10', 90001, 1001, 50000, 1761998910000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:08:30.000');

-- ---- 5.2 Lot 90002（11 条；start=30000，winner=1003，成交 145000）----
INSERT IGNORE INTO `bid_record`
  (`request_id`,             `auction_id`, `bidder_id`, `bid_price`, `bid_ts_ms`,    `source`,   `risk_result`, `reject_reason`, `created_at`)
VALUES
  ('seed-demo-90002-01', 90002, 1001, 32000,  1761999090000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:11:30.000'),
  ('seed-demo-90002-02', 90002, 1004, 36000,  1761999140000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:12:20.000'),
  ('seed-demo-90002-03', 90002, 1005, 41000,  1761999190000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:13:10.000'),
  ('seed-demo-90002-04', 90002, 1006, 46000,  1761999240000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:14:00.000'),
  ('seed-demo-90002-05', 90002, 1003, 50000,  1761999290000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:14:50.000'),
  ('seed-demo-90002-06', 90002, 1001, 55000,  1761999340000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:15:40.000'),
  ('seed-demo-90002-07', 90002, 1004, 65000,  1761999390000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:16:30.000'),
  ('seed-demo-90002-08', 90002, 1005, 80000,  1761999440000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:17:20.000'),
  ('seed-demo-90002-09', 90002, 1006, 100000, 1761999490000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:18:10.000'),
  ('seed-demo-90002-10', 90002, 1001, 120000, 1761999540000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:19:00.000'),
  ('seed-demo-90002-11', 90002, 1003, 145000, 1761999590000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:19:50.000');

-- ---- 5.3 Lot 90003（11 条；start=300000，CLOSED_FAILED，最高 750000 < reserve 800000）----
INSERT IGNORE INTO `bid_record`
  (`request_id`,             `auction_id`, `bidder_id`, `bid_price`, `bid_ts_ms`,    `source`,   `risk_result`, `reject_reason`, `created_at`)
VALUES
  ('seed-demo-90003-01', 90003, 1001, 320000, 1761999720000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:22:00.000'),
  ('seed-demo-90003-02', 90003, 1003, 340000, 1761999770000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:22:50.000'),
  ('seed-demo-90003-03', 90003, 1004, 360000, 1761999820000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:23:40.000'),
  ('seed-demo-90003-04', 90003, 1005, 400000, 1761999870000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:24:30.000'),
  ('seed-demo-90003-05', 90003, 1006, 440000, 1761999920000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:25:20.000'),
  ('seed-demo-90003-06', 90003, 1001, 480000, 1761999970000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:26:10.000'),
  ('seed-demo-90003-07', 90003, 1003, 520000, 1762000020000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:27:00.000'),
  ('seed-demo-90003-08', 90003, 1004, 580000, 1762000070000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:27:50.000'),
  ('seed-demo-90003-09', 90003, 1005, 640000, 1762000120000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:28:40.000'),
  ('seed-demo-90003-10', 90003, 1006, 700000, 1762000170000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:29:30.000'),
  ('seed-demo-90003-11', 90003, 1001, 750000, 1762000220000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:30:20.000');

-- ---- 5.4 Lot 90004（11 条；start=80000，winner=1004，成交 320000）----
INSERT IGNORE INTO `bid_record`
  (`request_id`,             `auction_id`, `bidder_id`, `bid_price`, `bid_ts_ms`,    `source`,   `risk_result`, `reject_reason`, `created_at`)
VALUES
  ('seed-demo-90004-01', 90004, 1001, 85000,  1762000350000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:32:30.000'),
  ('seed-demo-90004-02', 90004, 1003, 95000,  1762000400000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:33:20.000'),
  ('seed-demo-90004-03', 90004, 1005, 110000, 1762000450000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:34:10.000'),
  ('seed-demo-90004-04', 90004, 1006, 130000, 1762000500000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:35:00.000'),
  ('seed-demo-90004-05', 90004, 1004, 150000, 1762000550000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:35:50.000'),
  ('seed-demo-90004-06', 90004, 1001, 170000, 1762000600000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:36:40.000'),
  ('seed-demo-90004-07', 90004, 1003, 195000, 1762000650000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:37:30.000'),
  ('seed-demo-90004-08', 90004, 1005, 215000, 1762000700000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:38:20.000'),
  ('seed-demo-90004-09', 90004, 1006, 240000, 1762000750000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:39:10.000'),
  ('seed-demo-90004-10', 90004, 1001, 280000, 1762000800000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:40:00.000'),
  ('seed-demo-90004-11', 90004, 1004, 320000, 1762000850000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:40:50.000');

-- ---- 5.5 Lot 90005（10 条；start=200000，winner=1005，成交 720000）----
INSERT IGNORE INTO `bid_record`
  (`request_id`,             `auction_id`, `bidder_id`, `bid_price`, `bid_ts_ms`,    `source`,   `risk_result`, `reject_reason`, `created_at`)
VALUES
  ('seed-demo-90005-01', 90005, 1001, 220000, 1762000980000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:43:00.000'),
  ('seed-demo-90005-02', 90005, 1003, 250000, 1762001030000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:43:50.000'),
  ('seed-demo-90005-03', 90005, 1004, 290000, 1762001080000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:44:40.000'),
  ('seed-demo-90005-04', 90005, 1006, 340000, 1762001130000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:45:30.000'),
  ('seed-demo-90005-05', 90005, 1005, 400000, 1762001180000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:46:20.000'),
  ('seed-demo-90005-06', 90005, 1001, 450000, 1762001230000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:47:10.000'),
  ('seed-demo-90005-07', 90005, 1003, 500000, 1762001280000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:48:00.000'),
  ('seed-demo-90005-08', 90005, 1004, 580000, 1762001330000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:48:50.000'),
  ('seed-demo-90005-09', 90005, 1006, 650000, 1762001380000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:49:40.000'),
  ('seed-demo-90005-10', 90005, 1005, 720000, 1762001430000, 'live_ws', 'ALLOW', NULL, '2025-11-01 20:50:30.000');

-- ---- 6. 订单（order_deal）----
-- 90001 已支付（PAID），其余三笔 CREATED / UNPAID。
-- pay_deadline = closed_at + 24h；paid_at 仅 PAID 单据有值。
INSERT IGNORE INTO `order_deal`
  (`id`,    `auction_id`, `winner_id`, `seller_id`, `deal_price`, `deposit_amount`, `status`,    `pay_status`, `pay_deadline`,            `paid_at`,                 `closed_at`,               `created_at`,              `updated_at`)
VALUES
  (90001,  90001,        1001,        2001,        50000,        8000,             'PAID',      'PAID',       '2025-11-02 20:10:30.000', '2025-11-01 20:15:30.000', '2025-11-01 20:15:30.000', '2025-11-01 20:10:30.000', '2025-11-01 20:15:30.000'),
  (90002,  90002,        1003,        2001,        145000,       15000,            'CREATED',   'UNPAID',     '2025-11-02 20:21:00.000', NULL,                      NULL,                      '2025-11-01 20:21:00.000', '2025-11-01 20:21:00.000'),
  (90003,  90004,        1004,        2001,        320000,       40000,            'CREATED',   'UNPAID',     '2025-11-02 20:42:00.000', NULL,                      NULL,                      '2025-11-01 20:42:00.000', '2025-11-01 20:42:00.000'),
  (90004,  90005,        1005,        2001,        720000,       60000,            'CREATED',   'UNPAID',     '2025-11-02 20:52:30.000', NULL,                      NULL,                      '2025-11-01 20:52:30.000', '2025-11-01 20:52:30.000');

-- ---- 7. 保证金账本（deposit_ledger）----
-- 5 个拍品 × 5 个买家 = 25 行。
-- 中标人：CAPTURED + related_order_id 指向 order_deal.id。
-- 其他参与者 / CLOSED_FAILED 拍品全部参与者：RELEASED。

-- ---- 7.1 Lot 90001：winner=1001，order_deal.id=90001 ----
INSERT IGNORE INTO `deposit_ledger`
  (`id`,    `auction_id`, `user_id`, `amount`, `status`,     `related_order_id`, `remark`,                          `created_at`,              `updated_at`)
VALUES
  (90001,  90001,        1001,      8000,     'CAPTURED',   90001,              '中标，保证金抵扣订单',             '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000'),
  (90002,  90001,        1003,      8000,     'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000'),
  (90003,  90001,        1004,      8000,     'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000'),
  (90004,  90001,        1005,      8000,     'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000'),
  (90005,  90001,        1006,      8000,     'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:10:30.000');

-- ---- 7.2 Lot 90002：winner=1003，order_deal.id=90002 ----
INSERT IGNORE INTO `deposit_ledger`
  (`id`,    `auction_id`, `user_id`, `amount`, `status`,     `related_order_id`, `remark`,                          `created_at`,              `updated_at`)
VALUES
  (90006,  90002,        1001,      15000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000'),
  (90007,  90002,        1003,      15000,    'CAPTURED',   90002,              '中标，保证金抵扣订单',             '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000'),
  (90008,  90002,        1004,      15000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000'),
  (90009,  90002,        1005,      15000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000'),
  (90010,  90002,        1006,      15000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:21:00.000');

-- ---- 7.3 Lot 90003：CLOSED_FAILED，全部 RELEASED ----
INSERT IGNORE INTO `deposit_ledger`
  (`id`,    `auction_id`, `user_id`, `amount`, `status`,     `related_order_id`, `remark`,                          `created_at`,              `updated_at`)
VALUES
  (90011,  90003,        1001,      50000,    'RELEASED',   NULL,               '流拍，保证金已释放',               '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000'),
  (90012,  90003,        1003,      50000,    'RELEASED',   NULL,               '流拍，保证金已释放',               '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000'),
  (90013,  90003,        1004,      50000,    'RELEASED',   NULL,               '流拍，保证金已释放',               '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000'),
  (90014,  90003,        1005,      50000,    'RELEASED',   NULL,               '流拍，保证金已释放',               '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000'),
  (90015,  90003,        1006,      50000,    'RELEASED',   NULL,               '流拍，保证金已释放',               '2025-11-01 19:50:00.000', '2025-11-01 20:31:30.000');

-- ---- 7.4 Lot 90004：winner=1004，order_deal.id=90003 ----
INSERT IGNORE INTO `deposit_ledger`
  (`id`,    `auction_id`, `user_id`, `amount`, `status`,     `related_order_id`, `remark`,                          `created_at`,              `updated_at`)
VALUES
  (90016,  90004,        1001,      40000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000'),
  (90017,  90004,        1003,      40000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000'),
  (90018,  90004,        1004,      40000,    'CAPTURED',   90003,              '中标，保证金抵扣订单',             '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000'),
  (90019,  90004,        1005,      40000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000'),
  (90020,  90004,        1006,      40000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:42:00.000');

-- ---- 7.5 Lot 90005：winner=1005，order_deal.id=90004 ----
INSERT IGNORE INTO `deposit_ledger`
  (`id`,    `auction_id`, `user_id`, `amount`, `status`,     `related_order_id`, `remark`,                          `created_at`,              `updated_at`)
VALUES
  (90021,  90005,        1001,      60000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000'),
  (90022,  90005,        1003,      60000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000'),
  (90023,  90005,        1004,      60000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000'),
  (90024,  90005,        1005,      60000,    'CAPTURED',   90004,              '中标，保证金抵扣订单',             '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000'),
  (90025,  90005,        1006,      60000,    'RELEASED',   NULL,               '未中标，保证金已释放',             '2025-11-01 19:50:00.000', '2025-11-01 20:52:30.000');

-- ---- 8. 收尾：恢复外键检查 ----
SET FOREIGN_KEY_CHECKS = 1;

-- ---- 9. 摘要：打印各表插入条数 ----
SELECT 'live_room' AS table_name, COUNT(*) AS rows_in_demo FROM `live_room`      WHERE `id` = 90001
UNION ALL
SELECT 'item',                    COUNT(*)                FROM `item`            WHERE `id` BETWEEN 90001 AND 90005
UNION ALL
SELECT 'auction_lot',             COUNT(*)                FROM `auction_lot`     WHERE `auction_id` BETWEEN 90001 AND 90005
UNION ALL
SELECT 'bid_record',              COUNT(*)                FROM `bid_record`      WHERE `auction_id` BETWEEN 90001 AND 90005
UNION ALL
SELECT 'order_deal',              COUNT(*)                FROM `order_deal`      WHERE `id` BETWEEN 90001 AND 90004
UNION ALL
SELECT 'deposit_ledger',          COUNT(*)                FROM `deposit_ledger`  WHERE `id` BETWEEN 90001 AND 90025
UNION ALL
SELECT 'demo_users',              COUNT(*)                FROM `user`            WHERE `id` IN (1001, 1003, 1004, 1005, 1006, 2001);
