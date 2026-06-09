-- =============================================================================
-- seed_laozhao_demo_users.sql
-- 添加 1 个商家「老赵古玩店」和 5 个买家用户。
--
-- 说明：
--   * user.id 是 BIGINT 数字主键，不能写入字符串 laozhao
--   * 这里将 laozhao 作为商家的登录账号 account
--
-- 登录信息：
--   商家 account: laozhao
--   买家 account: buyer_linyutong / buyer_chenanran / buyer_zhouxinghe /
--                 buyer_zhaonianqing / buyer_wujingxing
--   password    : 123456
--
-- ID 段：
--   merchant user.id : 93000001
--   buyer user.id    : 93000011 ~ 93000015
--
-- 可重放性：
--   * 使用 INSERT IGNORE，重复执行不会修改已有用户
--   * 若目标 ID / account / phone 已被占用，对应行会被跳过
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';

INSERT IGNORE INTO `user`
  (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`)
VALUES
  (93000001, 'laozhao',           '13993000001', '老赵古玩店',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'merchant', 'ACTIVE'),

  (93000011, 'buyer_linyutong',   '13993000011', '林雨桐',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'buyer', 'ACTIVE'),
  (93000012, 'buyer_chenanran',   '13993000012', '陈安然',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'buyer', 'ACTIVE'),
  (93000013, 'buyer_zhouxinghe',  '13993000013', '周星河',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'buyer', 'ACTIVE'),
  (93000014, 'buyer_zhaonianqing','13993000014', '赵念青',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'buyer', 'ACTIVE'),
  (93000015, 'buyer_wujingxing',  '13993000015', '吴景行',
   '20bd9b829efea7e9f0fcfedeafee52b76495609635c6f6d8ebe70887fba09725', 'buyer', 'ACTIVE');

SELECT
  CONCAT('u_', `id`) AS `userId`,
  `account`,
  `nickname`,
  `role`,
  '123456' AS `password`
FROM `user`
WHERE `id` IN (93000001, 93000011, 93000012, 93000013, 93000014, 93000015)
ORDER BY `id`;
