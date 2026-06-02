-- =============================================================================
-- seed_1000_more_buyers.sql
-- 继续 seed_1000_buyers.sql 的规则，再额外生成 1000 个可登录买家账号。
--
-- ID 段：
--   user.id       : 91001101 ~ 91002100
--   API/domain ID : u_91001101 ~ u_91002100
--
-- 登录信息：
--   account  : loadbuyer1101 ~ loadbuyer2100
--   password : Passw0rd!
--
-- 可重放性：
--   * 使用 INSERT IGNORE，重复执行不会修改已有用户
--   * 若目标 ID / account / phone 已被占用，对应行会被跳过
--
-- 注意：
--   * 本脚本只插入 user 表，不创建 deposit_ledger
--   * 这些买家执行报名接口后，才会针对具体拍品生成 READY 保证金账本
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';

-- 统一成 4 位补零账号格式：loadbuyer1101 ~ loadbuyer2100。
UPDATE `user` AS u
LEFT JOIN `user` AS c
  ON c.`account` = CONCAT('loadbuyer', LPAD(u.`id` - 91000000, 4, '0'))
 AND c.`id` <> u.`id`
SET
  u.`account` = CONCAT('loadbuyer', LPAD(u.`id` - 91000000, 4, '0')),
  u.`nickname` = CONCAT('压测买家', LPAD(u.`id` - 91000000, 4, '0'))
WHERE u.`id` BETWEEN 91001101 AND 91002100
  AND u.`role` = 'buyer'
  AND c.`id` IS NULL
  AND (
    u.`account` <> CONCAT('loadbuyer', LPAD(u.`id` - 91000000, 4, '0'))
    OR u.`nickname` <> CONCAT('压测买家', LPAD(u.`id` - 91000000, 4, '0'))
  );

INSERT IGNORE INTO `user`
  (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`)
SELECT
  91000000 + seq.n AS `id`,
  CONCAT('loadbuyer', LPAD(seq.n, 4, '0')) AS `account`,
  CONCAT('13991', LPAD(seq.n, 6, '0')) AS `phone`,
  CONCAT('压测买家', LPAD(seq.n, 4, '0')) AS `nickname`,
  'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca' AS `password_hash`,
  'buyer' AS `role`,
  'ACTIVE' AS `status`
FROM (
  SELECT hundreds.i * 100 + tens.i * 10 + ones.i + 1101 AS n
  FROM (
    SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) AS ones
  CROSS JOIN (
    SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) AS tens
  CROSS JOIN (
    SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) AS hundreds
) AS seq
WHERE seq.n BETWEEN 1101 AND 2100
ORDER BY seq.n;

SELECT
  COUNT(*) AS `buyerCount`,
  MIN(CONCAT('u_', `id`)) AS `firstBuyerId`,
  MAX(CONCAT('u_', `id`)) AS `lastBuyerId`
FROM `user`
WHERE `id` BETWEEN 91001101 AND 91002100
  AND `role` = 'buyer';

SELECT
  CONCAT('u_', `id`) AS `buyerId`,
  `account`,
  'Passw0rd!' AS `password`
FROM `user`
WHERE `id` BETWEEN 91001101 AND 91002100
  AND `role` = 'buyer'
ORDER BY `id`;
