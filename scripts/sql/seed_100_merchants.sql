-- =============================================================================
-- seed_100_merchants.sql
-- 生成 100 个可登录商家账号，适合本地开发、商品/拍品/直播间压测。
--
-- ID 段：
--   user.id       : 92000001 ~ 92000100
--   API/domain ID : u_92000001 ~ u_92000100
--
-- 登录信息：
--   account  : loadmerchant0001 ~ loadmerchant0100
--   password : Passw0rd!
--
-- 可重放性：
--   * 使用 INSERT IGNORE，重复执行不会修改已有用户
--   * 若目标 ID / account / phone 已被占用，对应行会被跳过
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';

-- 统一成 4 位补零账号格式：loadmerchant0001 ~ loadmerchant0100。
UPDATE `user` AS u
LEFT JOIN `user` AS c
  ON c.`account` = CONCAT('loadmerchant', LPAD(u.`id` - 92000000, 4, '0'))
 AND c.`id` <> u.`id`
SET
  u.`account` = CONCAT('loadmerchant', LPAD(u.`id` - 92000000, 4, '0')),
  u.`nickname` = CONCAT('压测商家', LPAD(u.`id` - 92000000, 4, '0'))
WHERE u.`id` BETWEEN 92000001 AND 92000100
  AND u.`role` = 'merchant'
  AND c.`id` IS NULL
  AND (
    u.`account` <> CONCAT('loadmerchant', LPAD(u.`id` - 92000000, 4, '0'))
    OR u.`nickname` <> CONCAT('压测商家', LPAD(u.`id` - 92000000, 4, '0'))
  );

INSERT IGNORE INTO `user`
  (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`)
SELECT
  92000000 + seq.n AS `id`,
  CONCAT('loadmerchant', LPAD(seq.n, 4, '0')) AS `account`,
  CONCAT('13992', LPAD(seq.n, 6, '0')) AS `phone`,
  CONCAT('压测商家', LPAD(seq.n, 4, '0')) AS `nickname`,
  'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca' AS `password_hash`,
  'merchant' AS `role`,
  'ACTIVE' AS `status`
FROM (
  SELECT tens.i * 10 + ones.i + 1 AS n
  FROM (
    SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) AS ones
  CROSS JOIN (
    SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) AS tens
) AS seq
WHERE seq.n BETWEEN 1 AND 100
ORDER BY seq.n;

SELECT
  COUNT(*) AS `merchantCount`,
  MIN(CONCAT('u_', `id`)) AS `firstMerchantId`,
  MAX(CONCAT('u_', `id`)) AS `lastMerchantId`
FROM `user`
WHERE `id` BETWEEN 92000001 AND 92000100
  AND `role` = 'merchant';

SELECT
  CONCAT('u_', `id`) AS `merchantId`,
  `account`,
  'Passw0rd!' AS `password`
FROM `user`
WHERE `id` BETWEEN 92000001 AND 92000100
  AND `role` = 'merchant'
ORDER BY `id`;
