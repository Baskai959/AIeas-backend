-- =============================================================================
-- seed_100_buyers.sql
-- 生成 100 个可登录买家账号，适合本地开发、报名/出价压测。
--
-- ID 段：
--   user.id       : 91000001 ~ 91000100
--   API/domain ID : u_91000001 ~ u_91000100
--
-- 登录信息：
--   account  : loadbuyer001 ~ loadbuyer100
--   password : Passw0rd!
--
-- 可重放性：
--   * 使用 INSERT IGNORE，重复执行不会修改已有用户
--   * 若目标 ID / account / phone 已被占用，对应行会被跳过
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+08:00';

INSERT IGNORE INTO `user`
  (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`)
SELECT
  91000000 + seq.n AS `id`,
  CONCAT('loadbuyer', LPAD(seq.n, 3, '0')) AS `account`,
  CONCAT('13991', LPAD(seq.n, 6, '0')) AS `phone`,
  CONCAT('压测买家', LPAD(seq.n, 3, '0')) AS `nickname`,
  'e027cbdb3f9674449886392eaefd930e17d60411538b6fd2b7612431134e7fca' AS `password_hash`,
  'buyer' AS `role`,
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
  CONCAT('u_', `id`) AS `buyerId`,
  `account`,
  'Passw0rd!' AS `password`
FROM `user`
WHERE `id` BETWEEN 91000001 AND 91000100
  AND `role` = 'buyer'
ORDER BY `id`;
