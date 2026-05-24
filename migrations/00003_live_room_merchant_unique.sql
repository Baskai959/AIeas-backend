-- +goose Up
-- 业务约束：一个商家只能拥有一个直播间（live_room 与 merchant 1:1 关系）。
-- 注意：表中已存在 idx_merchant_status (merchant_id, status)，本迁移不会删除该索引；
-- 若存量数据中存在同一 merchant_id 拥有多条 live_room 的情况，请运维先去重再执行此迁移，
-- 否则唯一索引创建会失败。
ALTER TABLE `live_room`
  ADD UNIQUE KEY `uk_merchant` (`merchant_id`);

-- +goose Down
ALTER TABLE `live_room`
  DROP KEY `uk_merchant`;
