-- +goose Up
-- +goose StatementBegin
ALTER TABLE `live_analysis_report`
  ADD COLUMN `agent_request_id` VARCHAR(128) DEFAULT NULL COMMENT 'Agent 异步任务 ID，用于回调兜底定位' AFTER `task_id`,
  ADD COLUMN `live_session_id` BIGINT UNSIGNED DEFAULT NULL COMMENT '直播场次 ID，与 live_session.id 对应' AFTER `merchant_id`,
  ADD COLUMN `attempt_count` INT NOT NULL DEFAULT 0 COMMENT '已请求 Agent 生成次数，最多 3 次' AFTER `status`,
  ADD UNIQUE KEY `uk_live_session` (`live_session_id`),
  ADD KEY `idx_agent_request` (`agent_request_id`),
  ADD KEY `idx_session_status` (`live_session_id`, `status`);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `live_analysis_report`
  DROP KEY `idx_session_status`,
  DROP KEY `idx_agent_request`,
  DROP KEY `uk_live_session`,
  DROP COLUMN `attempt_count`,
  DROP COLUMN `live_session_id`,
  DROP COLUMN `agent_request_id`;
-- +goose StatementEnd
