-- +goose Up
-- +goose StatementBegin
SET NAMES utf8mb4;
-- +goose StatementEnd

-- ---------------------------------------------------------------------
-- live_analysis_report 直播总结报告表
--   前端发起 AI 直播总结后，后端持久化任务状态、prompt、生成结果与失败原因。
-- ---------------------------------------------------------------------
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS `live_analysis_report` (
  `task_id`       VARCHAR(64)   NOT NULL                COMMENT '报告生成任务 ID（lar_xxx）',
  `merchant_id`   VARCHAR(64)   NOT NULL                COMMENT '商家 ID（冗余便于按商家查询）',
  `status`        VARCHAR(16)   NOT NULL                COMMENT '任务状态：PENDING/RUNNING/SUCCEEDED/FAILED',
  `prompt`        TEXT          NOT NULL                COMMENT '发送给 Agent 的提示词',
  `report`        MEDIUMTEXT    DEFAULT NULL            COMMENT 'AI 生成的直播总结内容',
  `error_message` VARCHAR(1024) DEFAULT NULL            COMMENT '失败原因',
  `created_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`task_id`),
  KEY `idx_merchant_created` (`merchant_id`, `created_at`),
  KEY `idx_status_updated` (`status`, `updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='直播 AI 总结报告';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS `live_analysis_report`;
-- +goose StatementEnd
