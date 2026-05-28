-- +goose Up
INSERT INTO `config_item` (`config_key`, `config_value`, `description`)
VALUES ('live.agent_hook.default', JSON_OBJECT('enabled', false), '直播拍卖 AI Agent hook 默认开关')
ON DUPLICATE KEY UPDATE `description` = VALUES(`description`);

-- +goose Down
DELETE FROM `config_item` WHERE `config_key` = 'live.agent_hook.default';

