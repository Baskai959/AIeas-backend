package domain

const (
	ConfigKeyBlacklistStrategy = "risk.blacklist_strategy"

	DefaultBlacklistFrequencyWindowMs    int64 = 1000
	DefaultBlacklistFrequencyMaxRequests       = 10
)

// BlacklistStrategyConfig controls automatic blacklist rules on the bid path.
//
// Enabled is the global switch. Individual rule switches are kept true in the
// default value so enabling the policy from the admin side activates the full
// strategy unless the caller explicitly disables a rule.
type BlacklistStrategyConfig struct {
	Enabled                  bool  `json:"enabled"`
	FrequencyEnabled         bool  `json:"frequencyEnabled"`
	FrequencyWindowMs        int64 `json:"frequencyWindowMs"`
	FrequencyMaxRequests     int   `json:"frequencyMaxRequests"`
	UnreasonablePriceEnabled bool  `json:"unreasonablePriceEnabled"`
	MissingDepositEnabled    bool  `json:"missingDepositEnabled"`
	BlacklistDurationSeconds int64 `json:"blacklistDurationSeconds,omitempty"`
}

func DefaultBlacklistStrategyConfig() BlacklistStrategyConfig {
	return BlacklistStrategyConfig{
		Enabled:                  false,
		FrequencyEnabled:         true,
		FrequencyWindowMs:        DefaultBlacklistFrequencyWindowMs,
		FrequencyMaxRequests:     DefaultBlacklistFrequencyMaxRequests,
		UnreasonablePriceEnabled: true,
		MissingDepositEnabled:    true,
	}
}

func NormalizeBlacklistStrategyConfig(cfg BlacklistStrategyConfig) (BlacklistStrategyConfig, error) {
	if cfg.FrequencyWindowMs == 0 {
		cfg.FrequencyWindowMs = DefaultBlacklistFrequencyWindowMs
	}
	if cfg.FrequencyMaxRequests == 0 {
		cfg.FrequencyMaxRequests = DefaultBlacklistFrequencyMaxRequests
	}
	if cfg.FrequencyWindowMs < 0 || cfg.FrequencyMaxRequests < 0 || cfg.BlacklistDurationSeconds < 0 {
		return BlacklistStrategyConfig{}, ErrInvalidArgument
	}
	if cfg.FrequencyWindowMs > 60000 || cfg.FrequencyMaxRequests > 10000 {
		return BlacklistStrategyConfig{}, ErrInvalidArgument
	}
	return cfg, nil
}
