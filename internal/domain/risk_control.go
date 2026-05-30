package domain

// RiskControlConfig is a coarse runtime switch set for load testing and
// emergency operations. Defaults preserve production protection behavior.
type RiskControlConfig struct {
	Enabled bool `json:"enabled"`
}

func DefaultRiskControlConfig() RiskControlConfig {
	return RiskControlConfig{Enabled: true}
}
