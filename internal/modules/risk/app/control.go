package app

import (
	"context"

	"aieas_backend/internal/domain"
)

// RiskControlService is intentionally static: it is sourced from config/env at
// process start so load tests cannot be affected by an admin-side DB update.
type RiskControlService struct {
	cfg domain.RiskControlConfig
}

func NewRiskControlService(cfg domain.RiskControlConfig) *RiskControlService {
	return &RiskControlService{cfg: cfg}
}

func (s *RiskControlService) Config(ctx context.Context) domain.RiskControlConfig {
	_ = ctx
	if s == nil {
		return domain.DefaultRiskControlConfig()
	}
	return s.cfg
}

func (s *RiskControlService) Enabled(ctx context.Context) bool {
	return s.Config(ctx).Enabled
}
