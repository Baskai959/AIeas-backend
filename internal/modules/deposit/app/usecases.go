package app

import (
	"context"

	"aieas_backend/internal/domain"
)

// DepositUseCase 暴露保证金报名用例边界。
type DepositUseCase interface {
	Enroll(ctx context.Context, in EnrollInput) (domain.DepositLedger, error)
}
