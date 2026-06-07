package service

import (
	depositapp "aieas_backend/internal/modules/deposit/app"
	"aieas_backend/internal/tests/repository"
)

type DepositService = depositapp.DepositService
type EnrollInput = depositapp.EnrollInput

func NewDepositService(deposits repository.DepositRepository, auctions repository.AuctionRepository, realtime repository.AuctionRealtimeStore, risk *RiskService, tx repository.TxManager) *DepositService {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return depositapp.NewDepositService(deposits, auctions, realtime, risk, tx)
}
