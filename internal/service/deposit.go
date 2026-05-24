package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type DepositService struct {
	deposits repository.DepositRepository
	auctions repository.AuctionRepository
	realtime repository.AuctionRealtimeStore
	risk     *RiskService
	tx       repository.TxManager
}

type EnrollInput struct {
	AuctionID uint64
	UserID    string
	UserRole  domain.Role
}

func NewDepositService(deposits repository.DepositRepository, auctions repository.AuctionRepository, realtime repository.AuctionRealtimeStore, risk *RiskService, tx repository.TxManager) *DepositService {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return &DepositService{deposits: deposits, auctions: auctions, realtime: realtime, risk: risk, tx: tx}
}

func (s *DepositService) Enroll(ctx context.Context, in EnrollInput) (domain.DepositLedger, error) {
	in.UserID = strings.TrimSpace(in.UserID)
	if in.AuctionID == 0 || in.UserID == "" || in.UserRole != domain.RoleBuyer {
		return domain.DepositLedger{}, domain.ErrInvalidArgument
	}
	if s.risk != nil {
		blacklisted, err := s.risk.IsBlacklisted(ctx, in.UserID)
		if err != nil {
			return domain.DepositLedger{}, err
		}
		if blacklisted {
			return domain.DepositLedger{}, domain.ErrForbidden
		}
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.DepositLedger{}, err
	}
	if auction.Status != domain.AuctionStatusReady &&
		auction.Status != domain.AuctionStatusRunning &&
		auction.Status != domain.AuctionStatusExtended &&
		auction.Status != domain.AuctionStatusWarmingUp {
		return domain.DepositLedger{}, domain.ErrInvalidState
	}
	var deposit domain.DepositLedger
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		existing, findErr := s.deposits.FindByAuctionUser(txCtx, in.AuctionID, in.UserID)
		if findErr == nil {
			if existing.Status == domain.DepositStatusReady || existing.Status == domain.DepositStatusCaptured {
				deposit = existing
				return nil
			}
			existing.Amount = auction.DepositAmount
			existing.Status = domain.DepositStatusReady
			existing.Remark = "enrolled"
			if err := s.deposits.Update(txCtx, &existing); err != nil {
				return err
			}
			deposit = existing
			return nil
		}
		if !errors.Is(findErr, domain.ErrNotFound) {
			return findErr
		}
		now := time.Now().UTC()
		created := domain.DepositLedger{
			AuctionID: in.AuctionID,
			UserID:    in.UserID,
			Amount:    auction.DepositAmount,
			Status:    domain.DepositStatusReady,
			Remark:    "enrolled",
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := s.deposits.Create(txCtx, &created); err != nil {
			if errors.Is(err, domain.ErrConflict) {
				existing, findErr := s.deposits.FindByAuctionUser(txCtx, in.AuctionID, in.UserID)
				if findErr == nil {
					deposit = existing
					return nil
				}
			}
			return err
		}
		deposit = created
		return nil
	}); err != nil {
		return domain.DepositLedger{}, err
	}
	if err := s.realtime.MarkEnrollment(ctx, in.AuctionID, in.UserID); err != nil {
		return domain.DepositLedger{}, err
	}
	return deposit, nil
}
