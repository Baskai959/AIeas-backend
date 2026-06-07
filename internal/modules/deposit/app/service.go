package app

import (
	"context"
	"errors"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	depositports "aieas_backend/internal/modules/deposit/ports"
)

type DepositService struct {
	deposits     depositports.DepositRepository
	auctions     depositports.AuctionRepository
	realtime     depositports.AuctionRealtimeStore
	risk         depositports.BlacklistReader
	tx           depositports.TxManager
	controls     depositports.RiskControlUseCase
	participants AuctionParticipantNotifier
	telemetry    DepositTelemetry
}

type DepositTelemetry interface {
	StartEnroll(ctx context.Context, auctionID uint64, userID string) (context.Context, DepositSpan)
	ObserveEnroll(result string, elapsed time.Duration)
	IncDepositReady()
	IncDepositSyncRedisFail()
}

type DepositSpan interface {
	End()
	SetDepositStatus(status string)
	RecordError(err error)
}

type AuctionParticipantNotifier interface {
	NotifyParticipantUpdated(ctx context.Context, auctionID uint64, participantCount int) int
}

type auctionStateReader interface {
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
}

type EnrollInput struct {
	AuctionID uint64
	UserID    string
	UserRole  domain.Role
}

func NewDepositService(deposits depositports.DepositRepository, auctions depositports.AuctionRepository, realtime depositports.AuctionRealtimeStore, risk depositports.BlacklistReader, tx depositports.TxManager) *DepositService {
	return &DepositService{
		deposits: deposits,
		auctions: auctions,
		realtime: realtime,
		risk:     risk,
		tx:       tx,
	}
}

func (s *DepositService) SetRiskControlService(controls depositports.RiskControlUseCase) {
	s.controls = controls
}

func (s *DepositService) SetParticipantNotifier(notifier AuctionParticipantNotifier) {
	s.participants = notifier
}

// SetMetrics 注入观测性 Registry。nil 安全。
func (s *DepositService) SetTelemetry(telemetry DepositTelemetry) {
	if s == nil {
		return
	}
	s.telemetry = telemetry
}

func (s *DepositService) Enroll(ctx context.Context, in EnrollInput) (domain.DepositLedger, error) {
	ctx, span := s.startEnrollSpan(ctx, in)
	defer span.End()
	start := time.Now()
	deposit, err := s.enroll(ctx, in)
	elapsed := time.Since(start)
	span.SetDepositStatus(string(deposit.Status))
	if err != nil {
		span.RecordError(err)
	}
	if s.telemetry != nil {
		s.observeEnrollMetrics(err, deposit, elapsed)
	}
	return deposit, err
}

func (s *DepositService) enroll(ctx context.Context, in EnrollInput) (domain.DepositLedger, error) {
	in.UserID = strings.TrimSpace(in.UserID)
	if in.AuctionID == 0 || in.UserID == "" || in.UserRole != domain.RoleBuyer {
		return domain.DepositLedger{}, domain.ErrInvalidArgument
	}
	if s.risk != nil && s.blacklistCheckEnabled(ctx) {
		blacklisted, err := s.risk.IsBlacklisted(ctx, in.UserID)
		if err != nil {
			return domain.DepositLedger{}, err
		}
		if blacklisted {
			return domain.DepositLedger{}, domain.ErrForbidden
		}
	}
	if s.auctions == nil || s.deposits == nil {
		return domain.DepositLedger{}, domain.ErrNotFound
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
	if err := s.withinTx(ctx, func(txCtx context.Context) error {
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
	if s.realtime != nil {
		if err := s.realtime.MarkEnrollment(ctx, in.AuctionID, in.UserID); err != nil {
			if s.telemetry != nil {
				s.telemetry.IncDepositSyncRedisFail()
			}
			return domain.DepositLedger{}, err
		}
	}
	s.notifyParticipantUpdated(ctx, in.AuctionID)
	return deposit, nil
}

func (s *DepositService) notifyParticipantUpdated(ctx context.Context, auctionID uint64) {
	if s == nil || s.participants == nil || auctionID == 0 {
		return
	}
	reader, ok := s.realtime.(auctionStateReader)
	if !ok {
		return
	}
	state, exists, err := reader.GetAuctionState(ctx, auctionID)
	if err != nil || !exists || state.ParticipantCount <= 0 {
		return
	}
	s.participants.NotifyParticipantUpdated(ctx, auctionID, state.ParticipantCount)
}

func (s *DepositService) blacklistCheckEnabled(ctx context.Context) bool {
	if s.controls == nil {
		return true
	}
	return s.controls.Enabled(ctx)
}

func (s *DepositService) withinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if s.tx == nil {
		return fn(ctx)
	}
	return s.tx.WithinTx(ctx, fn)
}

func (s *DepositService) observeEnrollMetrics(err error, deposit domain.DepositLedger, elapsed time.Duration) {
	switch {
	case err == nil:
		s.telemetry.ObserveEnroll("ok", elapsed)
		if deposit.Status == domain.DepositStatusReady {
			s.telemetry.IncDepositReady()
		}
	case errors.Is(err, domain.ErrInvalidArgument):
		s.telemetry.ObserveEnroll("invalid_argument", elapsed)
	case errors.Is(err, domain.ErrForbidden):
		s.telemetry.ObserveEnroll("forbidden", elapsed)
	case errors.Is(err, domain.ErrInvalidState):
		s.telemetry.ObserveEnroll("invalid_state", elapsed)
	case errors.Is(err, domain.ErrNotFound):
		s.telemetry.ObserveEnroll("not_found", elapsed)
	default:
		s.telemetry.ObserveEnroll("error", elapsed)
	}
}

func (s *DepositService) startEnrollSpan(ctx context.Context, in EnrollInput) (context.Context, DepositSpan) {
	if s == nil || s.telemetry == nil {
		return ctx, noopDepositSpan{}
	}
	return s.telemetry.StartEnroll(ctx, in.AuctionID, in.UserID)
}

type noopDepositSpan struct{}

func (noopDepositSpan) End() {}

func (noopDepositSpan) SetDepositStatus(string) {}

func (noopDepositSpan) RecordError(error) {}
