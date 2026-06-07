package runtime

import (
	"context"
	"log/slog"
	"time"

	"aieas_backend/internal/domain"
)

const (
	DefaultScheduledAuctionStartInterval  = time.Second
	DefaultScheduledAuctionStartBatchSize = 100
)

type ScheduledAuctionLister interface {
	ListDueScheduled(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error)
}

type ScheduledAuctionActivator interface {
	ActivateDueScheduledAuction(ctx context.Context, auction domain.AuctionLot, now time.Time) (domain.AuctionLot, error)
}

type ScheduledAuctionStarter struct {
	auctions  ScheduledAuctionLister
	activator ScheduledAuctionActivator
	interval  time.Duration
	batchSize int
}

func NewScheduledAuctionStarter(auctions ScheduledAuctionLister, activator ScheduledAuctionActivator, interval time.Duration, batchSize int) *ScheduledAuctionStarter {
	if interval <= 0 {
		interval = DefaultScheduledAuctionStartInterval
	}
	if batchSize <= 0 || batchSize > 500 {
		batchSize = DefaultScheduledAuctionStartBatchSize
	}
	return &ScheduledAuctionStarter{
		auctions:  auctions,
		activator: activator,
		interval:  interval,
		batchSize: batchSize,
	}
}

func (s *ScheduledAuctionStarter) Start(ctx context.Context) {
	if s == nil || s.auctions == nil || s.activator == nil {
		return
	}
	go s.loop(ctx)
}

func (s *ScheduledAuctionStarter) loop(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.scan(ctx, time.Now().UTC())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.scan(ctx, now.UTC())
		}
	}
}

func (s *ScheduledAuctionStarter) scan(ctx context.Context, now time.Time) {
	auctions, err := s.auctions.ListDueScheduled(ctx, now, s.batchSize)
	if err != nil {
		slog.Default().Warn("list due scheduled auctions failed", "error", err)
		return
	}
	for _, auction := range auctions {
		if _, err := s.activator.ActivateDueScheduledAuction(ctx, auction, now); err != nil {
			slog.Default().Warn("activate due scheduled auction failed", "auction_id", auction.AuctionID, "live_session_id", liveSessionIDValue(auction.LiveSessionID), "error", err)
		}
	}
}

func liveSessionIDValue(id *uint64) uint64 {
	if id == nil {
		return 0
	}
	return *id
}
