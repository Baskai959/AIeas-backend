package runtime

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"aieas_backend/internal/domain"
)

const DefaultExpiredAuctionCleanupBatchSize = 100

type ExpiredAuctionLister interface {
	ListExpiredActive(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error)
}

type expiredAuctionHammer interface {
	Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error)
}

type ExpiredAuctionCleanupResult struct {
	Scanned int
	Closed  int
	Skipped int
	Failed  int
}

// ExpiredAuctionCleaner closes auctions whose DB countdown has already ended but whose
// status is still an active auction state.
type ExpiredAuctionCleaner struct {
	auctions  ExpiredAuctionLister
	hammer    expiredAuctionHammer
	batchSize int
}

func NewExpiredAuctionCleaner(auctions ExpiredAuctionLister, hammer expiredAuctionHammer, batchSize int) *ExpiredAuctionCleaner {
	if batchSize <= 0 {
		batchSize = DefaultExpiredAuctionCleanupBatchSize
	}
	return &ExpiredAuctionCleaner{auctions: auctions, hammer: hammer, batchSize: batchSize}
}

func (c *ExpiredAuctionCleaner) Cleanup(ctx context.Context) (ExpiredAuctionCleanupResult, error) {
	var result ExpiredAuctionCleanupResult
	if c == nil || c.auctions == nil || c.hammer == nil {
		return result, nil
	}
	for {
		now := time.Now().UTC()
		auctions, err := c.auctions.ListExpiredActive(ctx, now, c.batchSize)
		if err != nil {
			return result, err
		}
		if len(auctions) == 0 {
			return result, nil
		}
		batchClosed := 0
		for _, auction := range auctions {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			result.Scanned++
			requestID := "startup-cleanup-" + strconv.FormatUint(auction.AuctionID, 10) + "-" + strconv.FormatInt(now.UnixNano(), 10)
			closed, _, err := c.hammer.Hammer(ctx, domain.HammerInput{
				RequestID:      requestID,
				AuctionID:      auction.AuctionID,
				ActorID:        "system",
				ActorRole:      domain.RoleAdmin,
				ClosedBy:       "system",
				Now:            now,
				IdempotencyTTL: 24 * time.Hour,
			})
			if err != nil {
				if errors.Is(err, domain.ErrInvalidState) || errors.Is(err, domain.ErrOptimisticConflict) {
					result.Skipped++
					continue
				}
				result.Failed++
				slog.Default().Warn("startup expired auction cleanup failed", "auction_id", auction.AuctionID, "error", err)
				continue
			}
			if closed.Status.Terminal() {
				result.Closed++
				batchClosed++
			} else {
				result.Skipped++
			}
		}
		if len(auctions) < c.batchSize || batchClosed == 0 {
			return result, nil
		}
	}
}
