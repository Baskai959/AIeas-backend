package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/repository"
)

type BidRecordWriter struct {
	repo       repository.BidRepository
	log        bidRecordEventLog
	consumer   string
	maxRetries int64
	claimIdle  time.Duration
	interval   time.Duration
}

type bidRecordEventLog interface {
	Enabled() bool
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error)
	ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error)
	AckBidRecord(ctx context.Context, auctionID uint64, ids ...string) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
}

func NewBidRecordWriter(repo repository.BidRepository, log *redisinfra.EventLog, consumer string) *BidRecordWriter {
	if consumer == "" {
		consumer = fmt.Sprintf("bid-record-%d", time.Now().UTC().UnixNano())
	}
	return &BidRecordWriter{repo: repo, log: log, consumer: consumer, maxRetries: 5, claimIdle: 30 * time.Second, interval: time.Second}
}

func (w *BidRecordWriter) Start(ctx context.Context) {
	if w == nil || w.repo == nil || w.log == nil || !w.log.Enabled() {
		return
	}
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.runOnce(ctx)
			}
		}
	}()
}

func (w *BidRecordWriter) runOnce(ctx context.Context) {
	auctions, err := w.log.ActiveAuctions(ctx)
	if err != nil {
		return
	}
	for _, auctionID := range auctions {
		claimed, err := w.log.ClaimStaleBidRecordEvents(ctx, auctionID, w.consumer, w.claimIdle, 32)
		if err == nil {
			w.handleEvents(ctx, claimed)
		}
		events, err := w.log.ReadBidRecordGroup(ctx, auctionID, w.consumer, 64, 10*time.Millisecond)
		if err == nil {
			w.handleEvents(ctx, events)
		}
	}
}

func (w *BidRecordWriter) handleEvents(ctx context.Context, events []redisinfra.BidEvent) {
	for _, event := range events {
		if event.EventType != "bid.accepted" && event.EventType != "bid.rejected" {
			_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
			continue
		}
		if event.Deliveries >= w.maxRetries {
			_ = w.log.WriteBidRecordDLQ(ctx, event, "MAX_RETRIES")
			_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
			continue
		}
		result, err := WriteBidRecordIdempotent(ctx, w.repo, event)
		switch result {
		case BidRecordWriteOK, BidRecordWriteDuplicateConsistent:
			_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		case BidRecordWriteDuplicateConflict:
			_ = w.log.WriteBidRecordDLQ(ctx, event, "DUPLICATE_CONFLICT")
			_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		default:
			if err != nil && event.Deliveries+1 >= w.maxRetries {
				_ = w.log.WriteBidRecordDLQ(ctx, event, "MAX_RETRIES")
				_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
			}
		}
	}
}

type BidRecordWriteResult int

const (
	BidRecordWriteOK BidRecordWriteResult = iota
	BidRecordWriteDuplicateConsistent
	BidRecordWriteDuplicateConflict
	BidRecordWriteTemporaryFailure
)

func WriteBidRecordIdempotent(ctx context.Context, repo repository.BidRepository, event redisinfra.BidEvent) (BidRecordWriteResult, error) {
	record := event.ToBidRecord()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	err := repo.Create(ctx, &record)
	if err == nil {
		return BidRecordWriteOK, nil
	}
	if !errors.Is(err, domain.ErrConflict) && !isDuplicateError(err) {
		return BidRecordWriteTemporaryFailure, err
	}
	existing, findErr := repo.FindByRequestID(ctx, event.RequestID)
	if findErr != nil {
		return BidRecordWriteTemporaryFailure, findErr
	}
	if bidRecordConsistent(existing, event.ToBidRecord()) {
		return BidRecordWriteDuplicateConsistent, nil
	}
	return BidRecordWriteDuplicateConflict, err
}

func bidRecordConsistent(existing, incoming domain.BidRecord) bool {
	return existing.RequestID == incoming.RequestID && existing.AuctionID == incoming.AuctionID && existing.BidderID == incoming.BidderID && existing.BidPrice == incoming.BidPrice && existing.BidTSMS == incoming.BidTSMS && existing.Source == incoming.Source && existing.RiskResult == incoming.RiskResult && existing.RejectReason == incoming.RejectReason
}

func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique") || strings.Contains(msg, "1062")
}

type BidRecordReconciler struct {
	repo repository.BidRepository
	log  bidRecordReconcileLog
}

type bidRecordReconcileLog interface {
	Enabled() bool
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ReconcileCheckpoint(ctx context.Context, auctionID uint64) (int64, error)
	ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error)
	SetReconcileCheckpoint(ctx context.Context, auctionID uint64, seq int64) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
}

func NewBidRecordReconciler(repo repository.BidRepository, log *redisinfra.EventLog) *BidRecordReconciler {
	return &BidRecordReconciler{repo: repo, log: log}
}

func (r *BidRecordReconciler) Start(ctx context.Context, interval time.Duration) {
	if r == nil || r.repo == nil || r.log == nil || !r.log.Enabled() {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		_ = r.RunOnce(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = r.RunOnce(ctx)
			}
		}
	}()
}

func (r *BidRecordReconciler) RunOnce(ctx context.Context) error {
	if r == nil || r.repo == nil || r.log == nil || !r.log.Enabled() {
		return nil
	}
	auctions, err := r.log.ActiveAuctions(ctx)
	if err != nil {
		return err
	}
	for _, auctionID := range auctions {
		lastSeq, err := r.log.ReconcileCheckpoint(ctx, auctionID)
		if err != nil {
			return err
		}
		events, complete, err := r.log.ReplayBidEvents(ctx, auctionID, lastSeq, 512)
		if err != nil {
			return err
		}
		if !complete {
			_ = r.log.WriteBidRecordDLQ(ctx, redisinfra.BidEvent{AuctionID: auctionID, Seq: lastSeq}, "RECONCILE_GAP")
			continue
		}
		for _, event := range events {
			if event.EventType != "bid.accepted" && event.EventType != "bid.rejected" {
				continue
			}
			result, err := WriteBidRecordIdempotent(ctx, r.repo, event)
			if result == BidRecordWriteDuplicateConflict {
				_ = r.log.WriteBidRecordDLQ(ctx, event, "RECONCILE_DUPLICATE_CONFLICT")
			} else if result == BidRecordWriteTemporaryFailure {
				return err
			}
			if event.Seq > lastSeq {
				lastSeq = event.Seq
			}
		}
		if err := r.log.SetReconcileCheckpoint(ctx, auctionID, lastSeq); err != nil {
			return err
		}
	}
	return nil
}
