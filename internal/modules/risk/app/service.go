package app

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	riskports "aieas_backend/internal/modules/risk/ports"
)

type RiskService struct {
	repo      riskports.RiskRepository
	publisher riskports.EventPublisher
	cache     riskports.BlacklistCache
}

func NewRiskService(repo riskports.RiskRepository, publisher riskports.EventPublisher) *RiskService {
	return &RiskService{repo: repo, publisher: publisher}
}

func (s *RiskService) SetBlacklistCache(cache riskports.BlacklistCache) {
	if s == nil {
		return
	}
	s.cache = cache
}

func (s *RiskService) IsBlacklisted(ctx context.Context, userID string) (bool, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return false, domain.ErrInvalidArgument
	}
	if s.repo == nil {
		return false, nil
	}
	if s.cache != nil {
		ok, err := s.cache.GetOrLoad(ctx, userID, func(loadCtx context.Context) (bool, bool, error) {
			hit, err := s.repo.IsBlacklisted(loadCtx, userID, time.Now().UTC())
			if err != nil {
				return false, false, err
			}
			return hit, hit, nil
		})
		if err != nil {
			return false, nil
		}
		return ok, nil
	}
	ok, err := s.repo.IsBlacklisted(ctx, userID, time.Now().UTC())
	if err != nil {
		return false, nil
	}
	return ok, nil
}

func (s *RiskService) AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return domain.ErrInvalidArgument
	}
	item := &domain.Blacklist{
		UserID:    userID,
		Reason:    strings.TrimSpace(reason),
		CreatedBy: strings.TrimSpace(actorID),
		ExpiresAt: expiresAt,
		CreatedAt: time.Now().UTC(),
	}
	if item.Reason == "" {
		item.Reason = "manual"
	}
	if s.repo != nil {
		if err := s.repo.CreateBlacklist(ctx, item); err != nil && err != domain.ErrConflict {
			return err
		}
	}
	if s.cache != nil {
		_ = s.cache.Invalidate(ctx, userID)
	}
	return nil
}

func (s *RiskService) RemoveBlacklist(ctx context.Context, userID string) error {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return domain.ErrInvalidArgument
	}
	if s.repo != nil {
		if err := s.repo.DeleteBlacklist(ctx, userID); err != nil {
			return err
		}
	}
	if s.cache != nil {
		_ = s.cache.Invalidate(ctx, userID)
	}
	return nil
}

func (s *RiskService) ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error) {
	if s.repo == nil {
		return []domain.Blacklist{}, nil
	}
	return s.repo.ListBlacklist(ctx, limit, offset)
}

func (s *RiskService) ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error) {
	if s.repo == nil {
		return []domain.RiskEvent{}, nil
	}
	return s.repo.ListEvents(ctx, filter)
}

func (s *RiskService) HandleEvent(ctx context.Context, id uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error) {
	if s.repo == nil || id == 0 {
		return domain.RiskEvent{}, domain.ErrInvalidArgument
	}
	if status != domain.RiskEventReviewed && status != domain.RiskEventIgnored {
		return domain.RiskEvent{}, domain.ErrInvalidArgument
	}
	event, err := s.repo.FindEventByID(ctx, id)
	if err != nil {
		return domain.RiskEvent{}, err
	}
	now := time.Now().UTC()
	event.Status = status
	event.ReviewedBy = strings.TrimSpace(actorID)
	event.ReviewedAt = &now
	if err := s.repo.UpdateEvent(ctx, &event); err != nil {
		return domain.RiskEvent{}, err
	}
	return event, nil
}

func (s *RiskService) RecordEvent(ctx context.Context, eventType string, userID string, auctionID uint64, severity domain.RiskSeverity, payload interface{}) {
	if s == nil || s.repo == nil {
		return
	}
	if severity == "" {
		severity = domain.RiskSeverityLow
	}
	raw, _ := json.Marshal(payload)
	event := &domain.RiskEvent{
		EventType: strings.TrimSpace(eventType),
		UserID:    strings.TrimSpace(userID),
		AuctionID: auctionID,
		Severity:  severity,
		Payload:   raw,
		Status:    domain.RiskEventPending,
		CreatedAt: time.Now().UTC(),
	}
	if event.EventType == "" {
		event.EventType = "UNKNOWN"
	}
	if err := s.repo.CreateEvent(ctx, event); err == nil {
		broadcastJSON(s.publisher, auctionID, "risk.event", event)
	}
}

func broadcastJSON(publisher riskports.EventPublisher, auctionID uint64, eventType string, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, riskports.EventEnvelope{Type: eventType, Payload: raw})
}
