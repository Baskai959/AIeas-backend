package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

// BlacklistCache 是黑名单查询的缓存层抽象（L1+L2+singleflight）。nil 安全：
// RiskService 在 cache 为空时直接走 repo。
type BlacklistCache interface {
	GetOrLoad(ctx context.Context, userID string, loader func(ctx context.Context) (bool, bool, error)) (bool, error)
	Invalidate(ctx context.Context, userIDs ...string) error
}

type RiskService struct {
	repo      repository.RiskRepository
	publisher EventPublisher
	cache     BlacklistCache
}

func NewRiskService(repo repository.RiskRepository, realtime repository.AuctionRealtimeStore, publisher EventPublisher) *RiskService {
	// v2 起 RiskService 不再持有 realtime；保留入参签名以兼容现有 wiring（server.go / 测试）。
	// 黑名单的真源是 MySQL（repo），叠加可选 LayeredCache。
	_ = realtime
	return &RiskService{repo: repo, publisher: publisher}
}

// SetBlacklistCache 注入黑名单缓存层；nil 时回退到直接读 repo。
func (s *RiskService) SetBlacklistCache(cache BlacklistCache) {
	if s == nil {
		return
	}
	s.cache = cache
}

// IsBlacklisted 仅以 MySQL 为唯一真源。失败时采用 fail-open（视为非黑名单）：
// 黑名单是否决性约束，但读不到时允许出价能避免短暂的 MySQL 抖动直接打挂出价路径。
// 真正的拒绝由后续 Lua 脚本中的入场/押金等门面共同保证业务正确性。
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
			// found=true 时把布尔值缓存到 L1+L2；found=false 触发负缓存（视为 false）。
			return hit, hit, nil
		})
		if err != nil {
			// 默认放行：缓存层 / repo 故障时不阻断业务。上层 BidService 决定如何上报。
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
