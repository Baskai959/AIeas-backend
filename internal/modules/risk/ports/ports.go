package ports

import (
	"context"
	"encoding/json"
	"time"

	"aieas_backend/internal/domain"
)

// RiskRepository 是风控模块所需的黑名单/风险事件持久化端口。
type RiskRepository interface {
	IsBlacklisted(ctx context.Context, userID string, now time.Time) (bool, error)
	CreateBlacklist(ctx context.Context, item *domain.Blacklist) error
	DeleteBlacklist(ctx context.Context, userID string) error
	ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error)
	CreateEvent(ctx context.Context, event *domain.RiskEvent) error
	ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
	UpdateEvent(ctx context.Context, event *domain.RiskEvent) error
	FindEventByID(ctx context.Context, id uint64) (domain.RiskEvent, error)
}

// BlacklistCache 是风控模块黑名单缓存端口。
type BlacklistCache interface {
	GetOrLoad(ctx context.Context, userID string, loader func(ctx context.Context) (bool, bool, error)) (bool, error)
	Invalidate(ctx context.Context, userIDs ...string) error
}

// EventEnvelope 是风险事件广播载体，避免端口依赖 transport/ws。
type EventEnvelope struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	TS        int64           `json:"ts,omitempty"`
}

// EventPublisher 是风险事件广播端口。
type EventPublisher interface {
	Broadcast(auctionID uint64, env EventEnvelope) int
}
