package ports

import (
	"context"
	"encoding/json"
	"time"

	"aieas_backend/internal/domain"
)

// AuctionRepository 是 auction 用例所需的拍品持久化端口。
type AuctionRepository interface {
	Create(ctx context.Context, auction *domain.AuctionLot) error
	FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error)
	List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error)
	Search(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error)
	Update(ctx context.Context, auction *domain.AuctionLot) error
	Delete(ctx context.Context, id uint64) error
	CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error
}

// BidRepository 是 auction 出价路径所需的出价持久化端口。
type BidRepository interface {
	Create(ctx context.Context, bid *domain.BidRecord) error
	CreateIgnoreBatch(ctx context.Context, records []domain.BidRecord) error
	FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error)
	ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error)
	CountByAuction(ctx context.Context, auctionID uint64) (int, error)
	ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error)
}

// BidRoundRepository 是支持按开拍轮次过滤出价的可选端口。
type BidRoundRepository interface {
	ListByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64, limit int) ([]domain.BidRecord, error)
	CountByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64) (int, error)
}

// DepositRepository 是保证金路径所需持久化端口。
type DepositRepository interface {
	Create(ctx context.Context, deposit *domain.DepositLedger) error
	FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error)
	ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error)
	ListByUser(ctx context.Context, userID string, limit, offset int) ([]domain.DepositLedger, error)
	Update(ctx context.Context, deposit *domain.DepositLedger) error
}

// OrderRepository 是落槌结算路径所需订单持久化端口。
type OrderRepository interface {
	CreateIfAbsentByAuction(ctx context.Context, order *domain.OrderDeal) (domain.OrderDeal, bool, error)
	FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error)
	FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
	ListPayTimeoutCandidates(ctx context.Context, now time.Time, limit int) ([]domain.OrderDeal, error)
	Update(ctx context.Context, order *domain.OrderDeal) error
	UpdateStatusWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.OrderStatus) error
}

// AuctionRealtimeReader 是拍卖实时状态只读端口。
type AuctionRealtimeReader interface {
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
	TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error)
}

// AuctionRealtimeWriter 是拍卖实时状态写入端口。
type AuctionRealtimeWriter interface {
	InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error)
	MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error
	ResetAuctionParticipation(ctx context.Context, auctionID uint64) error
	BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error)
	PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error)
	Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error)
}

// AuctionRealtimeStore 汇总拍卖实时状态读写端口。
type AuctionRealtimeStore interface {
	AuctionRealtimeReader
	AuctionRealtimeWriter
}

// RealtimeReader / RealtimeWriter / RealtimeStore 是 auction 实时端口的过渡期短名。
type RealtimeReader = AuctionRealtimeReader
type RealtimeWriter = AuctionRealtimeWriter
type RealtimeStore = AuctionRealtimeStore

// EventEnvelope 是 auction 模块内部广播事件载体，避免端口依赖 transport/ws。
type EventEnvelope struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId,omitempty"`
	Seq       int64           `json:"seq,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	TS        int64           `json:"ts,omitempty"`
}

// EventPublisher 是拍卖事件广播端口。
type EventPublisher interface {
	Broadcast(auctionID uint64, env EventEnvelope) int
}

// SettlementEventPublisher 是落槌结算事件发布端口。
type SettlementEventPublisher interface {
	PublishAuctionClosed(ctx context.Context, auction domain.AuctionLot, result domain.HammerResult, order *domain.OrderDeal) error
	PublishOrderCreated(ctx context.Context, order domain.OrderDeal) error
}

// TxManager 是 auction 模块事务边界端口。
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// AuctionIDGenerator 是拍品 ID 生成端口。
type AuctionIDGenerator interface {
	NextAuctionID() (uint64, error)
}

// OrderIDGenerator 是订单 ID 生成端口。
type OrderIDGenerator interface {
	NextOrderID() (uint64, error)
}

// ProductAuditInput 是商品内容审核请求。
type ProductAuditInput struct {
	ProductText     string
	ImageName       string
	ContentType     string
	ImageSize       int64
	Image           []byte
	CallbackURL     string
	CallbackHeaders map[string]string
	CallbackContext map[string]interface{}
}

// ProductAuditImage 是商品审核图片内容。
type ProductAuditImage struct {
	ImageName   string
	ContentType string
	ImageSize   int64
	Image       []byte
}

// ProductAuditResult 是商品内容审核结果。
type ProductAuditResult struct {
	Success      bool    `json:"success"`
	IsApproved   bool    `json:"is_approved"`
	RejectReason *string `json:"reject_reason"`
	RequestID    string  `json:"request_id,omitempty"`
	Status       string  `json:"status,omitempty"`
	Message      string  `json:"message,omitempty"`
}

// ProductAuditor 是商品内容审核端口。
type ProductAuditor interface {
	AuditProduct(ctx context.Context, in ProductAuditInput) (ProductAuditResult, error)
}

// ProductAuditImageLoader 是商品审核图片加载端口。
type ProductAuditImageLoader interface {
	LoadProductAuditImage(ctx context.Context, imageURL string) (ProductAuditImage, error)
}

// RiskChecker 是出价风控检查端口。
type RiskChecker interface {
	CheckBidRisk(ctx context.Context, auctionID uint64, bidderID string, price int64) (domain.BidRiskResult, string, error)
}

// RiskUseCase 是 RiskChecker 的过渡期别名。
type RiskUseCase = RiskChecker

// OnlineCounter 是拍卖在线人数读取端口。
type OnlineCounter interface {
	OnlineCount(auctionID uint64) int
}

// BroadcastPublisher 是直播间/拍卖广播能力端口。
type BroadcastPublisher interface {
	Broadcast(auctionID uint64, env EventEnvelope) int
}
