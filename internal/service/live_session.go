package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

var (
	ErrLiveSessionBusy            = errors.New("live session is busy with another auction")
	ErrLotAlreadyMounted          = errors.New("auction already mounted to another live session")
	ErrLiveSessionLotInvalidState = errors.New("auction lot state does not allow this operation")
)

// OnlineCounter 用于在 Stats 接口中读取某 auction 的在线人数。
// 通过定义在 service 包中的最小 interface 解耦 transport 层的 Hub。
type OnlineCounter interface {
	OnlineCount(auctionID uint64) int
}

type AIAssistantSwitchNotifier interface {
	NotifySwitch(ctx context.Context, liveSessionID uint64, merchantID string, enabled bool)
}

// LiveSessionService 编排直播场次（live_session）领域操作：开播/闭播、统计累加、跨域查询。
type LiveSessionService struct {
	sessions        repository.LiveSessionRepository
	auctions        repository.AuctionRepository
	tx              repository.TxManager
	lock            repository.LiveSessionLock
	auction         *AuctionService
	bids            repository.BidRepository
	orders          repository.OrderRepository
	users           repository.UserRepository
	auctionRealtime repository.AuctionRealtimeStore
	hub             OnlineCounter
	sessionRealtime repository.LiveSessionRealtimeStore
	onEnded         func(ctx context.Context, session domain.LiveSession)
	hook            *LiveAgentHookService
	aiSwitch        AIAssistantSwitchNotifier

	mu sync.Mutex // 保护场次开关与闭播计数 flush 的临界区
}

// NewLiveSessionService 构造一个直播场次服务。bids/orders 可选，仅在查询时使用。
func NewLiveSessionService(sessions repository.LiveSessionRepository, auctions repository.AuctionRepository) *LiveSessionService {
	return &LiveSessionService{sessions: sessions, auctions: auctions, tx: repository.NoopTxManager{}, lock: repository.NewMemoryLiveSessionLock()}
}

// SetWriteDeps 注入直播场次主链路写操作所需依赖。
func (s *LiveSessionService) SetWriteDeps(tx repository.TxManager, lock repository.LiveSessionLock, auction *AuctionService) {
	if s == nil {
		return
	}
	if tx != nil {
		s.tx = tx
	}
	if lock != nil {
		s.lock = lock
	}
	s.auction = auction
}

// SetReadDeps 注入用于场次详情查询的额外仓储。
func (s *LiveSessionService) SetReadDeps(bids repository.BidRepository, orders repository.OrderRepository) {
	if s == nil {
		return
	}
	s.bids = bids
	s.orders = orders
}

func (s *LiveSessionService) SetUserRepository(users repository.UserRepository) {
	if s == nil {
		return
	}
	s.users = users
}

// SetRealtimeStore 注入跨实例的实时计数存储。注入后 IncrCounters 不再 RMW MySQL，
// 而是走 Redis HINCRBY/Lua CAS；CloseSession 时通过 FlushCountersToDB 一次性回写。
func (s *LiveSessionService) SetRealtimeStore(rt repository.LiveSessionRealtimeStore) {
	if s == nil {
		return
	}
	s.sessionRealtime = rt
}

// SetStatsDeps 注入 Stats 接口所需依赖。
func (s *LiveSessionService) SetStatsDeps(bids repository.BidRepository, realtime repository.AuctionRealtimeStore, hub OnlineCounter) {
	if s == nil {
		return
	}
	s.bids = bids
	s.auctionRealtime = realtime
	s.hub = hub
}

// SetLiveAgentHookService 注入直播拍卖事件 hook。
func (s *LiveSessionService) SetLiveAgentHookService(hook *LiveAgentHookService) {
	if s == nil {
		return
	}
	s.hook = hook
}

func (s *LiveSessionService) SetAIAssistantSwitchNotifier(notifier AIAssistantSwitchNotifier) {
	if s == nil {
		return
	}
	s.aiSwitch = notifier
}

// SetOnEnded 注入场次闭播完成后的回调（典型用法是 Hub.BroadcastSessionEnd）。
// 回调只会在 CloseSession 真正完成 LIVE -> ENDED 的状态切换后触发；
// 已经处于 ENDED 的幂等返回不会再次触发。回调中 panic 会被忽略，避免影响主路径。
func (s *LiveSessionService) SetOnEnded(fn func(ctx context.Context, session domain.LiveSession)) {
	if s == nil {
		return
	}
	s.onEnded = fn
}

type CreateLiveSessionInput struct {
	ActorID            string
	ActorRole          domain.Role
	MerchantID         string
	Title              string
	Description        string
	CoverURL           string
	Status             domain.LiveSessionStatus
	ScheduledStartTime *time.Time
	PlannedDurationSec int
}

type UpdateLiveSessionInput struct {
	ActorID            string
	ActorRole          domain.Role
	Title              *string
	Description        *string
	CoverURL           *string
	Status             *domain.LiveSessionStatus
	ScheduledStartTime *time.Time
	PlannedDurationSec *int
}

type ActivateLiveSessionAuctionInput struct {
	SessionID   uint64
	AuctionID   uint64
	ActorID     string
	ActorRole   domain.Role
	DurationSec int
}

type LiveSessionStats struct {
	LiveSessionID        uint64 `json:"liveSessionId"`
	Online               int    `json:"online"`
	LotsTotal            int    `json:"lotsTotal"`
	LotsSold             int    `json:"lotsSold"`
	LotsUnsold           int    `json:"lotsUnsold"`
	BidCount             int    `json:"bidCount"`
	GMVCent              int64  `json:"gmvCent"`
	ViewerPeak           int    `json:"viewerPeak"`
	ViewerTotal          int    `json:"viewerTotal"`
	ActiveAuctionID      uint64 `json:"activeAuctionId"`
	CurrentBidCount      int    `json:"currentBidCount"`
	CurrentRemainSeconds int64  `json:"currentRemainSeconds"`
	CurrentPrice         int64  `json:"currentPrice"`
}

func (s *LiveSessionService) Create(ctx context.Context, in CreateLiveSessionInput) (domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	title := strings.TrimSpace(in.Title)
	if title == "" || strings.TrimSpace(in.ActorID) == "" {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	merchantID := strings.TrimSpace(in.MerchantID)
	if in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if merchantID == "" {
		merchantID = in.ActorID
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, merchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	status := in.Status
	if status == "" {
		status = domain.LiveSessionStatusDraft
	}
	if status != domain.LiveSessionStatusDraft && status != domain.LiveSessionStatusScheduled {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if in.PlannedDurationSec < 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	now := time.Now().UTC()
	session := domain.LiveSession{
		MerchantID:         merchantID,
		Title:              title,
		Description:        strings.TrimSpace(in.Description),
		CoverURL:           strings.TrimSpace(in.CoverURL),
		Status:             status,
		ScheduledStartTime: in.ScheduledStartTime,
		PlannedDurationSec: in.PlannedDurationSec,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := s.sessions.Create(ctx, &session); err != nil {
		return domain.LiveSession{}, err
	}
	return session, nil
}

func (s *LiveSessionService) Update(ctx context.Context, id uint64, in UpdateLiveSessionInput) (domain.LiveSession, error) {
	if id == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	current, err := s.sessions.Get(ctx, id)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, current.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	if current.Status == domain.LiveSessionStatusEnded || current.Status == domain.LiveSessionStatusCancelled {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if in.Title != nil {
		title := strings.TrimSpace(*in.Title)
		if title == "" {
			return domain.LiveSession{}, domain.ErrInvalidArgument
		}
		current.Title = title
	}
	if in.Description != nil {
		current.Description = strings.TrimSpace(*in.Description)
	}
	if in.CoverURL != nil {
		current.CoverURL = strings.TrimSpace(*in.CoverURL)
	}
	if in.ScheduledStartTime != nil {
		current.ScheduledStartTime = in.ScheduledStartTime
	}
	if in.PlannedDurationSec != nil {
		if *in.PlannedDurationSec < 0 {
			return domain.LiveSession{}, domain.ErrInvalidArgument
		}
		current.PlannedDurationSec = *in.PlannedDurationSec
	}
	if in.Status != nil {
		if !in.Status.Valid() || !domain.CanTransitionLiveSession(current.Status, *in.Status) {
			return domain.LiveSession{}, domain.ErrInvalidState
		}
		current.Status = *in.Status
	}
	if err := s.sessions.Update(ctx, &current); err != nil {
		return domain.LiveSession{}, err
	}
	return current, nil
}

func (s *LiveSessionService) Start(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	if sessionID == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, current.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	if current.Status == domain.LiveSessionStatusLive {
		return current, nil
	}
	if current.Status != domain.LiveSessionStatusDraft && current.Status != domain.LiveSessionStatusScheduled {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if existing, err := s.sessions.GetActiveByMerchantID(ctx, current.MerchantID); err == nil && existing.ID != current.ID {
		return domain.LiveSession{}, domain.ErrInvalidState
	} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return domain.LiveSession{}, err
	}
	now := time.Now().UTC()
	current.Status = domain.LiveSessionStatusLive
	current.OpenedAt = &now
	current.ClosedAt = nil
	current.ActiveAuctionID = 0
	if err := s.sessions.Update(ctx, &current); err != nil {
		return domain.LiveSession{}, err
	}
	if s.hook != nil {
		s.hook.EmitLiveStarted(ctx, current.MerchantID, current.ID)
	}
	return current, nil
}

func (s *LiveSessionService) End(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	current, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, current.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	return s.CloseSession(ctx, sessionID)
}

// CloseSession 将场次从 LIVE 切换到 ENDED 并写入 closed_at。
// 已经 ENDED 的场次直接返回当前状态（幂等）。
//
// 闭播路径会先调用 FlushCountersToDB 把 realtime store 中累积的计数回写到 MySQL，
// 然后再做状态机切换 + Update。
func (s *LiveSessionService) CloseSession(ctx context.Context, sessionID uint64) (domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if sessionID == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if current.Status == domain.LiveSessionStatusEnded {
		return current, nil
	}
	if !domain.CanTransitionLiveSession(current.Status, domain.LiveSessionStatusEnded) {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	// 闭播前先把 realtime 累积计数回写到 MySQL，避免在 Update 之后再 flush 把 ENDED 行的 status 覆盖回 LIVE。
	if err := s.flushCountersLocked(ctx, sessionID); err != nil {
		return domain.LiveSession{}, err
	}
	// flush 之后重新读，得到包含最新计数的行。
	current, err = s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	now := time.Now().UTC()
	current.Status = domain.LiveSessionStatusEnded
	current.ClosedAt = &now
	activeAuctionID := current.ActiveAuctionID
	current.ActiveAuctionID = 0
	if err := s.sessions.Update(ctx, &current); err != nil {
		return domain.LiveSession{}, err
	}
	if activeAuctionID != 0 && s.lock != nil {
		_ = s.lock.Release(ctx, sessionID, activeAuctionID)
	}
	if err := s.unmountUnfinishedLots(ctx, current.ID, activeAuctionID); err != nil {
		return domain.LiveSession{}, err
	}
	if s.sessionRealtime != nil {
		_ = s.sessionRealtime.Reset(ctx, sessionID)
	}
	if s.onEnded != nil {
		fn := s.onEnded
		ended := current
		go func() {
			defer func() { _ = recover() }()
			fn(context.Background(), ended)
		}()
	}
	return current, nil
}

func (s *LiveSessionService) unmountUnfinishedLots(ctx context.Context, sessionID, activeAuctionID uint64) error {
	if s.auctions == nil {
		return nil
	}
	lots, err := s.auctions.List(ctx, domain.AuctionFilter{LiveSessionID: sessionID, Limit: 100})
	if err != nil {
		return err
	}
	for _, lot := range lots {
		if lot.AuctionID == activeAuctionID || lot.Status == domain.AuctionStatusClosedWon || lot.Status == domain.AuctionStatusSettled {
			continue
		}
		lot.LiveSessionID = nil
		if !lot.Status.Terminal() {
			lot.Status = domain.AuctionStatusReady
		}
		if err := s.auctions.Update(ctx, &lot); err != nil {
			return err
		}
	}
	return nil
}

// IncrCounters 对指定场次累加计数。
//
//   - 注入 RealtimeStore 时：HINCRBY 在 Redis 累积，ViewerPeakAtMin 通过 Lua CAS 取 max；
//     不再触碰 MySQL，避免热点行在拍卖高频路径上互锁。
//   - 未注入 RealtimeStore 时：退化为内存锁内 RMW MySQL 行（兼容单实例 / 测试场景）。
//
// 任意计数为零的字段会跳过；ViewerPeakAtMin 仅在大于当前 viewer_peak 时才覆盖。
func (s *LiveSessionService) IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error {
	if s == nil || s.sessions == nil || sessionID == 0 {
		return nil
	}
	if s.sessionRealtime != nil {
		if err := s.sessionRealtime.IncrCounters(ctx, sessionID, c); err != nil {
			return err
		}
		if c.ViewerPeakAtMin > 0 {
			if _, err := s.sessionRealtime.BumpViewerPeak(ctx, sessionID, c.ViewerPeakAtMin); err != nil {
				return err
			}
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return err
	}
	current.LotsTotal += c.LotsTotalDelta
	current.LotsSold += c.LotsSoldDelta
	current.LotsUnsold += c.LotsUnsoldDelta
	current.BidCount += c.BidCountDelta
	current.GMVCent += c.GMVCentDelta
	current.ViewerTotal += c.ViewerTotalAdd
	if c.ViewerPeakAtMin > current.ViewerPeak {
		current.ViewerPeak = c.ViewerPeakAtMin
	}
	return s.sessions.Update(ctx, &current)
}

// FlushCountersToDB 将 RealtimeStore 中累积的计数一次性回写到 MySQL，并清零 store。
// 未注入 RealtimeStore 时为 no-op。
//
// 调用方需要持有 s.mu（用于保证 flush 与状态机切换的原子性），如果未持有，请使用
// 公共入口 CloseSession 或独立的 FlushCountersToDB 锁版本。
func (s *LiveSessionService) FlushCountersToDB(ctx context.Context, sessionID uint64) error {
	if s == nil || s.sessions == nil || sessionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushCountersLocked(ctx, sessionID)
}

func (s *LiveSessionService) flushCountersLocked(ctx context.Context, sessionID uint64) error {
	if s.sessionRealtime == nil {
		return nil
	}
	counters, peak, err := s.sessionRealtime.LoadCounters(ctx, sessionID)
	if err != nil {
		return err
	}
	if counters.LotsTotalDelta == 0 && counters.LotsSoldDelta == 0 && counters.LotsUnsoldDelta == 0 &&
		counters.BidCountDelta == 0 && counters.GMVCentDelta == 0 && counters.ViewerTotalAdd == 0 && peak == 0 {
		return nil
	}
	current, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return err
	}
	current.LotsTotal += counters.LotsTotalDelta
	current.LotsSold += counters.LotsSoldDelta
	current.LotsUnsold += counters.LotsUnsoldDelta
	current.BidCount += counters.BidCountDelta
	current.GMVCent += counters.GMVCentDelta
	current.ViewerTotal += counters.ViewerTotalAdd
	if peak > current.ViewerPeak {
		current.ViewerPeak = peak
	}
	return s.sessions.Update(ctx, &current)
}

// Get 返回单个场次。
func (s *LiveSessionService) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	if id == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	return s.sessions.Get(ctx, id)
}

// ListByMerchant 列出某商家的所有直播场次。
// 当 actor 为商家时强制 merchantID = actorID；admin 可指定任意 merchantID。
func (s *LiveSessionService) ListByMerchant(ctx context.Context, merchantID string, status domain.LiveSessionStatus, actorID string, actorRole domain.Role, limit, offset int) ([]domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	merchantID = strings.TrimSpace(merchantID)
	if actorRole == domain.RoleMerchant {
		merchantID = actorID
	}
	if merchantID == "" {
		return nil, domain.ErrInvalidArgument
	}
	if !canAccessSellerOwned(actorID, actorRole, merchantID) {
		return nil, domain.ErrForbidden
	}
	filter := domain.LiveSessionFilter{MerchantID: merchantID, Status: status, Limit: limit, Offset: offset}
	return s.sessions.List(ctx, filter)
}

// ListVisible 列出当前 actor 可见的直播场次。
// buyer 只能看到 LIVE 场次；merchant 只能看到自己的场次；admin 可按条件查看全部。
func (s *LiveSessionService) ListVisible(ctx context.Context, merchantID string, status domain.LiveSessionStatus, actorID string, actorRole domain.Role, limit, offset int) ([]domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	merchantID = strings.TrimSpace(merchantID)
	switch actorRole {
	case domain.RoleBuyer:
		if status.Valid() && status != domain.LiveSessionStatusLive {
			return []domain.LiveSession{}, nil
		}
		return s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: merchantID, Status: domain.LiveSessionStatusLive, Limit: limit, Offset: offset})
	case domain.RoleMerchant:
		return s.ListByMerchant(ctx, merchantID, status, actorID, actorRole, limit, offset)
	case domain.RoleAdmin:
		return s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: merchantID, Status: status, Limit: limit, Offset: offset})
	default:
		return nil, domain.ErrForbidden
	}
}

// ListLots 返回某场次内的拍品列表（live_session_id 反查）。
func (s *LiveSessionService) ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	if s == nil || s.sessions == nil || s.auctions == nil {
		return nil, domain.ErrNotFound
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !canReadLiveSession(actorID, actorRole, session) {
		return nil, domain.ErrForbidden
	}
	all, err := s.auctions.List(ctx, domain.AuctionFilter{LiveSessionID: sessionID, Limit: 100})
	if err != nil {
		return nil, err
	}
	return s.enrichLots(ctx, all), nil
}

// ListBids 返回某场次的出价记录（按拍品聚合后 limit）。
func (s *LiveSessionService) ListBids(ctx context.Context, sessionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	return s.ListBidsPaged(ctx, sessionID, "priceDesc", limit, 0, actorID, actorRole)
}

// ListBidsPaged 返回某场次的出价记录，支持按时间或价格排序分页。
func (s *LiveSessionService) ListBidsPaged(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	if s == nil || s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return nil, domain.ErrForbidden
	}
	if s.bids == nil {
		return []domain.BidRecord{}, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	records, err := s.bids.ListByLiveSession(ctx, sessionID, normalizeBidSort(sortBy), limit, offset)
	if err != nil {
		return nil, err
	}
	return s.enrichBidderNicknames(ctx, records), nil
}

func (s *LiveSessionService) ListAuctionBids(ctx context.Context, sessionID, auctionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	if s == nil || s.sessions == nil || s.auctions == nil {
		return nil, domain.ErrNotFound
	}
	if sessionID == 0 || auctionID == 0 {
		return nil, domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return nil, domain.ErrForbidden
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return nil, err
	}
	if auction.LiveSessionID == nil || *auction.LiveSessionID != sessionID {
		return nil, domain.ErrInvalidArgument
	}
	if s.bids == nil {
		return []domain.BidRecord{}, nil
	}
	roundStartTSMS := auction.StartTime.UnixMilli()
	records, err := listAuctionBidRecordsForRound(ctx, s.bids, auctionID, roundStartTSMS, limit)
	if err != nil {
		return nil, err
	}
	return s.enrichBidderNicknames(ctx, records), nil
}

// ListOrders 返回某场次产生的订单。
func (s *LiveSessionService) ListOrders(ctx context.Context, sessionID uint64, limit, offset int, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error) {
	if s == nil || s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return nil, domain.ErrForbidden
	}
	if s.orders == nil {
		return []domain.OrderDeal{}, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	orders, err := s.orders.List(ctx, domain.OrderFilter{SellerID: session.MerchantID, LiveSessionID: sessionID, Limit: limit, Offset: offset})
	if err != nil {
		return nil, err
	}
	return s.enrichOrderWinnerNicknames(ctx, orders), nil
}

func (s *LiveSessionService) MountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	if sessionID == 0 || auctionID == 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	if session.Status == domain.LiveSessionStatusEnded || session.Status == domain.LiveSessionStatusCancelled {
		return domain.AuctionLot{}, domain.ErrInvalidState
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if auction.SellerID != session.MerchantID {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	if auction.LiveSessionID != nil && *auction.LiveSessionID != sessionID {
		return domain.AuctionLot{}, ErrLotAlreadyMounted
	}
	switch auction.Status {
	case domain.AuctionStatusDraft, domain.AuctionStatusPendingAudit, domain.AuctionStatusReady:
		// allowed
	default:
		return domain.AuctionLot{}, fmt.Errorf("%w: %w", ErrLiveSessionLotInvalidState, domain.ErrInvalidState)
	}
	if auction.LiveSessionID != nil && *auction.LiveSessionID == sessionID {
		return auction, nil
	}
	auction.LiveSessionID = &sessionID
	if err := s.auctions.Update(ctx, &auction); err != nil {
		return domain.AuctionLot{}, err
	}
	session.LotsTotal++
	_ = s.sessions.Update(ctx, &session)
	if s.hook != nil {
		s.hook.EmitLotMounted(ctx, session.MerchantID, sessionID, auction.AuctionID)
	}
	return auction, nil
}

func (s *LiveSessionService) UnmountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) error {
	if sessionID == 0 || auctionID == 0 {
		return domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return domain.ErrForbidden
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return err
	}
	if auction.LiveSessionID == nil || *auction.LiveSessionID != sessionID {
		return domain.ErrNotFound
	}
	if session.ActiveAuctionID == auctionID || auction.Status == domain.AuctionStatusRunning || auction.Status == domain.AuctionStatusExtended || auction.Status == domain.AuctionStatusHammerPending {
		return fmt.Errorf("%w: %w", ErrLiveSessionLotInvalidState, domain.ErrInvalidState)
	}
	if auction.Status == domain.AuctionStatusClosedWon || auction.Status == domain.AuctionStatusSettled {
		return fmt.Errorf("%w: %w", ErrLiveSessionLotInvalidState, domain.ErrInvalidState)
	}
	auction.LiveSessionID = nil
	if err := s.auctions.Update(ctx, &auction); err != nil {
		return err
	}
	if s.hook != nil {
		s.hook.EmitLotUnmounted(ctx, session.MerchantID, sessionID, auction.AuctionID)
	}
	return nil
}

func (s *LiveSessionService) ActivateAuctionWithOptions(ctx context.Context, in ActivateLiveSessionAuctionInput) (domain.AuctionLot, error) {
	if in.SessionID == 0 || in.AuctionID == 0 || in.DurationSec < 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, in.SessionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, session.MerchantID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	if session.Status != domain.LiveSessionStatusLive {
		return domain.AuctionLot{}, domain.ErrInvalidState
	}
	if session.ActiveAuctionID != 0 && session.ActiveAuctionID != in.AuctionID {
		return domain.AuctionLot{}, fmt.Errorf("%w: held by auction %d", ErrLiveSessionBusy, session.ActiveAuctionID)
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if auction.SellerID != session.MerchantID {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	if auction.LiveSessionID == nil || *auction.LiveSessionID != in.SessionID {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	switch auction.Status {
	case domain.AuctionStatusClosedWon, domain.AuctionStatusSettled:
		return domain.AuctionLot{}, domain.ErrInvalidState
	case domain.AuctionStatusClosedFailed:
		if err := s.resetAuctionToReady(ctx, &auction); err != nil {
			return domain.AuctionLot{}, err
		}
	}
	now := time.Now().UTC()
	var startTime, endTime time.Time
	if in.DurationSec > 0 {
		startTime = now
		endTime = startTime.Add(time.Duration(in.DurationSec) * time.Second)
		auction.StartTime = startTime
		auction.EndTime = endTime
	} else if auction.DurationSec > 0 && (auction.EndTime.IsZero() || !auction.EndTime.After(now)) {
		startTime = now
		endTime = startTime.Add(time.Duration(auction.DurationSec) * time.Second)
		auction.StartTime = startTime
		auction.EndTime = endTime
	} else if auction.EndTime.IsZero() || !auction.EndTime.After(now) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	} else if auction.StartTime.IsZero() {
		startTime = now
		endTime = auction.EndTime
		auction.StartTime = startTime
	}
	if s.lock != nil {
		acquired, holder, err := s.lock.Acquire(ctx, in.SessionID, in.AuctionID, lockTTLForAuction(auction))
		if err != nil {
			return domain.AuctionLot{}, err
		}
		if !acquired && holder != in.AuctionID {
			return domain.AuctionLot{}, fmt.Errorf("%w: held by auction %d", ErrLiveSessionBusy, holder)
		}
	}
	session.ActiveAuctionID = in.AuctionID
	if err := s.sessions.Update(ctx, &session); err != nil {
		if s.lock != nil {
			_ = s.lock.Release(ctx, in.SessionID, in.AuctionID)
		}
		return domain.AuctionLot{}, err
	}
	if s.auction != nil {
		started, err := s.auction.StartWithTiming(ctx, in.AuctionID, in.ActorID, in.ActorRole, startTime, endTime)
		if err != nil {
			if s.lock != nil {
				_ = s.lock.Release(ctx, in.SessionID, in.AuctionID)
			}
			session.ActiveAuctionID = 0
			_ = s.sessions.Update(ctx, &session)
			return domain.AuctionLot{}, err
		}
		auction = started
	}
	if s.hook != nil {
		durationSec := in.DurationSec
		if durationSec <= 0 && !auction.StartTime.IsZero() && !auction.EndTime.IsZero() && auction.EndTime.After(auction.StartTime) {
			durationSec = int(auction.EndTime.Sub(auction.StartTime).Seconds())
		}
		s.hook.EmitLotStarted(ctx, session.MerchantID, in.SessionID, auction.AuctionID, durationSec)
	}
	return auction, nil
}

func (s *LiveSessionService) DeactivateAuction(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	var cancelledAuctionID uint64
	if session.ActiveAuctionID != 0 {
		auction, err := s.auctions.FindByID(ctx, session.ActiveAuctionID)
		if err != nil {
			return domain.LiveSession{}, err
		}
		if !auction.Status.Terminal() {
			if err := s.resetAuctionToReady(ctx, &auction); err != nil {
				return domain.LiveSession{}, err
			}
			cancelledAuctionID = auction.AuctionID
		}
		if s.lock != nil {
			_ = s.lock.Release(ctx, sessionID, session.ActiveAuctionID)
		}
		session.ActiveAuctionID = 0
	}
	if err := s.sessions.Update(ctx, &session); err != nil {
		return domain.LiveSession{}, err
	}
	if cancelledAuctionID != 0 && s.hook != nil {
		s.hook.EmitLotCancelled(ctx, session.MerchantID, sessionID, cancelledAuctionID)
	}
	return session, nil
}

func (s *LiveSessionService) OnAuctionClosed(ctx context.Context, auctionID uint64) {
	if s == nil || s.sessions == nil || s.auctions == nil {
		return
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil || auction.LiveSessionID == nil {
		return
	}
	sessionID := *auction.LiveSessionID
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return
	}
	if session.ActiveAuctionID == auctionID {
		if s.lock != nil {
			_ = s.lock.Release(ctx, sessionID, auctionID)
		}
		session.ActiveAuctionID = 0
		_ = s.sessions.Update(ctx, &session)
	}
}

func (s *LiveSessionService) ActiveAuctionAndSession(ctx context.Context, sessionID uint64) (uint64, uint64, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return 0, 0, err
	}
	return session.ActiveAuctionID, session.ID, nil
}

func (s *LiveSessionService) Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (LiveSessionStats, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return LiveSessionStats{}, err
	}
	if !canReadLiveSession(actorID, actorRole, session) {
		return LiveSessionStats{}, domain.ErrForbidden
	}
	stats := LiveSessionStats{LiveSessionID: session.ID, LotsTotal: session.LotsTotal, LotsSold: session.LotsSold, LotsUnsold: session.LotsUnsold, BidCount: session.BidCount, GMVCent: session.GMVCent, ViewerPeak: session.ViewerPeak, ViewerTotal: session.ViewerTotal, ActiveAuctionID: session.ActiveAuctionID}
	if stats.LotsTotal == 0 {
		lots, err := s.auctions.List(ctx, domain.AuctionFilter{LiveSessionID: sessionID, Limit: 100})
		if err == nil {
			stats.LotsTotal = len(lots)
		}
	}
	if s.sessionRealtime != nil {
		if counters, peak, err := s.sessionRealtime.LoadCounters(ctx, sessionID); err == nil {
			stats.LotsTotal += counters.LotsTotalDelta
			stats.LotsSold += counters.LotsSoldDelta
			stats.LotsUnsold += counters.LotsUnsoldDelta
			stats.BidCount += counters.BidCountDelta
			stats.GMVCent += counters.GMVCentDelta
			stats.ViewerTotal += counters.ViewerTotalAdd
			if peak > stats.ViewerPeak {
				stats.ViewerPeak = peak
			}
		}
	}
	if session.ActiveAuctionID == 0 {
		return stats, nil
	}
	if s.hub != nil {
		stats.Online = s.hub.OnlineCount(session.ActiveAuctionID)
	}
	var endTime time.Time
	realtimeOK := false
	if s.auctionRealtime != nil {
		if state, ok, err := s.auctionRealtime.GetAuctionState(ctx, session.ActiveAuctionID); err == nil && ok {
			realtimeOK = true
			stats.CurrentPrice = state.CurrentPrice
			stats.CurrentBidCount = state.BidCount
			endTime = state.EndTime
		}
	}
	if s.bids != nil && !realtimeOK {
		if auction, err := s.auctions.FindByID(ctx, session.ActiveAuctionID); err == nil {
			if count, err := countAuctionBidsForRound(ctx, s.bids, session.ActiveAuctionID, auction.StartTime.UnixMilli()); err == nil {
				stats.CurrentBidCount = count
			}
		} else if count, err := s.bids.CountByAuction(ctx, session.ActiveAuctionID); err == nil {
			stats.CurrentBidCount = count
		}
	}
	if endTime.IsZero() {
		if auction, err := s.auctions.FindByID(ctx, session.ActiveAuctionID); err == nil {
			endTime = auction.EndTime
		}
	}
	if !endTime.IsZero() {
		remain := int64(time.Until(endTime).Seconds())
		if remain < 0 {
			remain = 0
		}
		stats.CurrentRemainSeconds = remain
	}
	return stats, nil
}

func (s *LiveSessionService) AgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (LiveAgentHookConfig, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return LiveAgentHookConfig{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return LiveAgentHookConfig{}, domain.ErrForbidden
	}
	if s.hook == nil {
		return LiveAgentHookConfig{}, nil
	}
	return s.hook.GetConfig(ctx, session.MerchantID)
}

func (s *LiveSessionService) UpdateAgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role, enabled bool) (LiveAgentHookConfig, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return LiveAgentHookConfig{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return LiveAgentHookConfig{}, domain.ErrForbidden
	}
	if s.hook == nil {
		return LiveAgentHookConfig{}, domain.ErrInvalidState
	}
	cfg, err := s.hook.SetConfig(ctx, session.MerchantID, actorID, enabled)
	if err != nil {
		return LiveAgentHookConfig{}, err
	}
	s.hook.EmitConfigChanged(ctx, session.MerchantID, session.ID, enabled)
	if s.aiSwitch != nil {
		s.aiSwitch.NotifySwitch(ctx, session.ID, session.MerchantID, enabled)
	}
	return cfg, nil
}

func (s *LiveSessionService) resetAuctionToReady(ctx context.Context, auction *domain.AuctionLot) error {
	switch auction.Status {
	case domain.AuctionStatusReady, domain.AuctionStatusWarmingUp, domain.AuctionStatusRunning, domain.AuctionStatusExtended, domain.AuctionStatusHammerPending, domain.AuctionStatusClosedFailed:
		// allowed
	default:
		return domain.ErrInvalidState
	}
	auction.Status = domain.AuctionStatusReady
	auction.WinnerID = nil
	auction.DealPrice = nil
	auction.ClosedAt = nil
	auction.ClosedBy = ""
	if auction.DurationSec > 0 {
		auction.StartTime = time.Time{}
		auction.EndTime = time.Time{}
	}
	if err := s.auctions.Update(ctx, auction); err != nil {
		return err
	}
	realtime := s.auctionRealtime
	minIncrement := int64(1)
	if s.auction != nil {
		realtime = s.auction.realtime
		minIncrement = s.auction.cfg.MinIncrementCent
		if s.auction.timer != nil {
			s.auction.timer.Stop(auction.AuctionID)
		}
	}
	if realtime != nil {
		if minIncrement <= 0 {
			minIncrement = 1
		}
		if _, err := realtime.InitAuction(ctx, *auction, domain.MinIncrementForPrice(auction.IncrementRule, auction.StartPrice, minIncrement)); err != nil {
			return err
		}
	}
	return nil
}

func normalizeBidSort(sortBy string) string {
	switch sortBy {
	case "timeAsc", "priceDesc":
		return sortBy
	default:
		return "timeDesc"
	}
}

func filterBidRecordsByRoundStart(records []domain.BidRecord, roundStartTSMS int64) []domain.BidRecord {
	if roundStartTSMS <= 0 || len(records) == 0 {
		return records
	}
	out := records[:0]
	for _, record := range records {
		if record.BidTSMS >= roundStartTSMS {
			out = append(out, record)
		}
	}
	return out
}

func listAuctionBidRecordsForRound(ctx context.Context, bids repository.BidRepository, auctionID uint64, roundStartTSMS int64, limit int) ([]domain.BidRecord, error) {
	if bids == nil {
		return []domain.BidRecord{}, nil
	}
	if roundBids, ok := bids.(repository.BidRoundRepository); ok {
		return roundBids.ListByAuctionSince(ctx, auctionID, roundStartTSMS, limit)
	}
	records, err := bids.ListByAuction(ctx, auctionID, limit)
	if err != nil {
		return nil, err
	}
	return filterBidRecordsByRoundStart(records, roundStartTSMS), nil
}

func countAuctionBidsForRound(ctx context.Context, bids repository.BidRepository, auctionID uint64, roundStartTSMS int64) (int, error) {
	if bids == nil {
		return 0, nil
	}
	if roundBids, ok := bids.(repository.BidRoundRepository); ok {
		return roundBids.CountByAuctionSince(ctx, auctionID, roundStartTSMS)
	}
	records, err := listAuctionBidRecordsForRound(ctx, bids, auctionID, roundStartTSMS, 100)
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

func lockTTLForAuction(auction domain.AuctionLot) time.Duration {
	if auction.EndTime.IsZero() {
		return time.Hour
	}
	d := time.Until(auction.EndTime) + 5*time.Minute
	if d <= 0 {
		return time.Hour
	}
	return d
}

func (s *LiveSessionService) enrichBidderNicknames(ctx context.Context, records []domain.BidRecord) []domain.BidRecord {
	if len(records) == 0 || s.users == nil {
		return records
	}
	out := make([]domain.BidRecord, len(records))
	copy(out, records)
	cache := make(map[string]string, len(out))
	for i := range out {
		id := strings.TrimSpace(out[i].BidderID)
		if id == "" {
			continue
		}
		if nickname, ok := cache[id]; ok {
			out[i].BidderNickname = nickname
			continue
		}
		user, err := s.users.FindByID(id)
		if err != nil {
			cache[id] = ""
			continue
		}
		nickname := strings.TrimSpace(user.Nickname)
		cache[id] = nickname
		out[i].BidderNickname = nickname
	}
	return out
}

func (s *LiveSessionService) enrichOrderWinnerNicknames(ctx context.Context, orders []domain.OrderDeal) []domain.OrderDeal {
	if len(orders) == 0 || s.users == nil {
		return orders
	}
	out := make([]domain.OrderDeal, len(orders))
	copy(out, orders)
	cache := make(map[string]string, len(out))
	for i := range out {
		winnerID := strings.TrimSpace(out[i].WinnerID)
		if winnerID == "" {
			continue
		}
		if nickname, ok := cache[winnerID]; ok {
			out[i].WinnerNickname = nickname
			continue
		}
		user, err := s.users.FindByID(winnerID)
		if err != nil {
			cache[winnerID] = ""
			continue
		}
		nickname := strings.TrimSpace(user.Nickname)
		cache[winnerID] = nickname
		out[i].WinnerNickname = nickname
	}
	return out
}

func (s *LiveSessionService) enrichLots(ctx context.Context, lots []domain.AuctionLot) []domain.AuctionLot {
	if len(lots) == 0 || s.bids == nil {
		return lots
	}
	out := make([]domain.AuctionLot, len(lots))
	for i := range lots {
		out[i] = lots[i]
		roundStartTSMS := lots[i].StartTime.UnixMilli()
		if count, err := countAuctionBidsForRound(ctx, s.bids, lots[i].AuctionID, roundStartTSMS); err == nil {
			out[i].BidCount = count
		}
		if records, err := listAuctionBidRecordsForRound(ctx, s.bids, lots[i].AuctionID, roundStartTSMS, 1); err == nil && len(records) > 0 {
			out[i].CurrentPrice = records[0].BidPrice
			out[i].LeaderBidderID = records[0].BidderID
		}
		if out[i].CurrentPrice == 0 && out[i].DealPrice != nil {
			out[i].CurrentPrice = *out[i].DealPrice
		}
	}
	return out
}
