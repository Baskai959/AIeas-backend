package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

// LiveSessionService 编排直播场次（live_session）领域操作：开播/闭播、统计累加、跨域查询。
type LiveSessionService struct {
	sessions repository.LiveSessionRepository
	rooms    repository.LiveRoomRepository
	auctions repository.AuctionRepository
	bids     repository.BidRepository
	orders   repository.OrderRepository
	realtime repository.LiveSessionRealtimeStore
	onEnded  func(ctx context.Context, session domain.LiveSession)

	mu sync.Mutex // 保护场次开关与闭播计数 flush 的临界区
}

// NewLiveSessionService 构造一个直播场次服务。bids/orders 可选，仅在查询时使用。
func NewLiveSessionService(sessions repository.LiveSessionRepository, rooms repository.LiveRoomRepository, auctions repository.AuctionRepository) *LiveSessionService {
	return &LiveSessionService{sessions: sessions, rooms: rooms, auctions: auctions}
}

// SetReadDeps 注入用于场次详情查询的额外仓储。
func (s *LiveSessionService) SetReadDeps(bids repository.BidRepository, orders repository.OrderRepository) {
	if s == nil {
		return
	}
	s.bids = bids
	s.orders = orders
}

// SetRealtimeStore 注入跨实例的实时计数存储。注入后 IncrCounters 不再 RMW MySQL，
// 而是走 Redis HINCRBY/Lua CAS；CloseSession 时通过 FlushCountersToDB 一次性回写。
func (s *LiveSessionService) SetRealtimeStore(rt repository.LiveSessionRealtimeStore) {
	if s == nil {
		return
	}
	s.realtime = rt
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

// OpenSession 在直播间下开启一个 LIVE 场次：
//   - 同一 room 下若已存在 LIVE 场次（GetActiveByRoomID）则直接返回它，保证幂等。
//   - 否则插入 status=LIVE, opened_at=now 的新行，所有计数置零。
func (s *LiveSessionService) OpenSession(ctx context.Context, roomID uint64, merchantID, title string) (domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if roomID == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.sessions.GetActiveByRoomID(ctx, roomID); err == nil {
		return existing, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.LiveSession{}, err
	}
	now := time.Now().UTC()
	session := domain.LiveSession{
		LiveRoomID: roomID,
		MerchantID: strings.TrimSpace(merchantID),
		Title:      strings.TrimSpace(title),
		Status:     domain.LiveSessionStatusLive,
		OpenedAt:   now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.sessions.Create(ctx, &session); err != nil {
		return domain.LiveSession{}, err
	}
	return session, nil
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
	if err := s.sessions.Update(ctx, &current); err != nil {
		return domain.LiveSession{}, err
	}
	if s.realtime != nil {
		_ = s.realtime.Reset(ctx, sessionID)
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

// CloseActiveByRoom 关闭某直播间下当前的 LIVE 场次（若存在）。无活跃场次时返回 nil。
func (s *LiveSessionService) CloseActiveByRoom(ctx context.Context, roomID uint64) (domain.LiveSession, bool, error) {
	if s == nil || s.sessions == nil {
		return domain.LiveSession{}, false, nil
	}
	current, err := s.sessions.GetActiveByRoomID(ctx, roomID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return domain.LiveSession{}, false, nil
		}
		return domain.LiveSession{}, false, err
	}
	closed, err := s.CloseSession(ctx, current.ID)
	if err != nil {
		return domain.LiveSession{}, false, err
	}
	return closed, true, nil
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
	if s.realtime != nil {
		if err := s.realtime.IncrCounters(ctx, sessionID, c); err != nil {
			return err
		}
		if c.ViewerPeakAtMin > 0 {
			if _, err := s.realtime.BumpViewerPeak(ctx, sessionID, c.ViewerPeakAtMin); err != nil {
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
	if s.realtime == nil {
		return nil
	}
	counters, peak, err := s.realtime.LoadCounters(ctx, sessionID)
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

// ListByRoom 列出指定直播间的场次（最近优先）。
// 当调用方为商家角色时，需要校验 room 归属；admin 直接放行。
func (s *LiveSessionService) ListByRoom(ctx context.Context, roomID uint64, status domain.LiveSessionStatus, actorID string, actorRole domain.Role, limit, offset int) ([]domain.LiveSession, error) {
	if s == nil || s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	if roomID == 0 {
		return nil, domain.ErrInvalidArgument
	}
	if s.rooms != nil {
		room, err := s.rooms.FindByID(ctx, roomID)
		if err != nil {
			return nil, err
		}
		if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
			return nil, domain.ErrForbidden
		}
	}
	filter := domain.LiveSessionFilter{LiveRoomID: roomID, Status: status, Limit: limit, Offset: offset}
	return s.sessions.List(ctx, filter)
}

// ListByMerchant 列出某商家所有直播间下的场次。
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

// ListLots 返回某场次内的拍品列表（live_session_id 反查）。
func (s *LiveSessionService) ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	if s == nil || s.sessions == nil || s.auctions == nil {
		return nil, domain.ErrNotFound
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return nil, domain.ErrForbidden
	}
	all, err := s.auctions.List(ctx, domain.AuctionFilter{LiveRoomID: session.LiveRoomID, Limit: 100})
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.AuctionLot, 0, len(all))
	for _, lot := range all {
		if lot.LiveSessionID != nil && *lot.LiveSessionID == sessionID {
			filtered = append(filtered, lot)
		}
	}
	return filtered, nil
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
	return s.bids.ListByLiveSession(ctx, sessionID, normalizeBidSort(sortBy), limit, offset)
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
	return s.orders.List(ctx, domain.OrderFilter{SellerID: session.MerchantID, LiveSessionID: sessionID, Limit: limit, Offset: offset})
}

func normalizeBidSort(sortBy string) string {
	switch sortBy {
	case "timeAsc", "priceDesc":
		return sortBy
	default:
		return "timeDesc"
	}
}
