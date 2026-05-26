package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

// ErrLiveRoomBusy 表示直播间已存在另一拍品在拍。
var ErrLiveRoomBusy = errors.New("live room is busy with another auction")

// ErrLotAlreadyMounted 表示拍品已挂入其他直播间，不能再挂到当前房间。
var ErrLotAlreadyMounted = errors.New("auction already mounted to another live room")

// ErrLiveRoomAlreadyExists 表示该商家已经拥有一个直播间（merchant ↔ live_room 1:1）。
var ErrLiveRoomAlreadyExists = errors.New("merchant already has a live room")

// OnlineCounter 用于在 Stats 接口中读取某 auction 的在线人数。
// 通过定义在 service 包中的最小 interface 解耦 transport 层的 Hub。
type OnlineCounter interface {
	OnlineCount(auctionID uint64) int
}

// LiveRoomService 编排直播间领域操作，并通过 LiveRoomLock 保证同一时刻只有一个拍品在拍。
type LiveRoomService struct {
	rooms    repository.LiveRoomRepository
	auctions repository.AuctionRepository
	tx       repository.TxManager
	lock     repository.LiveRoomLock
	auction  *AuctionService
	sessions *LiveSessionService
	hammer   *HammerService

	// 以下为 Stats 接口可选依赖，通过 SetStatsDeps 注入。
	bids     repository.BidRepository
	realtime repository.AuctionRealtimeStore
	hub      OnlineCounter
}

func NewLiveRoomService(rooms repository.LiveRoomRepository, auctions repository.AuctionRepository, tx repository.TxManager, lock repository.LiveRoomLock) *LiveRoomService {
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	if lock == nil {
		lock = repository.NewMemoryLiveRoomLock()
	}
	return &LiveRoomService{rooms: rooms, auctions: auctions, tx: tx, lock: lock}
}

// SetAuctionService 注入 AuctionService 以便 Activate 时启动拍卖。
func (s *LiveRoomService) SetAuctionService(auction *AuctionService) {
	s.auction = auction
}

// SetLiveSessionService 注入直播场次服务，用于 Activate/Deactivate 时打开/关闭场次并回填 lot.live_session_id。
func (s *LiveRoomService) SetLiveSessionService(sessions *LiveSessionService) {
	s.sessions = sessions
}

// SetHammerService 注入结拍服务，用于 Deactivate 时收尾房间内仍在拍的拍品（强制 Force=true）。
// 未注入时退化为旧行为：仅释放房间锁、不动 auction 状态。
func (s *LiveRoomService) SetHammerService(hammer *HammerService) {
	s.hammer = hammer
}

// SetStatsDeps 注入 Stats 接口所需依赖（可选）。
// 任意参数为 nil 时表示对应数据源不可用：
//   - bids == nil 时 currentBidCount 始终为 0
//   - realtime == nil 时 currentPrice/currentRemainSeconds 仅依据 auction 持久化字段
//   - hub == nil 时 online 始终为 0
func (s *LiveRoomService) SetStatsDeps(bids repository.BidRepository, realtime repository.AuctionRealtimeStore, hub OnlineCounter) {
	s.bids = bids
	s.realtime = realtime
	s.hub = hub
}

// CreateLiveRoomInput 描述创建直播间的请求。
type CreateLiveRoomInput struct {
	ActorID     string
	ActorRole   domain.Role
	MerchantID  string
	Title       string
	Description string
	CoverURL    string
	Status      domain.LiveRoomStatus
}

// UpdateLiveRoomInput 描述更新直播间的请求（部分字段）。
type UpdateLiveRoomInput struct {
	ActorID     string
	ActorRole   domain.Role
	Title       *string
	Description *string
	CoverURL    *string
	Status      *domain.LiveRoomStatus
}

type ActivateAuctionInput struct {
	RoomID      uint64
	AuctionID   uint64
	ActorID     string
	ActorRole   domain.Role
	DurationSec int
}

func (s *LiveRoomService) Create(ctx context.Context, in CreateLiveRoomInput) (domain.LiveRoom, error) {
	title := strings.TrimSpace(in.Title)
	if title == "" || strings.TrimSpace(in.ActorID) == "" {
		return domain.LiveRoom{}, domain.ErrInvalidArgument
	}
	merchantID := strings.TrimSpace(in.MerchantID)
	if in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if merchantID == "" {
		merchantID = in.ActorID
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, merchantID) {
		return domain.LiveRoom{}, domain.ErrForbidden
	}
	status := in.Status
	if status == "" {
		status = domain.LiveRoomStatusOffline
	}
	if !status.Valid() {
		return domain.LiveRoom{}, domain.ErrInvalidArgument
	}
	// 一个商家最多只能拥有一个直播间。
	if _, err := s.rooms.FindByMerchantID(ctx, merchantID); err == nil {
		return domain.LiveRoom{}, ErrLiveRoomAlreadyExists
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.LiveRoom{}, err
	}
	room := domain.LiveRoom{
		MerchantID:  merchantID,
		Title:       title,
		Description: strings.TrimSpace(in.Description),
		CoverURL:    strings.TrimSpace(in.CoverURL),
		Status:      status,
	}
	if err := s.rooms.Create(ctx, &room); err != nil {
		return domain.LiveRoom{}, err
	}
	return room, nil
}

func (s *LiveRoomService) Get(ctx context.Context, id uint64) (domain.LiveRoom, error) {
	return s.rooms.FindByID(ctx, id)
}

func (s *LiveRoomService) List(ctx context.Context, filter domain.LiveRoomFilter, actorID string, actorRole domain.Role) ([]domain.LiveRoom, error) {
	switch actorRole {
	case domain.RoleBuyer:
		filter.Status = domain.LiveRoomStatusLive
	case domain.RoleMerchant:
		filter.MerchantID = actorID
	case domain.RoleAdmin:
	default:
		return nil, domain.ErrForbidden
	}
	return s.rooms.List(ctx, filter)
}

func (s *LiveRoomService) Update(ctx context.Context, id uint64, in UpdateLiveRoomInput) (domain.LiveRoom, error) {
	var updated domain.LiveRoom
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.rooms.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if !canAccessSellerOwned(in.ActorID, in.ActorRole, current.MerchantID) {
			return domain.ErrForbidden
		}
		if in.Title != nil {
			title := strings.TrimSpace(*in.Title)
			if title == "" {
				return domain.ErrInvalidArgument
			}
			current.Title = title
		}
		if in.Description != nil {
			current.Description = strings.TrimSpace(*in.Description)
		}
		if in.CoverURL != nil {
			current.CoverURL = strings.TrimSpace(*in.CoverURL)
		}
		if in.Status != nil {
			if !in.Status.Valid() || !domain.CanTransitionLiveRoom(current.Status, *in.Status) {
				return domain.ErrInvalidState
			}
			current.Status = *in.Status
		}
		if err := s.rooms.Update(txCtx, &current); err != nil {
			return err
		}
		updated = current
		return nil
	}); err != nil {
		return domain.LiveRoom{}, err
	}
	return updated, nil
}

func (s *LiveRoomService) Delete(ctx context.Context, id uint64, actorID string, actorRole domain.Role) error {
	return s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		room, err := s.rooms.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
			return domain.ErrForbidden
		}
		if room.ActiveAuctionID != 0 {
			return domain.ErrInvalidState
		}
		return s.rooms.Delete(txCtx, id)
	})
}

// ListLots 返回直播间内挂载的拍品列表。
func (s *LiveRoomService) ListLots(ctx context.Context, roomID uint64) ([]domain.AuctionLot, error) {
	if _, err := s.rooms.FindByID(ctx, roomID); err != nil {
		return nil, err
	}
	return s.auctions.List(ctx, domain.AuctionFilter{LiveRoomID: roomID, Limit: 100})
}

// ActivateAuction 在房间内启动指定拍品；保证同一房间同时只有一个拍品在拍。
func (s *LiveRoomService) ActivateAuction(ctx context.Context, roomID uint64, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	return s.ActivateAuctionWithOptions(ctx, ActivateAuctionInput{RoomID: roomID, AuctionID: auctionID, ActorID: actorID, ActorRole: actorRole})
}

// ActivateAuctionWithOptions 在房间内启动指定拍品，可按前端传入的 durationSec 重置讲解时长。
func (s *LiveRoomService) ActivateAuctionWithOptions(ctx context.Context, in ActivateAuctionInput) (domain.AuctionLot, error) {
	if in.DurationSec < 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	room, err := s.rooms.FindByID(ctx, in.RoomID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, room.MerchantID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if auction.LiveRoomID != in.RoomID {
		return domain.AuctionLot{}, fmt.Errorf("%w: auction %d not in live room %d", domain.ErrInvalidArgument, in.AuctionID, in.RoomID)
	}
	if auction.Status.Terminal() {
		return domain.AuctionLot{}, domain.ErrInvalidState
	}
	var startTime, endTime time.Time
	if in.DurationSec > 0 {
		startTime = time.Now().UTC()
		endTime = startTime.Add(time.Duration(in.DurationSec) * time.Second)
		auction.StartTime = startTime
		auction.EndTime = endTime
	}
	ttl := lockTTLForAuction(auction)
	acquired, holder, err := s.lock.Acquire(ctx, in.RoomID, in.AuctionID, ttl)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !acquired {
		return domain.AuctionLot{}, fmt.Errorf("%w: held by auction %d", ErrLiveRoomBusy, holder)
	}
	// 写持久化字段；只要 active 不一致就更新。
	if room.ActiveAuctionID != in.AuctionID || room.Status != domain.LiveRoomStatusLive {
		room.ActiveAuctionID = in.AuctionID
		room.Status = domain.LiveRoomStatusLive
		if err := s.rooms.Update(ctx, &room); err != nil {
			_ = s.lock.Release(ctx, in.RoomID, in.AuctionID)
			return domain.AuctionLot{}, err
		}
	}
	// 直播场次：本拍品挂入直播间后，所属场次为当前 LIVE 场次（不存在则开新一场）。
	// 仅当 lot 之前未绑定 session（live_session_id IS NULL）时才回填，避免覆盖既有归属。
	if s.sessions != nil {
		session, err := s.sessions.OpenSession(ctx, in.RoomID, room.MerchantID, room.Title)
		if err != nil {
			_ = s.lock.Release(ctx, in.RoomID, in.AuctionID)
			return domain.AuctionLot{}, err
		}
		if auction.LiveSessionID == nil {
			sessionID := session.ID
			auction.LiveSessionID = &sessionID
			if err := s.auctions.Update(ctx, &auction); err != nil {
				_ = s.lock.Release(ctx, in.RoomID, in.AuctionID)
				return domain.AuctionLot{}, err
			}
			_ = s.sessions.IncrCounters(ctx, session.ID, domain.LiveSessionCounters{LotsTotalDelta: 1})
		}
	}
	if s.auction != nil {
		started, err := s.auction.StartWithTiming(ctx, in.AuctionID, in.ActorID, in.ActorRole, startTime, endTime)
		if err != nil {
			_ = s.lock.Release(ctx, in.RoomID, in.AuctionID)
			room.ActiveAuctionID = 0
			_ = s.rooms.Update(ctx, &room)
			return domain.AuctionLot{}, err
		}
		auction = started
	}
	return auction, nil
}

// DeactivateAuction 关闭房间当前在拍的拍品并释放房间锁。
//
// 调用顺序（先收尾 lot 再释放锁，保证押金/订单/state 全部走 HammerService 的 canonical 路径）：
//  1. 若当前 ActiveAuctionID 指向一个非 Terminal 拍品，使用 HammerService.Hammer 以 Force=true 收尾它。
//     Hammer 内部会广播 auction.closed、释放押金、调用 OnAuctionClosed（释放房间锁、清 ActiveAuctionID）。
//  2. 重新读 room；若锁与 ActiveAuctionID 在第 1 步未被清掉（如 hammer 未注入或 lot 已是终态），
//     再做一次保护性 release + Update。
//  3. 关闭当前 LIVE 场次（CloseSession 内部会先把 realtime 计数 flush 到 MySQL）。
func (s *LiveRoomService) DeactivateAuction(ctx context.Context, roomID uint64, actorID string, actorRole domain.Role) (domain.LiveRoom, error) {
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return domain.LiveRoom{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
		return domain.LiveRoom{}, domain.ErrForbidden
	}
	// Step 1: 若 active auction 仍在非终态，走 HammerService 的 canonical 收尾路径。
	if room.ActiveAuctionID != 0 && s.hammer != nil {
		if auction, err := s.auctions.FindByID(ctx, room.ActiveAuctionID); err == nil && !auction.Status.Terminal() {
			_, _, _ = s.hammer.Hammer(ctx, domain.HammerInput{
				RequestID: fmt.Sprintf("deactivate-%d-%d", roomID, time.Now().UTC().UnixNano()),
				AuctionID: auction.AuctionID,
				ActorID:   actorID,
				ActorRole: actorRole,
				ClosedBy:  actorID,
				Force:     true,
				Now:       time.Now().UTC(),
			})
		}
	}
	// Step 2: 重新读 room，取 hammer 路径里 OnAuctionClosed 已应用的最新状态。
	room, err = s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return domain.LiveRoom{}, err
	}
	if room.ActiveAuctionID != 0 {
		_ = s.lock.Release(ctx, roomID, room.ActiveAuctionID)
		room.ActiveAuctionID = 0
	}
	if err := s.rooms.Update(ctx, &room); err != nil {
		return domain.LiveRoom{}, err
	}
	// Step 3: 关闭当前 LIVE 场次（若存在）。CloseSession 会先 flush 计数再切状态。
	if s.sessions != nil {
		_, _, _ = s.sessions.CloseActiveByRoom(ctx, roomID)
	}
	return room, nil
}

// OnAuctionClosed 由 HammerService 终态回调，用于自动释放房间锁。
// 不返回错误以免影响主结拍流程。
func (s *LiveRoomService) OnAuctionClosed(ctx context.Context, auctionID uint64) {
	if s == nil {
		return
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil || auction.LiveRoomID == 0 {
		return
	}
	roomID := auction.LiveRoomID
	_ = s.lock.Release(ctx, roomID, auctionID)
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return
	}
	if room.ActiveAuctionID == auctionID {
		room.ActiveAuctionID = 0
		_ = s.rooms.Update(ctx, &room)
	}
}

// ActiveAuctionID 返回直播间当前正在拍卖的 auction ID（0 表示无）。
func (s *LiveRoomService) ActiveAuctionID(ctx context.Context, roomID uint64) (uint64, error) {
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return 0, err
	}
	return room.ActiveAuctionID, nil
}

// ActiveAuctionAndSession 返回直播间当前正在拍卖的 auction ID 以及该拍品所属的 liveSessionID。
// 当房间无 active auction 时返回 (0, 0, nil)；找不到 auction 不视为致命错误，liveSessionID 退化为 0。
//
// WS 入口用它一次性拿到订阅所需的双键，避免一次连接内多次回查 auction。
func (s *LiveRoomService) ActiveAuctionAndSession(ctx context.Context, roomID uint64) (uint64, uint64, error) {
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return 0, 0, err
	}
	if room.ActiveAuctionID == 0 {
		return 0, 0, nil
	}
	auction, err := s.auctions.FindByID(ctx, room.ActiveAuctionID)
	if err != nil {
		return room.ActiveAuctionID, 0, nil
	}
	var sessionID uint64
	if auction.LiveSessionID != nil {
		sessionID = *auction.LiveSessionID
	}
	return room.ActiveAuctionID, sessionID, nil
}

// LiveRoomStats 描述直播间的实时统计指标。
type LiveRoomStats struct {
	RoomID               uint64 `json:"roomId"`
	Online               int    `json:"online"`
	LotsTotal            int    `json:"lotsTotal"`
	ActiveAuctionID      uint64 `json:"activeAuctionId"`
	CurrentBidCount      int    `json:"currentBidCount"`
	CurrentRemainSeconds int64  `json:"currentRemainSeconds"`
	CurrentPrice         int64  `json:"currentPrice"`
}

// MountAuction 将一个拍品挂载到直播间下。
//
// 校验顺序：直播间存在 -> 操作者权限 -> 拍品存在 -> 拍品 sellerId 与房间 merchant 一致 ->
// 拍品未挂入其他房间（同房间幂等）-> 拍品状态非 Terminal/非 Running。
func (s *LiveRoomService) MountAuction(ctx context.Context, roomID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	if auctionID == 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if auction.SellerID != room.MerchantID {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	// 已挂入其他直播间则冲突；同房间重复挂载视为幂等成功。
	if auction.LiveRoomID != 0 && auction.LiveRoomID != roomID {
		return domain.AuctionLot{}, ErrLotAlreadyMounted
	}
	// 仅允许挂入未进入 WARMING_UP/RUNNING 等运行态、且非 Terminal 的拍品。
	switch auction.Status {
	case domain.AuctionStatusDraft, domain.AuctionStatusPendingAudit, domain.AuctionStatusReady:
		// allowed
	default:
		return domain.AuctionLot{}, domain.ErrInvalidState
	}
	if auction.LiveRoomID == roomID {
		// 幂等返回，无需再次写库。
		return auction, nil
	}
	auction.LiveRoomID = roomID
	if err := s.auctions.Update(ctx, &auction); err != nil {
		return domain.AuctionLot{}, err
	}
	return auction, nil
}

// UnmountAuction 将拍品从直播间移除。当前正在拍的拍品不允许被移除，需先 deactivate。
func (s *LiveRoomService) UnmountAuction(ctx context.Context, roomID, auctionID uint64, actorID string, actorRole domain.Role) error {
	if auctionID == 0 {
		return domain.ErrInvalidArgument
	}
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return err
	}
	if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
		return domain.ErrForbidden
	}
	auction, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return err
	}
	// 拍品不属于该房间则视为不存在。
	if auction.LiveRoomID != roomID {
		return domain.ErrNotFound
	}
	if room.ActiveAuctionID == auctionID {
		return domain.ErrInvalidState
	}
	auction.LiveRoomID = 0
	if err := s.auctions.Update(ctx, &auction); err != nil {
		return err
	}
	return nil
}

// Stats 返回直播间的实时统计信息。
//
// online 按 active auction 统计（直播间 WebSocket 已路由到 active auction），
// 当 ActiveAuctionID == 0 时三个 current* 字段与 online 全部为 0。
func (s *LiveRoomService) Stats(ctx context.Context, roomID uint64) (LiveRoomStats, error) {
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return LiveRoomStats{}, err
	}
	stats := LiveRoomStats{
		RoomID:          room.ID,
		ActiveAuctionID: room.ActiveAuctionID,
	}
	// lotsTotal：沿用现有 List 接口取长度，limit 取仓储允许的最大值（100）。
	lots, err := s.auctions.List(ctx, domain.AuctionFilter{LiveRoomID: roomID, Limit: 100})
	if err != nil {
		return LiveRoomStats{}, err
	}
	stats.LotsTotal = len(lots)
	if room.ActiveAuctionID == 0 {
		return stats, nil
	}
	if s.hub != nil {
		if c := s.hub.OnlineCount(room.ActiveAuctionID); c > 0 {
			stats.Online = c
		}
	}
	if s.bids != nil {
		// 沿用 ListByAuction 长度；limit 取较大值。
		records, err := s.bids.ListByAuction(ctx, room.ActiveAuctionID, 100)
		if err == nil {
			stats.CurrentBidCount = len(records)
		}
	}
	// currentPrice / currentRemainSeconds：优先 realtime store，失败回落到 auction 持久化字段。
	var endTime time.Time
	if s.realtime != nil {
		state, ok, err := s.realtime.GetAuctionState(ctx, room.ActiveAuctionID)
		if err == nil && ok {
			stats.CurrentPrice = state.CurrentPrice
			endTime = state.EndTime
		}
	}
	if endTime.IsZero() {
		auction, err := s.auctions.FindByID(ctx, room.ActiveAuctionID)
		if err == nil {
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
