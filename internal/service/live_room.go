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
	if actorRole == domain.RoleMerchant {
		filter.MerchantID = actorID
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
	if auction.LiveRoomID != roomID {
		return domain.AuctionLot{}, fmt.Errorf("%w: auction %d not in live room %d", domain.ErrInvalidArgument, auctionID, roomID)
	}
	if auction.Status.Terminal() {
		return domain.AuctionLot{}, domain.ErrInvalidState
	}
	ttl := lockTTLForAuction(auction)
	acquired, holder, err := s.lock.Acquire(ctx, roomID, auctionID, ttl)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !acquired {
		return domain.AuctionLot{}, fmt.Errorf("%w: held by auction %d", ErrLiveRoomBusy, holder)
	}
	// 写持久化字段；只要 active 不一致就更新。
	if room.ActiveAuctionID != auctionID || room.Status != domain.LiveRoomStatusLive {
		room.ActiveAuctionID = auctionID
		room.Status = domain.LiveRoomStatusLive
		if err := s.rooms.Update(ctx, &room); err != nil {
			_ = s.lock.Release(ctx, roomID, auctionID)
			return domain.AuctionLot{}, err
		}
	}
	if s.auction != nil {
		started, err := s.auction.Start(ctx, auctionID, actorID, actorRole)
		if err != nil {
			_ = s.lock.Release(ctx, roomID, auctionID)
			room.ActiveAuctionID = 0
			_ = s.rooms.Update(ctx, &room)
			return domain.AuctionLot{}, err
		}
		auction = started
	}
	return auction, nil
}

// DeactivateAuction 释放房间锁；可由商家/管理员主动调用，也可在 OnAuctionClosed 钩子中触发。
func (s *LiveRoomService) DeactivateAuction(ctx context.Context, roomID uint64, actorID string, actorRole domain.Role) (domain.LiveRoom, error) {
	room, err := s.rooms.FindByID(ctx, roomID)
	if err != nil {
		return domain.LiveRoom{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, room.MerchantID) {
		return domain.LiveRoom{}, domain.ErrForbidden
	}
	if room.ActiveAuctionID != 0 {
		_ = s.lock.Release(ctx, roomID, room.ActiveAuctionID)
		room.ActiveAuctionID = 0
	}
	if room.Status == domain.LiveRoomStatusLive {
		room.Status = domain.LiveRoomStatusOffline
	}
	if err := s.rooms.Update(ctx, &room); err != nil {
		return domain.LiveRoom{}, err
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
		if room.Status == domain.LiveRoomStatusLive {
			room.Status = domain.LiveRoomStatusOffline
		}
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
