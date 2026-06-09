package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	auctionports "aieas_backend/internal/modules/auction/ports"
	orderports "aieas_backend/internal/modules/order/ports"
)

type HammerEventPublisher = auctionports.EventPublisher

type LiveSessionCounterWriter interface {
	IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error
}

type HammerLiveAgentHook interface {
	EmitAuctionClosed(ctx context.Context, merchantID string, sessionID, auctionID uint64, status domain.AuctionStatus, price int64, auto bool, reason string)
}

type HammerService struct {
	auctions   auctionports.AuctionRepository
	bids       auctionports.BidRepository
	orders     auctionports.OrderRepository
	deposits   auctionports.DepositRepository
	realtime   auctionports.AuctionRealtimeStore
	tx         auctionports.TxManager
	publisher  HammerEventPublisher
	orderID    auctionports.OrderIDGenerator
	sessions   LiveSessionCounterWriter
	onClose    func(ctx context.Context, auctionID uint64)
	metrics    AuctionMetrics
	tracer     AuctionTracer
	hook       HammerLiveAgentHook
	events     auctionports.SettlementEventPublisher
	payTimeout time.Duration

	// asyncBidEnabled 表示装配期是否启用了异步竞价闭环。
	// 同步模式（false）下 Hammer 走快路径，行为完全不变；
	// 异步模式下 Hammer 走 BeginHammerPending → 屏障 → finalizeHammer 三步。
	asyncBidEnabled bool
	barrier         *InFlightBarrier
	publisherGate   *HammerPublisherGate
	drainMaxWait    time.Duration
}

type HammerServiceDeps struct {
	Auctions        auctionports.AuctionRepository
	Bids            auctionports.BidRepository
	Orders          auctionports.OrderRepository
	Deposits        auctionports.DepositRepository
	Realtime        auctionports.AuctionRealtimeStore
	Tx              auctionports.TxManager
	Publisher       HammerEventPublisher
	OrderID         auctionports.OrderIDGenerator
	Sessions        LiveSessionCounterWriter
	Metrics         AuctionMetrics
	Tracer          AuctionTracer
	LiveAgentHook   HammerLiveAgentHook
	Events          auctionports.SettlementEventPublisher
	OnClose         func(ctx context.Context, auctionID uint64)
	OrderPayTimeout time.Duration
	// 异步落锤过渡态相关：装配期注入。同步模式三个字段都留空即可。
	AsyncBidEnabled bool
	Barrier         *InFlightBarrier
	PublisherGate   *HammerPublisherGate
	DrainMaxWait    time.Duration
}

func NewHammerService(auctions auctionports.AuctionRepository, orders auctionports.OrderRepository, deposits auctionports.DepositRepository, realtime auctionports.AuctionRealtimeStore, tx auctionports.TxManager, publisher HammerEventPublisher) *HammerService {
	return NewHammerServiceWithDeps(HammerServiceDeps{Auctions: auctions, Orders: orders, Deposits: deposits, Realtime: realtime, Tx: tx, Publisher: publisher})
}

func NewHammerServiceWithDeps(deps HammerServiceDeps) *HammerService {
	realtime := deps.Realtime
	if realtime == nil {
		realtime = noopRealtimeStore{}
	}
	tx := deps.Tx
	if tx == nil {
		tx = noopTxManager{}
	}
	payTimeout := deps.OrderPayTimeout
	if payTimeout <= 0 {
		payTimeout = orderports.DefaultPayTimeout
	}
	drainMaxWait := deps.DrainMaxWait
	if drainMaxWait <= 0 {
		drainMaxWait = 5 * time.Second
	}
	return &HammerService{
		auctions: deps.Auctions, bids: deps.Bids, orders: deps.Orders, deposits: deps.Deposits,
		realtime: realtime, tx: tx, publisher: deps.Publisher, orderID: deps.OrderID,
		sessions: deps.Sessions, onClose: deps.OnClose, metrics: deps.Metrics, tracer: deps.Tracer,
		hook: deps.LiveAgentHook, events: deps.Events, payTimeout: payTimeout,
		asyncBidEnabled: deps.AsyncBidEnabled,
		barrier:         deps.Barrier,
		publisherGate:   deps.PublisherGate,
		drainMaxWait:    drainMaxWait,
	}
}

func (s *HammerService) SetOnClose(fn func(ctx context.Context, auctionID uint64)) {
	s.onClose = fn
}

func (s *HammerService) SetOrderIDGenerator(idGen auctionports.OrderIDGenerator) {
	s.orderID = idGen
}

func (s *HammerService) SetOrderPayTimeout(timeout time.Duration) {
	if timeout > 0 {
		s.payTimeout = timeout
	}
}

func (s *HammerService) SetLiveSessionService(sessions LiveSessionCounterWriter) {
	s.sessions = sessions
}

func (s *HammerService) SetLiveAgentHookService(hook HammerLiveAgentHook) {
	s.hook = hook
}

func (s *HammerService) SetMetrics(reg AuctionMetrics) {
	s.metrics = reg
}

func (s *HammerService) SetSettlementEventPublisher(publisher auctionports.SettlementEventPublisher) {
	s.events = publisher
}

// SetAsyncBidWiring 装配期注入异步竞价过渡态依赖。任一为 nil 时不影响同步路径。
//
// 注意：barrier / gate / drainMaxWait 之间互相依赖（barrier 需要 gate；drainMaxWait 仅在
// async 模式下生效）。本函数对 drainMaxWait<=0 的入参做兜底（默认 5s）。
func (s *HammerService) SetAsyncBidWiring(enabled bool, barrier *InFlightBarrier, gate *HammerPublisherGate, drainMaxWait time.Duration) {
	if s == nil {
		return
	}
	s.asyncBidEnabled = enabled
	s.barrier = barrier
	s.publisherGate = gate
	if drainMaxWait > 0 {
		s.drainMaxWait = drainMaxWait
	}
}

// BeginHammerPendingResult 是 BeginHammerPending 的返回结果。
type BeginHammerPendingResult struct {
	// AlreadyClosed 表示 auction 已经处于 HAMMER_PENDING / 终态 / SETTLED，
	// 调用方应该直接返回缓存结果，不再走 finalizeHammer。
	AlreadyClosed bool
	// AlreadyPending 表示 auction 已经在 HAMMER_PENDING（避免重复广播）。
	AlreadyPending bool
	// TransitionedAt 是过渡态切换时刻。
	TransitionedAt time.Time
	// HammerInput 是被 normalize 后的 HammerInput，供后续 FinalizeHammer 使用。
	HammerInput domain.HammerInput
}

// BeginHammerPending 把 auction 切到 HAMMER_PENDING，并广播 auction.state(HAMMER_PENDING)。
// 与 Hammer 共享入参语义。仅做"过渡态"切换：不调 Redis Lua、不写 order、不释放保证金。
//
// 流程：
//  1. normalize HammerInput；
//  2. 取 auction，做与 Hammer 同样的鉴权/早期校验（仅只读那部分，避免重复 close）；
//  3. 已是 HAMMER_PENDING / 终态：直接返回（AlreadyClosed/AlreadyPending=true）；
//  4. version-CAS 把状态写入 HAMMER_PENDING；
//  5. 广播 auction.state(HAMMER_PENDING)；
//  6. 关闭 publisher 闸门（如已注入）防止新命令入队。
func (s *HammerService) BeginHammerPending(ctx context.Context, in domain.HammerInput) (BeginHammerPendingResult, error) {
	in.RequestID = strings.TrimSpace(in.RequestID)
	if in.RequestID == "" {
		in.RequestID = "hammer-pending-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	}
	if in.AuctionID == 0 {
		return BeginHammerPendingResult{}, domain.ErrInvalidArgument
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	if in.IdempotencyTTL <= 0 {
		in.IdempotencyTTL = 24 * time.Hour
	}
	// Force=true（CAP_PRICE / 管理员强制）路径直接进入过渡态，不做 anti-sniping 复核：
	// 这是一个"立即落锤"语义，与 timer/auto-hammer 不同。
	if !in.Force {
		// 二次确认：进入 BeginHammerPending 之前再读一次 Redis state；如果 anti-sniping
		// 链路刚好把 endTime 推后到 now 之后，则不切 HAMMER_PENDING、不关闸门、不广播，
		// 让本轮 timer tick 回到 RUNNING/EXTENDED 等下一拍判断。
		if state, ok, err := s.realtime.GetAuctionState(ctx, in.AuctionID); err == nil && ok && !state.EndTime.IsZero() && in.Now.Before(state.EndTime) {
			return BeginHammerPendingResult{}, domain.ErrInvalidState
		}
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return BeginHammerPendingResult{}, err
	}
	if in.ActorRole != domain.RoleAdmin && !(in.ActorRole == domain.RoleMerchant && in.ActorID == auction.SellerID) {
		return BeginHammerPendingResult{}, domain.ErrForbidden
	}
	// 已经在 HAMMER_PENDING：不重复广播，但仍然要 Close 闸门（幂等），并直接进入 finalize。
	if auction.Status == domain.AuctionStatusHammerPending {
		if s.publisherGate != nil {
			s.publisherGate.Close(in.AuctionID)
		}
		return BeginHammerPendingResult{AlreadyPending: true, TransitionedAt: in.Now, HammerInput: in}, nil
	}
	// 已经是终态/已结算：调用方根据现有逻辑回放幂等结果，不再过渡也不广播。
	if auction.Status.Terminal() || auction.Status == domain.AuctionStatusSettled {
		return BeginHammerPendingResult{AlreadyClosed: true, TransitionedAt: in.Now, HammerInput: in}, nil
	}
	// 状态机：仅允许从 RUNNING/EXTENDED 切到 HAMMER_PENDING。
	if !domain.CanTransitionAuction(auction.Status, domain.AuctionStatusHammerPending) {
		return BeginHammerPendingResult{}, domain.ErrInvalidState
	}
	// CAS 写状态。若发生乐观锁冲突，仍按 invalid_state 处理（保留与 finalizeHammer 一致的语义）。
	pending := auction
	pending.Status = domain.AuctionStatusHammerPending
	if in.ClosedBy != "" {
		pending.ClosedBy = in.ClosedBy
	}
	expectedVersion := pending.Version
	allowedFrom := []domain.AuctionStatus{domain.AuctionStatusRunning, domain.AuctionStatusExtended}
	if err := s.auctions.CloseWithVersion(ctx, &pending, expectedVersion, allowedFrom); err != nil {
		// 闸门优先关闭：防止 publish 已经发过来的命令在我们还没切完状态前进入 worker。
		// 这里失败一般说明状态又被别的路径推进了；重新读一次返回 alreadyClosed/alreadyPending 即可。
		if existing, fetchErr := s.auctions.FindByID(ctx, in.AuctionID); fetchErr == nil {
			if existing.Status == domain.AuctionStatusHammerPending {
				if s.publisherGate != nil {
					s.publisherGate.Close(in.AuctionID)
				}
				return BeginHammerPendingResult{AlreadyPending: true, TransitionedAt: in.Now, HammerInput: in}, nil
			}
			if existing.Status.Terminal() || existing.Status == domain.AuctionStatusSettled {
				return BeginHammerPendingResult{AlreadyClosed: true, TransitionedAt: in.Now, HammerInput: in}, nil
			}
		}
		return BeginHammerPendingResult{}, err
	}
	// 闸门关闭：禁止新命令入队，必须发生在状态切换之后、屏障开始之前。
	if s.publisherGate != nil {
		s.publisherGate.Close(in.AuctionID)
	}
	// 广播 auction.state(HAMMER_PENDING)。复用 auction.state 帧，前端识别 status 即可。
	s.broadcastHammerPendingState(pending, in.Now)
	return BeginHammerPendingResult{TransitionedAt: in.Now, HammerInput: in}, nil
}

func (s *HammerService) broadcastHammerPendingState(auction domain.AuctionLot, now time.Time) {
	if s.publisher == nil || auction.AuctionID == 0 {
		return
	}
	state := domain.AuctionState{
		AuctionID:     auction.AuctionID,
		Status:        domain.AuctionStatusHammerPending,
		StartPrice:    auction.StartPrice,
		CapPrice:      auction.CapPrice,
		IncrementRule: auction.IncrementRule,
		CurrentPrice:  auction.CurrentPrice,
		LeaderBidderID: auction.LeaderBidderID,
		BidCount:       auction.BidCount,
		ParticipantCount: auction.ParticipantCount,
		StartTime:        auction.StartTime,
		EndTime:          auction.EndTime,
		Version:          auction.Version,
		Source:           "mysql",
	}
	if auction.LiveSessionID != nil {
		state.LiveSessionID = *auction.LiveSessionID
	}
	serverTime := now.UTC()
	state.ServerTime = &serverTime
	broadcastJSON(s.publisher, auction.AuctionID, "auction.state", state)
}

func (s *HammerService) Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	ctx, span := startAuctionSpan(ctx, s.tracer, "hammer.close",
		Int64Attr("auction.id", int64(in.AuctionID)),
		StringAttr("hammer.request_id", in.RequestID),
		StringAttr("actor.id", in.ActorID),
		BoolAttr("hammer.force", in.Force),
	)
	defer span.End()
	start := time.Now()
	// 收口入口：sync 模式 / Force=true（CAP_PRICE / 管理员强制）走快路径，行为完全不变。
	// 否则进入 BeginHammerPending → 屏障 → finalize 三步路径。
	useFastPath := !s.asyncBidEnabled || in.Force || s.barrier == nil
	var (
		result domain.HammerResult
		order  *domain.OrderDeal
		err    error
	)
	if useFastPath {
		result, order, err = s.hammerInternal(ctx, in)
	} else {
		result, order, err = s.hammerWithDrain(ctx, in)
	}
	elapsed := time.Since(start)
	span.SetAttributes(
		StringAttr("auction.status", string(result.Status)),
		BoolAttr("hammer.duplicate", result.Duplicate),
		Int64Attr("hammer.price", result.Price),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(AuctionStatusError, err.Error())
	}
	if s.metrics != nil {
		switch {
		case err != nil:
			s.metrics.ObserveHammer("error", elapsed)
		case result.Duplicate:
			s.metrics.IncHammerDuplicate()
			s.metrics.ObserveHammer("duplicate", elapsed)
		case result.Status == domain.AuctionStatusClosedWon:
			s.metrics.ObserveHammer("won", elapsed)
		case result.Status == domain.AuctionStatusClosedFailed:
			s.metrics.ObserveHammer("failed", elapsed)
		default:
			s.metrics.ObserveHammer("other", elapsed)
		}
	}
	return result, order, err
}

// hammerWithDrain 是异步模式下 Hammer 的实现：
//  1. BeginHammerPending：切 HAMMER_PENDING + 广播 auction.state；
//  2. 屏障等待 in-flight 排空（pending=0 + 闸门已关 + 宽限期已过）；
//  3. 真正落锤（finalizeHammer）；
//  4. finalize 完成后清理闸门（不论成功/失败）。
//
// 屏障 ok=false 时强制 finalize（fallback），并打 IncHammerDrainTimeout 指标。
func (s *HammerService) hammerWithDrain(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	pending, err := s.BeginHammerPending(ctx, in)
	if err != nil {
		return domain.HammerResult{}, nil, err
	}
	// HammerPending 指标埋点。
	s.recordHammerPending(in)
	if pending.AlreadyClosed {
		// 已经是终态：直接走 hammerInternal，让它复用已有的 existingCloseResult 逻辑回缓存 / 重放。
		defer s.openGate(in.AuctionID)
		return s.hammerInternal(ctx, pending.HammerInput)
	}
	// 屏障等待。
	maxWait := s.drainMaxWait
	if maxWait <= 0 {
		maxWait = 5 * time.Second
	}
	ok := true
	if s.barrier != nil {
		ok = s.barrier.WaitDrain(ctx, in.AuctionID, maxWait)
	}
	if !ok && s.metrics != nil {
		// barrier 内部已埋一次 timeout 指标；这里是包装语义，不重复打。
		_ = ok
	}
	// 真正落锤。无论 ok=true/false 都进 finalize。
	result, order, finalErr := s.hammerInternal(ctx, pending.HammerInput)
	s.openGate(in.AuctionID)
	return result, order, finalErr
}

func (s *HammerService) recordHammerPending(in domain.HammerInput) {
	type pendingMetrics interface {
		IncHammerPending(trigger string)
	}
	if pm, ok := s.metrics.(pendingMetrics); ok {
		pm.IncHammerPending(HammerTriggerFromInput(in))
	}
}

func (s *HammerService) openGate(auctionID uint64) {
	if s == nil || s.publisherGate == nil || auctionID == 0 {
		return
	}
	s.publisherGate.Open(auctionID)
}

func (s *HammerService) hammerInternal(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	in.RequestID = strings.TrimSpace(in.RequestID)
	if in.RequestID == "" {
		in.RequestID = "hammer-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	}
	if in.AuctionID == 0 {
		return domain.HammerResult{}, nil, domain.ErrInvalidArgument
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	if in.IdempotencyTTL <= 0 {
		in.IdempotencyTTL = 24 * time.Hour
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.HammerResult{}, nil, err
	}
	if in.ActorRole != domain.RoleAdmin && !(in.ActorRole == domain.RoleMerchant && in.ActorID == auction.SellerID) {
		return domain.HammerResult{}, nil, domain.ErrForbidden
	}
	if terminal, order, ok, err := s.existingCloseResult(ctx, auction, in.RequestID); err != nil {
		return domain.HammerResult{}, nil, err
	} else if ok {
		terminal.Duplicate = true
		return terminal, order, nil
	}
	effectiveEnd := auction.EndTime
	if state, ok, err := s.realtime.GetAuctionState(ctx, in.AuctionID); err != nil {
		return domain.HammerResult{}, nil, err
	} else if ok && !state.EndTime.IsZero() {
		effectiveEnd = state.EndTime
	}
	if !in.Force && in.Now.Before(effectiveEnd) {
		return domain.HammerResult{}, nil, domain.ErrInvalidState
	}
	in.ReservePrice = auction.ReservePrice
	result, err := s.realtime.Hammer(ctx, in)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) && !in.Now.Before(effectiveEnd) {
			result, err = s.fallbackHammerFromBidRecords(ctx, auction, in)
			if err != nil {
				return domain.HammerResult{}, nil, err
			}
		} else {
			return domain.HammerResult{}, nil, err
		}
	}
	var order *domain.OrderDeal
	txStart := time.Now()
	txErr := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.auctions.FindByID(txCtx, in.AuctionID)
		if err != nil {
			return err
		}
		closedAt := result.ClosedAt
		current.Status = result.Status
		current.ClosedAt = &closedAt
		if in.ClosedBy != "" {
			current.ClosedBy = in.ClosedBy
		} else {
			current.ClosedBy = in.ActorID
		}
		if result.Status == domain.AuctionStatusClosedWon && (result.WinnerID == "" || result.Price < current.ReservePrice) {
			result.Status = domain.AuctionStatusClosedFailed
			result.WinnerID = ""
			result.Price = 0
		}
		current.Status = result.Status
		if result.Status == domain.AuctionStatusClosedWon {
			current.WinnerID = &result.WinnerID
			current.DealPrice = &result.Price
		} else {
			current.WinnerID = nil
			current.DealPrice = nil
		}
		expectedVersion := current.Version
		allowedFrom := []domain.AuctionStatus{
			domain.AuctionStatusRunning,
			domain.AuctionStatusExtended,
			domain.AuctionStatusHammerPending,
		}
		if err := s.auctions.CloseWithVersion(txCtx, &current, expectedVersion, allowedFrom); err != nil {
			return err
		}
		if result.Status != domain.AuctionStatusClosedWon {
			return s.releaseDeposits(txCtx, current.AuctionID, "released_by_auction_failed", nil)
		}
		var orderID uint64
		if s.orderID != nil {
			generated, err := s.orderID.NextOrderID()
			if err != nil {
				return err
			}
			orderID = generated
		}
		depositAmount := current.DepositAmount
		dealOrder := &domain.OrderDeal{
			ID:            orderID,
			AuctionID:     current.AuctionID,
			LiveSessionID: cloneLiveSessionID(current.LiveSessionID),
			LotSnapshot:   buildLotDealSnapshot(current, result.Price, result.WinnerID, closedAt),
			WinnerID:      result.WinnerID,
			SellerID:      current.SellerID,
			DealPrice:     result.Price,
			DepositAmount: depositAmount,
			Status:        domain.OrderStatusCreated,
			PayStatus:     domain.PayStatusUnpaid,
			PayDeadline:   ptrTime(in.Now.Add(s.payTimeout)),
			CreatedAt:     in.Now,
			UpdatedAt:     in.Now,
		}
		created, _, err := s.orders.CreateIfAbsentByAuction(txCtx, dealOrder)
		if err != nil {
			return err
		}
		order = &created
		if s.deposits != nil {
			if deposit, err := s.deposits.FindByAuctionUser(txCtx, current.AuctionID, result.WinnerID); err == nil {
				deposit.Status = domain.DepositStatusCaptured
				deposit.RelatedOrderID = &created.ID
				deposit.Remark = "captured_by_hammer"
				if err := s.deposits.Update(txCtx, &deposit); err != nil {
					return err
				}
			}
			return s.releaseDeposits(txCtx, current.AuctionID, "released_by_hammer", &result.WinnerID)
		}
		return nil
	})
	if s.metrics != nil {
		s.metrics.ObserveHammerMySQLTx(time.Since(txStart))
		if txErr != nil && !errors.Is(txErr, domain.ErrOptimisticConflict) && !errors.Is(txErr, domain.ErrInvalidState) {
			s.metrics.IncHammerMySQLFail()
		}
	}
	if txErr != nil {
		err := txErr
		if errors.Is(err, domain.ErrOptimisticConflict) {
			if s.metrics != nil {
				s.metrics.IncHammerOptimisticConflict()
			}
			return domain.HammerResult{}, nil, err
		}
		if errors.Is(err, domain.ErrInvalidState) {
			if existing, fetchErr := s.auctions.FindByID(ctx, in.AuctionID); fetchErr == nil {
				if terminal, terminalOrder, ok, terminalErr := s.existingCloseResult(ctx, existing, in.RequestID); terminalErr == nil && ok {
					terminal.Duplicate = true
					return terminal, terminalOrder, nil
				}
			}
			return domain.HammerResult{}, nil, err
		}
		return domain.HammerResult{}, nil, err
	}
	if s.sessions != nil && auction.LiveSessionID != nil {
		counters := domain.LiveSessionCounters{}
		if result.Status == domain.AuctionStatusClosedWon {
			counters.LotsSoldDelta = 1
			counters.GMVCentDelta = result.Price
		} else {
			counters.LotsUnsoldDelta = 1
		}
		_ = s.sessions.IncrCounters(ctx, *auction.LiveSessionID, counters)
	}
	s.publishSettlementEvents(ctx, auction, result, order, in)
	payload := map[string]interface{}{
		"auctionId":  result.AuctionID,
		"status":     result.Status,
		"winnerId":   result.WinnerID,
		"price":      result.Price,
		"closedAt":   result.ClosedAt,
		"serverTime": result.ClosedAt,
	}
	if auction.LiveSessionID != nil {
		payload["liveSessionId"] = *auction.LiveSessionID
	}
	if order != nil {
		payload["orderId"] = order.ID
	}
	broadcastJSON(s.publisher, result.AuctionID, "auction.closed", payload)
	if s.hook != nil && auction.LiveSessionID != nil {
		reason := ""
		if result.Status == domain.AuctionStatusClosedFailed {
			reason = "未达到保留价或无人有效出价"
		}
		s.hook.EmitAuctionClosed(ctx, auction.SellerID, *auction.LiveSessionID, auction.AuctionID, result.Status, result.Price, isAutoHammerInput(in), reason)
	}
	if s.onClose != nil {
		s.onClose(ctx, result.AuctionID)
	}
	return result, order, nil
}

func isAutoHammerInput(in domain.HammerInput) bool {
	closedBy := strings.ToUpper(strings.TrimSpace(in.ClosedBy))
	if closedBy == "SYSTEM" || closedBy == "CAP_PRICE" {
		return true
	}
	requestID := strings.ToLower(strings.TrimSpace(in.RequestID))
	return strings.HasPrefix(requestID, "auto-") || strings.HasPrefix(requestID, "cap-")
}

func buildLotDealSnapshot(lot domain.AuctionLot, dealPrice int64, winnerID string, closedAt time.Time) json.RawMessage {
	payload := map[string]interface{}{
		"auctionId":     lot.AuctionID,
		"sellerId":      lot.SellerID,
		"liveSessionId": lot.LiveSessionID,
		"title":         lot.Title,
		"description":   lot.Description,
		"category":      lot.Category,
		"brand":         lot.Brand,
		"condition":     lot.ConditionGrade,
		"imageUrls":     lot.ImageURLs,
		"coverUrl":      lot.CoverURL,
		"startPrice":    lot.StartPrice,
		"reservePrice":  lot.ReservePrice,
		"capPrice":      lot.CapPrice,
		"incrementRule": json.RawMessage(lot.IncrementRule),
		"depositAmount": lot.DepositAmount,
		"dealPrice":     dealPrice,
		"winnerId":      winnerID,
		"closedAt":      closedAt,
	}
	data, _ := json.Marshal(payload)
	return data
}

func (s *HammerService) publishSettlementEvents(ctx context.Context, auction domain.AuctionLot, result domain.HammerResult, order *domain.OrderDeal, in domain.HammerInput) {
	if s.events == nil {
		return
	}
	eventAuction := auction
	eventAuction.Status = result.Status
	eventAuction.ClosedAt = &result.ClosedAt
	if in.ClosedBy != "" {
		eventAuction.ClosedBy = in.ClosedBy
	} else {
		eventAuction.ClosedBy = in.ActorID
	}
	if result.Status == domain.AuctionStatusClosedWon {
		eventAuction.WinnerID = &result.WinnerID
		eventAuction.DealPrice = &result.Price
	} else {
		eventAuction.WinnerID = nil
		eventAuction.DealPrice = nil
	}
	if err := s.events.PublishAuctionClosed(ctx, eventAuction, result, order); err != nil {
		slog.Default().Warn("publish auction closed kafka event failed", "auction_id", result.AuctionID, "error", err)
	}
	if order != nil {
		if err := s.events.PublishOrderCreated(ctx, *order); err != nil {
			slog.Default().Warn("publish order created kafka event failed", "auction_id", result.AuctionID, "order_id", order.ID, "error", err)
		}
	}
}

func (s *HammerService) fallbackHammerFromBidRecords(ctx context.Context, auction domain.AuctionLot, in domain.HammerInput) (domain.HammerResult, error) {
	result := domain.HammerResult{
		RequestID: in.RequestID,
		AuctionID: auction.AuctionID,
		Status:    domain.AuctionStatusClosedFailed,
		ClosedAt:  in.Now,
		Version:   auction.Version + 1,
	}
	records, err := listAuctionBidRecordsForRound(ctx, s.bids, auction.AuctionID, auction.StartTime.UnixMilli(), 1)
	if err != nil {
		return domain.HammerResult{}, err
	}
	if len(records) == 0 {
		return result, nil
	}
	top := records[0]
	if top.BidderID == "" || top.BidPrice < auction.ReservePrice {
		return result, nil
	}
	result.Status = domain.AuctionStatusClosedWon
	result.WinnerID = top.BidderID
	result.Price = top.BidPrice
	return result, nil
}

func (s *HammerService) existingCloseResult(ctx context.Context, auction domain.AuctionLot, requestID string) (domain.HammerResult, *domain.OrderDeal, bool, error) {
	if !auction.Status.Terminal() {
		return domain.HammerResult{}, nil, false, nil
	}
	closedAt := time.Now().UTC()
	if auction.ClosedAt != nil {
		closedAt = *auction.ClosedAt
	}
	result := domain.HammerResult{RequestID: requestID, AuctionID: auction.AuctionID, Status: auction.Status, ClosedAt: closedAt}
	if auction.WinnerID != nil {
		result.WinnerID = *auction.WinnerID
	}
	if auction.DealPrice != nil {
		result.Price = *auction.DealPrice
	}
	var order *domain.OrderDeal
	if auction.Status == domain.AuctionStatusClosedWon && s.orders != nil {
		if existing, err := s.orders.FindByAuctionID(ctx, auction.AuctionID); err == nil {
			order = &existing
		} else if err != domain.ErrNotFound {
			return domain.HammerResult{}, nil, false, err
		}
	}
	return result, order, true, nil
}

func (s *HammerService) releaseDeposits(ctx context.Context, auctionID uint64, remark string, winnerID *string) error {
	if s.deposits == nil {
		return nil
	}
	deposits, err := s.deposits.ListByAuction(ctx, auctionID)
	if err != nil {
		return err
	}
	for _, deposit := range deposits {
		if winnerID != nil && deposit.UserID == *winnerID {
			continue
		}
		if deposit.Status != domain.DepositStatusReady && deposit.Status != domain.DepositStatusPending {
			continue
		}
		deposit.Status = domain.DepositStatusReleased
		deposit.RelatedOrderID = nil
		deposit.Remark = remark
		if err := s.deposits.Update(ctx, &deposit); err != nil {
			return err
		}
	}
	return nil
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func cloneLiveSessionID(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func broadcastJSON(publisher HammerEventPublisher, auctionID uint64, eventType string, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, auctionports.EventEnvelope{Type: eventType, Payload: raw})
}

type noopTxManager struct{}

func (noopTxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type noopRealtimeStore struct{}

func (noopRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	_ = auctionID
	return domain.AuctionState{}, false, nil
}

func (noopRealtimeStore) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return nil, nil
}

func (noopRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = auction
	_ = minIncrement
	return domain.AuctionState{}, nil
}

func (noopRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	_ = auctionID
	_ = userID
	return nil
}

func (noopRealtimeStore) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	_ = ctx
	_ = auctionID
	_ = userID
	return false, false, nil
}

func (noopRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
	_ = input
	return domain.BidResult{}, nil
}

func (noopRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	return domain.HammerResult{RequestID: input.RequestID, AuctionID: input.AuctionID, Status: domain.AuctionStatusClosedFailed, ClosedAt: input.Now}, nil
}
