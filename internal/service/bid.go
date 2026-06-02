package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/singleflight"
)

const bidAuctionCacheTTL = 2 * time.Second
const bidRealtimeStateCacheTTL = 200 * time.Millisecond
const bidNicknameCacheTTL = 5 * time.Minute
const defaultBidIdempotencyTTL = 30 * time.Second
const bidRankingBroadcastDelay = 200 * time.Millisecond

type BidService struct {
	bids      repository.BidRepository
	auctions  repository.AuctionRepository
	realtime  repository.AuctionRealtimeStore
	risk      *RiskService
	hammer    *HammerService
	publisher EventPublisher
	sessions  *LiveSessionService
	cfg       appconfig.AuctionConfig
	metrics   *metrics.Registry
	hook      *LiveAgentHookService
	configs   repository.ConfigRepository
	controls  *RiskControlService
	users     repository.UserRepository

	auctionCacheMu  sync.RWMutex
	auctionCache    map[uint64]cachedBidAuction
	auctionCacheTTL time.Duration
	auctionGroup    singleflight.Group

	realtimeStateCache sync.Map // map[uint64]*bidRealtimeStateCell

	nicknameCacheMu sync.RWMutex
	nicknameCache   map[string]cachedBidderNickname

	rankingBroadcastMu     sync.Mutex
	rankingBroadcastTimers map[uint64]*time.Timer

	blacklistStrategyMu        sync.RWMutex
	blacklistStrategyCached    bool
	blacklistStrategy          domain.BlacklistStrategyConfig
	blacklistStrategyExpiresAt time.Time
}

type PlaceBidInput struct {
	RequestID            string
	AuctionID            uint64
	BidderID             string
	UserRole             domain.Role
	Price                int64
	ExpectedCurrentPrice *int64
	Source               string
}

func NewBidService(bids repository.BidRepository, auctions repository.AuctionRepository, realtime repository.AuctionRealtimeStore, risk *RiskService, publisher EventPublisher, cfg appconfig.AuctionConfig) *BidService {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	if cfg.MinIncrementCent <= 0 {
		cfg.MinIncrementCent = 1
	}
	if cfg.AntiSnipeMs <= 0 {
		cfg.AntiSnipeMs = 30000
	}
	if cfg.ExtendMs <= 0 {
		cfg.ExtendMs = 30000
	}
	if cfg.MaxExtendCount <= 0 {
		cfg.MaxExtendCount = 20
	}
	if cfg.FreqLimitCount <= 0 {
		cfg.FreqLimitCount = 10
	}
	if cfg.FreqWindowMs <= 0 {
		cfg.FreqWindowMs = 1000
	}
	if cfg.BidIdempotencyTTL.Std() <= 0 {
		cfg.BidIdempotencyTTL = appconfig.Duration(defaultBidIdempotencyTTL)
	}
	return &BidService{
		bids:                   bids,
		auctions:               auctions,
		realtime:               realtime,
		risk:                   risk,
		publisher:              publisher,
		cfg:                    cfg,
		auctionCache:           make(map[uint64]cachedBidAuction),
		auctionCacheTTL:        bidAuctionCacheTTL,
		nicknameCache:          make(map[string]cachedBidderNickname),
		rankingBroadcastTimers: make(map[uint64]*time.Timer),
	}
}

// SetLiveSessionService 注入直播场次服务，用于在 persistBid 时回填 live_session_id 与累加 bid_count。
func (s *BidService) SetLiveSessionService(sessions *LiveSessionService) {
	s.sessions = sessions
}

// SetHammerService enables cap-price auto close after an accepted bid reaches capPrice.
func (s *BidService) SetHammerService(hammer *HammerService) {
	s.hammer = hammer
}

// SetLiveAgentHookService 注入直播拍卖事件 hook。
func (s *BidService) SetLiveAgentHookService(hook *LiveAgentHookService) {
	s.hook = hook
}

func (s *BidService) SetConfigRepository(configs repository.ConfigRepository) {
	s.configs = configs
}

func (s *BidService) SetRiskControlService(controls *RiskControlService) {
	s.controls = controls
}

func (s *BidService) SetUserRepository(users repository.UserRepository) {
	s.users = users
}

// SetMetrics 注入观测性 Registry。nil 安全：未注入时所有 Observe* 调用走 noop。
func (s *BidService) SetMetrics(reg *metrics.Registry) {
	s.metrics = reg
}

func (s *BidService) Place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	ctx, span := tracing.StartSpan(ctx, "bid.place",
		attribute.Int64("auction.id", int64(in.AuctionID)),
		attribute.String("bid.request_id", in.RequestID),
		attribute.String("bid.source", in.Source),
		attribute.Int64("bid.price", in.Price),
	)
	defer span.End()
	start := time.Now()
	result, err := s.place(ctx, in)
	elapsed := time.Since(start)
	span.SetAttributes(
		attribute.Bool("bid.accepted", result.Accepted),
		attribute.Bool("bid.duplicate", result.Duplicate),
		attribute.String("bid.reject_reason", result.Reason),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if !result.Accepted && !result.Duplicate {
		span.SetStatus(codes.Error, result.Reason)
	}
	if s.metrics != nil {
		switch {
		case err != nil:
			s.metrics.ObserveBid("error", "internal", elapsed)
		case result.Duplicate:
			s.metrics.IncBidDuplicate()
			s.metrics.ObserveBid("duplicate", "", elapsed)
		case result.Accepted:
			s.metrics.ObserveBid("accepted", "", elapsed)
		default:
			reason := strings.TrimSpace(result.Reason)
			if reason == "" {
				reason = "unknown"
			}
			s.metrics.IncBidReject(reason)
			s.metrics.ObserveBid("rejected", reason, elapsed)
			if reason == "FREQ_LIMIT" {
				s.metrics.IncBidFreqLimit()
			}
		}
	}
	return result, err
}

func (s *BidService) place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	stageStart := time.Now()
	in.RequestID = strings.TrimSpace(in.RequestID)
	in.BidderID = strings.TrimSpace(in.BidderID)
	if in.RequestID == "" || in.AuctionID == 0 || in.BidderID == "" || in.Price <= 0 || in.UserRole != domain.RoleBuyer || in.ExpectedCurrentPrice == nil {
		s.observeBidStage("input_validate", "error", stageStart)
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	s.observeBidStage("input_validate", "ok", stageStart)
	// P0-3：bidStreamEnabled=true 时跳过 MySQL `bid_record` 的 FindByRequestID 前置查询。
	// Redis 幂等由 bid.lua 在同一次 EVALSHA 内完成，避免热路径额外 GET。
	streamEnabled := bidStreamEnabled(s.realtime)
	if !streamEnabled && s.bids != nil {
		stageStart = time.Now()
		record, err := s.bids.FindByRequestID(ctx, in.RequestID)
		if err == nil {
			s.observeBidStage("mysql_idempotency", "hit", stageStart)
			return bidResultFromRecord(record), nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			s.observeBidStage("mysql_idempotency", "error", stageStart)
			return domain.BidResult{}, err
		}
		s.observeBidStage("mysql_idempotency", "miss", stageStart)
	}
	if result, ok := s.preRedisLocalStateReject(in); ok {
		return result, nil
	}
	if result, ok := s.preRedisLocalFloorReject(ctx, in); ok {
		return result, nil
	}
	stageStart = time.Now()
	auction, err := s.bidAuctionSnapshot(ctx, in.AuctionID)
	if err != nil {
		s.observeBidStage("auction_snapshot", "error", stageStart)
		return domain.BidResult{}, err
	}
	s.observeBidStage("auction_snapshot", "ok", stageStart)
	liveSessionID := auction.LiveSessionID
	now := time.Now().UTC()
	if in.Source == "" {
		in.Source = "live_ws"
	}
	stageStart = time.Now()
	riskControl := s.currentRiskControl(ctx)
	riskControlEnabled := riskControl.Enabled
	if riskControlEnabled {
		s.observeBidStage("risk_control", "enabled", stageStart)
	} else {
		s.observeBidStage("risk_control", "disabled", stageStart)
	}
	blacklistStrategy := domain.BlacklistStrategyConfig{}
	if riskControlEnabled {
		stageStart = time.Now()
		blacklistStrategy = s.currentBlacklistStrategy(ctx)
		if blacklistStrategy.Enabled {
			s.observeBidStage("blacklist_strategy", "enabled", stageStart)
		} else {
			s.observeBidStage("blacklist_strategy", "disabled", stageStart)
		}
	}
	// v2 起黑名单完全在 service 层做前置门面拦截：MySQL（source of truth）+ LayeredCache，
	// 不再下沉到 Lua（避免把全局黑名单 key 复制到每个 RT shard）。
	// RiskService.IsBlacklisted 在 cache/repo 故障时 fail-open，由 cap-price 等下游约束兜底。
	if s.risk != nil && riskControlEnabled {
		stageStart = time.Now()
		isBlacklisted, err := s.risk.IsBlacklisted(ctx, in.BidderID)
		if err != nil {
			s.observeBidStage("blacklist_lookup", "error", stageStart)
			return domain.BidResult{}, err
		}
		if isBlacklisted {
			s.observeBidStage("blacklist_lookup", "hit", stageStart)
			result := domain.BidResult{
				RequestID:     in.RequestID,
				AuctionID:     in.AuctionID,
				LiveSessionID: liveSessionID,
				BidderID:      in.BidderID,
				Price:         in.Price,
				Accepted:      false,
				Reason:        "BLACKLIST",
				CurrentPrice:  auction.StartPrice,
				EndTime:       auction.EndTime,
				Event:         "bid.rejected",
				RiskResult:    domain.BidRiskReject,
			}
			s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
			return result, nil
		}
		s.observeBidStage("blacklist_lookup", "miss", stageStart)
	}
	stageStart = time.Now()
	rule, err := domain.ParseIncrementRule(auction.IncrementRule)
	if err != nil {
		s.observeBidStage("increment_rule", "error", stageStart)
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	s.observeBidStage("increment_rule", "ok", stageStart)
	minIncrement := rule.AmountForPrice(auction.StartPrice)
	if minIncrement <= 0 {
		minIncrement = s.cfg.MinIncrementCent
	}
	if minIncrement <= 0 {
		minIncrement = 1
	}
	freqLimitCount := s.cfg.FreqLimitCount
	freqWindowMS := s.cfg.FreqWindowMs
	if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.FrequencyEnabled {
		freqLimitCount = blacklistStrategy.FrequencyMaxRequests
		freqWindowMS = blacklistStrategy.FrequencyWindowMs
	} else if !riskControlEnabled {
		freqLimitCount = 0
		freqWindowMS = 0
	}
	stageStart = time.Now()
	bidderNickname := s.bidderNickname(ctx, in.BidderID)
	if bidderNickname == "" {
		s.observeBidStage("bidder_nickname", "empty", stageStart)
	} else {
		s.observeBidStage("bidder_nickname", "ok", stageStart)
	}
	stageStart = time.Now()
	result, err := s.realtime.PlaceBid(ctx, domain.BidInput{
		RequestID:            in.RequestID,
		AuctionID:            in.AuctionID,
		LiveSessionID:        liveSessionID,
		BidderID:             in.BidderID,
		BidderNickname:       bidderNickname,
		Price:                in.Price,
		ExpectedCurrentPrice: in.ExpectedCurrentPrice,
		Now:                  now,
		Source:               in.Source,
		MinIncrement:         minIncrement,
		AntiSnipingMS:        int64(auction.AntiSnipingSec) * 1000,
		AntiExtendMS:         int64(auction.AntiExtendSec) * 1000,
		AntiExtendMode:       domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		MaxExtendCount:       s.cfg.MaxExtendCount,
		FreqLimitCount:       freqLimitCount,
		FreqWindowMS:         freqWindowMS,
		IdempotencyTTL:       s.bidIdempotencyTTL(),
		StartPrice:           auction.StartPrice,
		CapPrice:             auction.CapPrice,
		IncrementRule:        rule,
	})
	if err != nil {
		s.observeBidStage("realtime_place_bid", "error", stageStart)
		return domain.BidResult{}, err
	}
	s.observeBidStage("realtime_place_bid", bidStageResult(result), stageStart)
	s.storeBidRealtimeStateFromResult(in.AuctionID, result, time.Now().Add(bidRealtimeStateCacheTTL))
	if result.RiskResult == "" {
		if result.Accepted {
			result.RiskResult = domain.BidRiskAllow
		} else {
			result.RiskResult = domain.BidRiskReject
		}
	}
	stageStart = time.Now()
	s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
	s.observeBidStage("enrich_result", bidStageResult(result), stageStart)
	if !result.Accepted && !result.Duplicate && riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.MissingDepositEnabled && s.risk != nil &&
		(result.Reason == "NOT_ENROLLED" || result.Reason == "DEPOSIT_NOT_READY") {
		stageStart = time.Now()
		s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_"+result.Reason, result)
		s.observeBidStage("schedule_auto_blacklist", "missing_deposit", stageStart)
	}
	if !result.Accepted && !result.Duplicate && riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.UnreasonablePriceEnabled && s.risk != nil &&
		(isAutoBlacklistPriceReason(result.Reason) || bidAboveAllowedMax(auction.StartPrice, result.CurrentPrice, auction.CapPrice, in.Price, rule)) {
		stageStart = time.Now()
		s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_"+result.Reason, result)
		s.observeBidStage("schedule_auto_blacklist", "price", stageStart)
	}
	if result.Accepted && !result.Duplicate && s.hook != nil && auction.LiveSessionID != 0 {
		hookCtx := context.WithoutCancel(ctx)
		sellerID := auction.SellerID
		sessionID := auction.LiveSessionID
		bidderID := result.BidderID
		bidderName := result.BidderNickname
		currentPrice := result.CurrentPrice
		go func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Default().Error("live agent hook panic", "auction_id", in.AuctionID, "session_id", sessionID, "panic", r)
				}
			}()
			emitStart := time.Now()
			s.hook.EmitHighestBidWithBidderName(hookCtx, sellerID, sessionID, bidderID, bidderName, currentPrice)
			s.observeBidStage("live_agent_hook", "emit", emitStart)
		}()
	}
	if !streamEnabled {
		stageStart = time.Now()
		s.persistBid(ctx, in, result, now)
		s.observeBidStage("sync_persist_bid", bidStageResult(result), stageStart)
	}
	if result.Accepted && !result.Duplicate && result.AutoClosed && s.hammer != nil {
		stageStart = time.Now()
		if _, _, err := s.hammer.Hammer(ctx, domain.HammerInput{
			RequestID:      "cap-" + in.RequestID,
			AuctionID:      in.AuctionID,
			ActorID:        "system",
			ActorRole:      domain.RoleAdmin,
			ClosedBy:       "CAP_PRICE",
			Force:          true,
			Now:            now,
			IdempotencyTTL: 24 * time.Hour,
		}); err != nil {
			s.observeBidStage("cap_hammer", "error", stageStart)
			return domain.BidResult{}, err
		}
		s.observeBidStage("cap_hammer", "ok", stageStart)
	}
	if result.Reason == "FREQ_LIMIT" && !result.Duplicate && s.risk != nil {
		stageStart = time.Now()
		s.risk.RecordEvent(ctx, "BID_FREQ", in.BidderID, in.AuctionID, domain.RiskSeverityMid, result)
		if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.FrequencyEnabled {
			s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_FREQ_LIMIT", result)
		}
		s.observeBidStage("risk_record_freq", "ok", stageStart)
	}
	if !streamEnabled {
		stageStart = time.Now()
		s.publishBidResult(ctx, result)
		s.observeBidStage("direct_publish_result", bidStageResult(result), stageStart)
	}
	return result, nil
}

func (s *BidService) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	if auctionID == 0 {
		return nil, domain.ErrInvalidArgument
	}
	return s.realtime.TopN(ctx, auctionID, limit)
}

func (s *BidService) bidIdempotencyTTL() time.Duration {
	if s == nil || s.cfg.BidIdempotencyTTL.Std() <= 0 {
		return defaultBidIdempotencyTTL
	}
	return s.cfg.BidIdempotencyTTL.Std()
}

func (s *BidService) preRedisLocalStateReject(in PlaceBidInput) (domain.BidResult, bool) {
	stageStart := time.Now()
	state, ok := s.cachedBidRealtimeState(in.AuctionID, stageStart)
	if !ok {
		return domain.BidResult{}, false
	}
	reason := conservativeLocalStateRejectReason(in, state)
	if reason == "" {
		return domain.BidResult{}, false
	}
	s.observeBidStage("local_state_precheck", reason, stageStart)
	return rejectBidFromState(in, state, reason), true
}

func (s *BidService) preRedisLocalFloorReject(ctx context.Context, in PlaceBidInput) (domain.BidResult, bool) {
	stageStart := time.Now()
	state, ok := s.cachedBidRealtimeState(in.AuctionID, stageStart)
	if !ok || state.AuctionID != in.AuctionID {
		return domain.BidResult{}, false
	}
	if state.CurrentPrice <= 0 {
		return domain.BidResult{}, false
	}
	if state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended {
		return domain.BidResult{}, false
	}
	auction, ok := s.cachedBidAuction(in.AuctionID, stageStart)
	if !ok {
		return domain.BidResult{}, false
	}
	rule, err := domain.ParseIncrementRule(auction.IncrementRule)
	if err != nil {
		return domain.BidResult{}, false
	}
	reason, hit := snapshotFloorPreRejectReason(in, state, true, auction, rule)
	if !hit {
		return domain.BidResult{}, false
	}
	result := rejectBidFromState(in, state, reason)
	s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
	s.observeBidStage("pre_reject_local", reason, stageStart)
	return result, true
}

func conservativeLocalStateRejectReason(in PlaceBidInput, state domain.AuctionState) string {
	if state.AuctionID == 0 || (state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended) {
		return ""
	}
	if in.ExpectedCurrentPrice == nil {
		return domain.BidRejectMissingExpectedState
	}
	expectedCurrentPrice := *in.ExpectedCurrentPrice
	if expectedCurrentPrice < 0 {
		return domain.BidRejectStaleAuctionState
	}
	if state.StartPrice > 0 && in.Price <= state.StartPrice {
		return domain.BidRejectBelowStartPrice
	}
	if state.CapPrice > 0 && in.Price > state.CapPrice {
		return domain.BidRejectAboveCapPrice
	}
	rule, err := domain.ParseIncrementRule(state.IncrementRule)
	if err != nil || rule.Type != domain.IncrementRuleTypeFixed || rule.Amount <= 0 || rule.MaxBidSteps <= 0 {
		return ""
	}
	amount := rule.AmountForPrice(state.CurrentPrice)
	if amount <= 0 {
		return ""
	}
	if expectedCurrentPrice > state.CurrentPrice {
		return ""
	}
	expectedAmount := rule.AmountForPrice(expectedCurrentPrice)
	if expectedAmount <= 0 {
		return ""
	}
	expectedMaxAllowed := expectedCurrentPrice + expectedAmount*int64(rule.MaxBidSteps)
	if state.CapPrice > 0 && expectedMaxAllowed > state.CapPrice {
		expectedMaxAllowed = state.CapPrice
	}
	if expectedCurrentPrice < state.CurrentPrice && in.Price > expectedMaxAllowed {
		return domain.BidRejectAboveExpectedMaxBidSteps
	}
	isCapBid := state.CapPrice > 0 && in.Price == state.CapPrice
	if !isCapBid && (in.Price-state.CurrentPrice)%amount != 0 {
		return domain.BidRejectStepMismatch
	}
	if isCapBid {
		if in.Price <= state.CurrentPrice {
			return domain.BidRejectBelowMinIncrement
		}
		return ""
	}
	if in.Price < state.CurrentPrice+amount {
		return domain.BidRejectBelowMinIncrement
	}
	return ""
}

type bidAuctionSnapshot struct {
	AuctionID      uint64
	SellerID       string
	LiveSessionID  uint64
	StartPrice     int64
	CapPrice       int64
	IncrementRule  json.RawMessage
	AntiSnipingSec int
	AntiExtendSec  int
	AntiExtendMode domain.AuctionExtendMode
	Status         domain.AuctionStatus
	StartTime      time.Time
	EndTime        time.Time
}

type cachedBidAuction struct {
	value     bidAuctionSnapshot
	expiresAt time.Time
}

type cachedBidRealtimeState struct {
	value     domain.AuctionState
	expiresAt time.Time
}

type bidRealtimeStateCell struct {
	value atomic.Pointer[cachedBidRealtimeState]
}

type cachedBidderNickname struct {
	nickname  string
	expiresAt time.Time
}

type bidAuctionSnapshotLoad struct {
	value  bidAuctionSnapshot
	source string
}

func (s *BidService) bidAuctionSnapshot(ctx context.Context, auctionID uint64) (bidAuctionSnapshot, error) {
	if auctionID == 0 {
		return bidAuctionSnapshot{}, domain.ErrInvalidArgument
	}
	stageStart := time.Now()
	now := time.Now()
	if cached, ok := s.cachedBidAuction(auctionID, now); ok {
		s.observeBidStage("auction_snapshot_source", "cache_hit", stageStart)
		return cached, nil
	}
	value, err, _ := s.auctionGroup.Do(strconv.FormatUint(auctionID, 10), func() (interface{}, error) {
		if cached, ok := s.cachedBidAuction(auctionID, time.Now()); ok {
			return bidAuctionSnapshotLoad{value: cached, source: "cache_hit_after_wait"}, nil
		}
		if s.auctions == nil {
			return bidAuctionSnapshot{}, domain.ErrNotFound
		}
		auction, err := s.auctions.FindByID(ctx, auctionID)
		if err != nil {
			return bidAuctionSnapshot{}, err
		}
		snapshot := bidAuctionSnapshotFromLot(auction)
		s.storeBidAuction(snapshot, time.Now().Add(s.effectiveAuctionCacheTTL()))
		return bidAuctionSnapshotLoad{value: snapshot, source: "db"}, nil
	})
	if err != nil {
		s.observeBidStage("auction_snapshot_source", "error", stageStart)
		return bidAuctionSnapshot{}, err
	}
	loaded, ok := value.(bidAuctionSnapshotLoad)
	if !ok {
		s.observeBidStage("auction_snapshot_source", "error", stageStart)
		return bidAuctionSnapshot{}, domain.ErrInvalidState
	}
	s.observeBidStage("auction_snapshot_source", loaded.source, stageStart)
	return loaded.value, nil
}

func (s *BidService) cachedBidAuction(auctionID uint64, now time.Time) (bidAuctionSnapshot, bool) {
	s.auctionCacheMu.RLock()
	defer s.auctionCacheMu.RUnlock()
	if s.auctionCache == nil {
		return bidAuctionSnapshot{}, false
	}
	cached, ok := s.auctionCache[auctionID]
	if !ok || now.After(cached.expiresAt) {
		return bidAuctionSnapshot{}, false
	}
	return cached.value, true
}

func (s *BidService) storeBidAuction(snapshot bidAuctionSnapshot, expiresAt time.Time) {
	if snapshot.AuctionID == 0 {
		return
	}
	s.auctionCacheMu.Lock()
	defer s.auctionCacheMu.Unlock()
	if s.auctionCache == nil {
		s.auctionCache = make(map[uint64]cachedBidAuction)
	}
	s.auctionCache[snapshot.AuctionID] = cachedBidAuction{value: snapshot, expiresAt: expiresAt}
}

func (s *BidService) effectiveAuctionCacheTTL() time.Duration {
	if s == nil || s.auctionCacheTTL <= 0 {
		return bidAuctionCacheTTL
	}
	return s.auctionCacheTTL
}

func (s *BidService) cachedBidRealtimeState(auctionID uint64, now time.Time) (domain.AuctionState, bool) {
	if s == nil || auctionID == 0 {
		return domain.AuctionState{}, false
	}
	raw, ok := s.realtimeStateCache.Load(auctionID)
	if !ok {
		return domain.AuctionState{}, false
	}
	cell, ok := raw.(*bidRealtimeStateCell)
	if !ok || cell == nil {
		return domain.AuctionState{}, false
	}
	return cell.load(now)
}

func (s *BidService) storeBidRealtimeState(state domain.AuctionState, expiresAt time.Time) {
	if s == nil || state.AuctionID == 0 {
		return
	}
	cell := s.realtimeStateCell(state.AuctionID)
	if cell == nil {
		return
	}
	now := time.Now()
	next := &cachedBidRealtimeState{value: state, expiresAt: expiresAt}
	for {
		current := cell.value.Load()
		if current != nil && current.expiresAt.After(now) && !cachedStateNewerThan(state, current.value, true) {
			return
		}
		if cell.value.CompareAndSwap(current, next) {
			return
		}
	}
}

func (s *BidService) realtimeStateCell(auctionID uint64) *bidRealtimeStateCell {
	if s == nil || auctionID == 0 {
		return nil
	}
	cell := &bidRealtimeStateCell{}
	raw, _ := s.realtimeStateCache.LoadOrStore(auctionID, cell)
	stored, ok := raw.(*bidRealtimeStateCell)
	if !ok {
		return nil
	}
	return stored
}

func (c *bidRealtimeStateCell) load(now time.Time) (domain.AuctionState, bool) {
	if c == nil {
		return domain.AuctionState{}, false
	}
	cached := c.value.Load()
	if cached == nil || now.After(cached.expiresAt) {
		return domain.AuctionState{}, false
	}
	return cached.value, true
}

func cachedStateNewerThan(cached domain.AuctionState, base domain.AuctionState, baseOK bool) bool {
	if cached.AuctionID == 0 {
		return false
	}
	if !baseOK || base.AuctionID == 0 {
		return true
	}
	if cached.Version > base.Version {
		return true
	}
	return cached.CurrentPrice > base.CurrentPrice
}

func bidRealtimeStateFromResult(auctionID uint64, result domain.BidResult) (domain.AuctionState, bool) {
	id := auctionID
	if result.AuctionID != 0 {
		id = result.AuctionID
	}
	if id == 0 {
		return domain.AuctionState{}, false
	}
	if result.AuctionStatus == "" && result.CurrentPrice <= 0 && result.Version == 0 && result.EndTime.IsZero() && result.LeaderBidderID == "" {
		return domain.AuctionState{}, false
	}
	state := domain.AuctionState{
		AuctionID:      id,
		LiveSessionID:  result.LiveSessionID,
		Status:         result.AuctionStatus,
		CurrentPrice:   result.CurrentPrice,
		LeaderBidderID: result.LeaderBidderID,
		EndTime:        result.EndTime,
		ExtendCount:    result.ExtendCount,
		Version:        result.Version,
	}
	return state, true
}

func (s *BidService) storeBidRealtimeStateFromResult(auctionID uint64, result domain.BidResult, expiresAt time.Time) {
	state, ok := bidRealtimeStateFromResult(auctionID, result)
	if !ok {
		return
	}
	s.storeBidRealtimeState(state, expiresAt)
}

func bidAuctionSnapshotFromLot(auction domain.AuctionLot) bidAuctionSnapshot {
	snapshot := bidAuctionSnapshot{
		AuctionID:      auction.AuctionID,
		SellerID:       auction.SellerID,
		StartPrice:     auction.StartPrice,
		CapPrice:       auction.CapPrice,
		IncrementRule:  append(json.RawMessage(nil), auction.IncrementRule...),
		AntiSnipingSec: auction.AntiSnipingSec,
		AntiExtendSec:  auction.AntiExtendSec,
		AntiExtendMode: domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		Status:         auction.Status,
		StartTime:      auction.StartTime,
		EndTime:        auction.EndTime,
	}
	if auction.LiveSessionID != nil {
		snapshot.LiveSessionID = *auction.LiveSessionID
	}
	return snapshot
}

func (a bidAuctionSnapshot) liveSessionPtr() *uint64 {
	if a.LiveSessionID == 0 {
		return nil
	}
	id := a.LiveSessionID
	return &id
}

func bidStreamEnabled(realtime repository.AuctionRealtimeStore) bool {
	type streamStore interface{ StreamEnabled() bool }
	store, ok := realtime.(streamStore)
	return ok && store.StreamEnabled()
}

func (s *BidService) observeBidStage(stage, result string, start time.Time) {
	if s == nil || s.metrics == nil || start.IsZero() {
		return
	}
	s.metrics.ObserveBidStage(stage, result, time.Since(start))
}

func bidStageResult(result domain.BidResult) string {
	if result.Duplicate {
		return "duplicate"
	}
	if result.Accepted {
		return "accepted"
	}
	reason := strings.TrimSpace(result.Reason)
	if reason != "" {
		return reason
	}
	return "rejected"
}

const blacklistStrategyCacheTTL = time.Second

func (s *BidService) currentRiskControl(ctx context.Context) domain.RiskControlConfig {
	if s.controls == nil {
		return domain.DefaultRiskControlConfig()
	}
	return s.controls.Config(ctx)
}

func (s *BidService) currentBlacklistStrategy(ctx context.Context) domain.BlacklistStrategyConfig {
	now := time.Now()
	s.blacklistStrategyMu.RLock()
	if s.blacklistStrategyCached && now.Before(s.blacklistStrategyExpiresAt) {
		cfg := s.blacklistStrategy
		s.blacklistStrategyMu.RUnlock()
		return cfg
	}
	s.blacklistStrategyMu.RUnlock()

	cfg, err := readBlacklistStrategyConfig(ctx, s.configs)
	if err != nil {
		fallback := domain.DefaultBlacklistStrategyConfig()
		s.blacklistStrategyMu.RLock()
		if s.blacklistStrategyCached {
			fallback = s.blacklistStrategy
		}
		s.blacklistStrategyMu.RUnlock()
		s.blacklistStrategyMu.Lock()
		s.blacklistStrategy = fallback
		s.blacklistStrategyCached = true
		s.blacklistStrategyExpiresAt = now.Add(blacklistStrategyCacheTTL)
		s.blacklistStrategyMu.Unlock()
		return fallback
	}
	s.blacklistStrategyMu.Lock()
	s.blacklistStrategy = cfg
	s.blacklistStrategyCached = true
	s.blacklistStrategyExpiresAt = now.Add(blacklistStrategyCacheTTL)
	s.blacklistStrategyMu.Unlock()
	return cfg
}

func (s *BidService) scheduleAutoBlacklist(ctx context.Context, cfg domain.BlacklistStrategyConfig, bidderID string, auctionID uint64, reason string, payload interface{}) {
	if s.risk == nil || !cfg.Enabled {
		return
	}
	base := context.WithoutCancel(ctx)
	go func() {
		taskCtx, cancel := context.WithTimeout(base, 2*time.Second)
		defer cancel()
		if err := s.autoBlacklistBidder(taskCtx, cfg, bidderID, auctionID, reason, payload); err != nil {
			slog.Default().Warn("auto blacklist failed", "auction_id", auctionID, "bidder_id", bidderID, "reason", reason, "error", err)
		}
	}()
}

func (s *BidService) autoBlacklistBidder(ctx context.Context, cfg domain.BlacklistStrategyConfig, bidderID string, auctionID uint64, reason string, payload interface{}) error {
	if s.risk == nil || !cfg.Enabled {
		return nil
	}
	expiresAt := blacklistExpiresAt(cfg, time.Now().UTC())
	if err := s.risk.AddBlacklist(ctx, bidderID, reason, systemBlacklistActorID, expiresAt); err != nil {
		return err
	}
	s.risk.RecordEvent(ctx, "AUTO_BLACKLIST", bidderID, auctionID, domain.RiskSeverityHigh, payload)
	return nil
}

func isAutoBlacklistPriceReason(reason string) bool {
	switch reason {
	case domain.BidRejectAboveMaxBidSteps, domain.BidRejectAboveExpectedMaxBidSteps, domain.BidRejectAboveCapPrice:
		return true
	default:
		return false
	}
}

func bidAboveAllowedMax(startPrice, currentPrice, capPrice, price int64, rule domain.IncrementRule) bool {
	if price <= startPrice || rule.MaxBidSteps <= 0 {
		return false
	}
	amount := rule.AmountForPrice(currentPrice)
	if amount <= 0 {
		return false
	}
	maxAllowed := currentPrice + amount*int64(rule.MaxBidSteps)
	if capPrice > 0 && maxAllowed > capPrice {
		maxAllowed = capPrice
	}
	return price > maxAllowed
}

func rejectBidFromState(in PlaceBidInput, state domain.AuctionState, reason string) domain.BidResult {
	return domain.BidResult{
		RequestID:      in.RequestID,
		AuctionID:      in.AuctionID,
		LiveSessionID:  state.LiveSessionID,
		BidderID:       in.BidderID,
		Price:          in.Price,
		Accepted:       false,
		Reason:         reason,
		CurrentPrice:   state.CurrentPrice,
		LeaderBidderID: state.LeaderBidderID,
		EndTime:        state.EndTime,
		ExtendCount:    state.ExtendCount,
		Version:        state.Version,
		Event:          "bid.rejected",
		RiskResult:     domain.BidRiskReject,
		AuctionStatus:  state.Status,
	}
}

// snapshotFloorPreRejectReason 在 EVALSHA 之前，基于"快照 current_price 对应的最小合法价"
// 做保守预拒。利用真实 current_price 单调上涨的不变量：若 price 连快照价对应的 floor 都不满足，
// Lua 必然也会拒。仅在以下"安全条件全部满足"时启用，任一不满足都返回 (,false) 放行进 Lua：
//   - 快照命中 (stateOK) 且 state.AuctionID 非零；
//   - 拍卖处于 RUNNING / EXTENDED；
//   - 加价规则为 fixed（ladder 第一版不预拒）；
//   - StartPrice / 加价 amount 字段可信（>0）。
//
// 仅返回 BELOW_MIN_INCREMENT / BELOW_START_PRICE 这一类"价格不够"的拒因，绝不替代
// NOT_ENROLLED / DEPOSIT_NOT_READY / FREQ_LIMIT 等需要权威 RT 状态的判定。
func snapshotFloorPreRejectReason(in PlaceBidInput, state domain.AuctionState, stateOK bool, auction bidAuctionSnapshot, rule domain.IncrementRule) (string, bool) {
	if !stateOK || state.AuctionID == 0 {
		return "", false
	}
	if state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended {
		return "", false
	}
	if rule.Type != domain.IncrementRuleTypeFixed || rule.Amount <= 0 {
		return "", false
	}
	startPrice := auction.StartPrice
	if startPrice <= 0 {
		return "", false
	}
	currentPrice := state.CurrentPrice
	if currentPrice <= 0 {
		return "", false
	}
	if in.Price <= startPrice {
		return domain.BidRejectBelowStartPrice, true
	}
	capPrice := auction.CapPrice
	if capPrice > 0 && in.Price == capPrice && in.Price > currentPrice {
		return "", false
	}
	// 仅在 price 与 amount 步长对齐时预拒，否则交给 Lua 区分 STEP_MISMATCH。
	if (in.Price-currentPrice)%rule.Amount != 0 {
		return "", false
	}
	var floor int64
	if currentPrice <= startPrice {
		floor = startPrice + rule.Amount
	} else {
		floor = currentPrice + rule.Amount
	}
	if in.Price < floor {
		return domain.BidRejectBelowMinIncrement, true
	}
	return "", false
}

func (s *BidService) persistBid(ctx context.Context, in PlaceBidInput, result domain.BidResult, now time.Time) {
	if s.bids == nil || !result.Accepted {
		return
	}
	var sessionID *uint64
	if result.LiveSessionID != 0 {
		id := result.LiveSessionID
		sessionID = &id
	} else if s.auctions != nil {
		if lot, err := s.auctions.FindByID(ctx, in.AuctionID); err == nil && lot.LiveSessionID != nil {
			id := *lot.LiveSessionID
			sessionID = &id
		}
	}
	record := domain.BidRecord{
		RequestID:     in.RequestID,
		AuctionID:     in.AuctionID,
		LiveSessionID: sessionID,
		BidderID:      in.BidderID,
		BidPrice:      in.Price,
		BidTSMS:       now.UnixMilli(),
		Source:        in.Source,
		RiskResult:    result.RiskResult,
		RejectReason:  result.Reason,
		CreatedAt:     now,
	}
	if result.Accepted {
		record.RiskResult = domain.BidRiskAllow
		record.RejectReason = ""
	}
	if err := s.bids.Create(ctx, &record); err != nil && !errors.Is(err, domain.ErrConflict) {
		return
	}
	// 仅在出价被接受时累加场次的 bid_count（拒绝/重复不计）。
	if result.Accepted && sessionID != nil && s.sessions != nil {
		_ = s.sessions.IncrCounters(ctx, *sessionID, domain.LiveSessionCounters{BidCountDelta: 1})
	}
}

func (s *BidService) publishBidResult(ctx context.Context, result domain.BidResult) {
	if result.Accepted {
		broadcastJSONWithSeq(s.publisher, result.AuctionID, "bid.accepted", result.Seq, result)
		s.scheduleRankingBroadcast(result.AuctionID)
		if result.Extended {
			broadcastJSON(s.publisher, result.AuctionID, "timer.extended", map[string]interface{}{
				"auctionId":   result.AuctionID,
				"endTime":     result.EndTime,
				"extendCount": result.ExtendCount,
			})
		}
		return
	}
}

func (s *BidService) scheduleRankingBroadcast(auctionID uint64) {
	if s == nil || auctionID == 0 {
		return
	}
	s.rankingBroadcastMu.Lock()
	defer s.rankingBroadcastMu.Unlock()
	if _, ok := s.rankingBroadcastTimers[auctionID]; ok {
		return
	}
	s.rankingBroadcastTimers[auctionID] = time.AfterFunc(bidRankingBroadcastDelay, func() {
		s.flushRankingBroadcast(auctionID)
	})
}

func (s *BidService) flushRankingBroadcast(auctionID uint64) {
	if s == nil || auctionID == 0 {
		return
	}
	s.rankingBroadcastMu.Lock()
	delete(s.rankingBroadcastTimers, auctionID)
	s.rankingBroadcastMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ranking, err := s.TopN(ctx, auctionID, 10)
	if err != nil {
		return
	}
	ranking = s.enrichRanking(ctx, ranking)
	broadcastJSON(s.publisher, auctionID, "ranking.updated", map[string]interface{}{
		"auctionId": auctionID,
		"ranking":   ranking,
	})
}

func (s *BidService) enrichBidResult(ctx context.Context, result *domain.BidResult, liveSessionID *uint64) {
	if result == nil {
		return
	}
	if result.LiveSessionID == 0 && liveSessionID != nil {
		result.LiveSessionID = *liveSessionID
	}
	if result.Accepted && result.BidderNickname == "" {
		result.BidderNickname = s.bidderNickname(ctx, result.BidderID)
	}
}

func (s *BidService) enrichRanking(ctx context.Context, ranking []domain.RankingEntry) []domain.RankingEntry {
	if len(ranking) == 0 || s.users == nil {
		return ranking
	}
	out := make([]domain.RankingEntry, len(ranking))
	copy(out, ranking)
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
		nickname := s.bidderNickname(ctx, id)
		cache[id] = nickname
		out[i].BidderNickname = nickname
	}
	return out
}

func (s *BidService) bidderNickname(ctx context.Context, userID string) string {
	if s == nil || s.users == nil || strings.TrimSpace(userID) == "" {
		return ""
	}
	userID = strings.TrimSpace(userID)
	if nickname, ok := s.cachedBidderNickname(userID, time.Now()); ok {
		return nickname
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return ""
	}
	nickname := strings.TrimSpace(user.Nickname)
	s.storeBidderNickname(userID, nickname, time.Now().Add(bidNicknameCacheTTL))
	return nickname
}

func (s *BidService) cachedBidderNickname(userID string, now time.Time) (string, bool) {
	s.nicknameCacheMu.RLock()
	defer s.nicknameCacheMu.RUnlock()
	if s.nicknameCache == nil {
		return "", false
	}
	cached, ok := s.nicknameCache[userID]
	if !ok || now.After(cached.expiresAt) {
		return "", false
	}
	return cached.nickname, true
}

func (s *BidService) storeBidderNickname(userID, nickname string, expiresAt time.Time) {
	s.nicknameCacheMu.Lock()
	defer s.nicknameCacheMu.Unlock()
	if s.nicknameCache == nil {
		s.nicknameCache = make(map[string]cachedBidderNickname)
	}
	s.nicknameCache[userID] = cachedBidderNickname{nickname: nickname, expiresAt: expiresAt}
}

func bidResultFromRecord(record domain.BidRecord) domain.BidResult {
	accepted := record.RejectReason == ""
	result := domain.BidResult{
		RequestID:      record.RequestID,
		AuctionID:      record.AuctionID,
		BidderID:       record.BidderID,
		BidderNickname: record.BidderNickname,
		Price:          record.BidPrice,
		Accepted:       accepted,
		Duplicate:      true,
		Reason:         record.RejectReason,
		RiskResult:     record.RiskResult,
	}
	if accepted {
		result.CurrentPrice = record.BidPrice
		result.Event = "bid.accepted"
	} else {
		result.Event = "bid.rejected"
	}
	return result
}
