package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	adminports "aieas_backend/internal/modules/admin/ports"
	auctionports "aieas_backend/internal/modules/auction/ports"

	"golang.org/x/sync/singleflight"
)

const bidAuctionCacheTTL = 2 * time.Second
const bidRealtimeStateCacheTTL = 200 * time.Millisecond
const bidPrerequisiteCacheTTL = 30 * time.Second
const bidNicknameCacheTTL = 5 * time.Minute
const defaultBidIdempotencyTTL = 30 * time.Second
const defaultBidLiveAgentHookQuiet = 5 * time.Second
const bidRankingBroadcastDelay = 200 * time.Millisecond
const SamePriceInflightLimit = samePriceInflightLimit
const SamePriceGateIdleTTL = samePriceGateIdleTTL
const BidRealtimeStateCacheTTL = bidRealtimeStateCacheTTL

const (
	samePriceInflightLimit     int32 = 2
	samePriceGateIdleTTL             = 30 * time.Second
	samePriceGateSweepInterval       = 10 * time.Second
)

type BidRiskService interface {
	IsBlacklisted(ctx context.Context, userID string) (bool, error)
	AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error
	RecordEvent(ctx context.Context, eventType string, userID string, auctionID uint64, severity domain.RiskSeverity, payload interface{})
}

type BidHammer interface {
	Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error)
}

type BidSessionCounterWriter interface {
	IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error
}

type BidLiveAgentHook interface {
	EmitHighestBidWithBidderName(ctx context.Context, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64)
}

type BidLiveAgentHookImmediate interface {
	EmitHighestBidWithBidderNameNow(ctx context.Context, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64)
}

type BidRiskControlProvider interface {
	Config(ctx context.Context) domain.RiskControlConfig
}

type pendingBidLiveAgentHook struct {
	merchantID string
	sessionID  uint64
	bidderID   string
	bidderName string
	price      int64
	generation uint64
	timer      *time.Timer
}

type BidService struct {
	bids      auctionports.BidRepository
	auctions  auctionports.AuctionRepository
	realtime  auctionports.AuctionRealtimeStore
	risk      BidRiskService
	hammer    BidHammer
	publisher EventPublisher
	sessions  BidSessionCounterWriter
	cfg       appconfig.AuctionConfig
	metrics   AuctionMetrics
	tracer    AuctionTracer
	hook      BidLiveAgentHook
	configs   adminports.ConfigRepository
	controls  BidRiskControlProvider
	users     BidUserReader

	auctionSnapshots AuctionSnapshotCache

	auctionCacheMu  sync.RWMutex
	auctionCache    map[uint64]cachedBidAuction
	auctionCacheTTL time.Duration
	auctionGroup    singleflight.Group

	realtimeStateCache sync.Map // map[uint64]*bidRealtimeStateCell
	prerequisiteCache  sync.Map // map[string]bidPrerequisiteCacheEntry

	profileCacheMu sync.RWMutex
	profileCache   map[string]cachedBidderProfile

	rankingBroadcastMu     sync.Mutex
	rankingBroadcastTimers map[uint64]*time.Timer

	liveAgentHookMu          sync.Mutex
	liveAgentHookQuietPeriod time.Duration
	pendingLiveAgentHooks    map[string]*pendingBidLiveAgentHook

	samePriceGateMu        sync.Mutex
	samePriceInflight      map[string]*samePriceGateEntry
	samePriceGateNextSweep time.Time

	blacklistStrategyMu        sync.RWMutex
	blacklistStrategyCached    bool
	blacklistStrategy          domain.BlacklistStrategyConfig
	blacklistStrategyExpiresAt time.Time
}

type BidServiceDeps struct {
	Bids             auctionports.BidRepository
	Auctions         auctionports.AuctionRepository
	Realtime         auctionports.AuctionRealtimeStore
	Risk             BidRiskService
	Publisher        EventPublisher
	Hammer           BidHammer
	Sessions         BidSessionCounterWriter
	Config           appconfig.AuctionConfig
	Metrics          AuctionMetrics
	Tracer           AuctionTracer
	LiveAgentHook    BidLiveAgentHook
	Configs          adminports.ConfigRepository
	RiskControls     BidRiskControlProvider
	Users            BidUserReader
	AuctionSnapshots AuctionSnapshotCache
}

type BidUserReader interface {
	FindByID(id string) (domain.User, error)
}

type AuctionMetrics interface {
	ObserveBid(result, reason string, duration time.Duration)
	IncBidDuplicate()
	IncBidReject(reason string)
	IncBidFreqLimit()
	ObserveBidStage(stage, result string, duration time.Duration)
	IncBidRoute(decision, reason string)
	ObserveHammer(result string, duration time.Duration)
	IncHammerDuplicate()
	ObserveHammerMySQLTx(duration time.Duration)
	IncHammerMySQLFail()
	IncHammerOptimisticConflict()
}

func NewBidService(bids auctionports.BidRepository, auctions auctionports.AuctionRepository, realtime auctionports.AuctionRealtimeStore, risk BidRiskService, publisher EventPublisher, cfg appconfig.AuctionConfig) *BidService {
	return NewBidServiceWithDeps(BidServiceDeps{Bids: bids, Auctions: auctions, Realtime: realtime, Risk: risk, Publisher: publisher, Config: cfg})
}

func NewBidServiceWithDeps(deps BidServiceDeps) *BidService {
	realtime := deps.Realtime
	if realtime == nil {
		realtime = noopRealtimeStore{}
	}
	risk := deps.Risk
	if isNilInterface(risk) {
		risk = nil
	}
	hammer := deps.Hammer
	if isNilInterface(hammer) {
		hammer = nil
	}
	publisher := deps.Publisher
	if isNilInterface(publisher) {
		publisher = nil
	}
	sessions := deps.Sessions
	if isNilInterface(sessions) {
		sessions = nil
	}
	hook := deps.LiveAgentHook
	if isNilInterface(hook) {
		hook = nil
	}
	controls := deps.RiskControls
	if isNilInterface(controls) {
		controls = nil
	}
	cfg := deps.Config
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
		bids:                     deps.Bids,
		auctions:                 deps.Auctions,
		realtime:                 realtime,
		risk:                     risk,
		hammer:                   hammer,
		publisher:                publisher,
		sessions:                 sessions,
		cfg:                      cfg,
		metrics:                  deps.Metrics,
		tracer:                   deps.Tracer,
		hook:                     hook,
		configs:                  deps.Configs,
		controls:                 controls,
		users:                    deps.Users,
		auctionSnapshots:         deps.AuctionSnapshots,
		auctionCache:             make(map[uint64]cachedBidAuction),
		auctionCacheTTL:          bidAuctionCacheTTL,
		profileCache:             make(map[string]cachedBidderProfile),
		rankingBroadcastTimers:   make(map[uint64]*time.Timer),
		liveAgentHookQuietPeriod: defaultBidLiveAgentHookQuiet,
		pendingLiveAgentHooks:    make(map[string]*pendingBidLiveAgentHook),
	}
}

// SetLiveSessionService 仅保留给测试替换直播场次服务；业务装配应通过 BidServiceDeps.Sessions 注入。
func (s *BidService) SetLiveSessionService(sessions BidSessionCounterWriter) {
	if isNilInterface(sessions) {
		s.sessions = nil
		return
	}
	s.sessions = sessions
}

// SetHammerService 仅保留给测试替换落槌服务；业务装配应通过 BidServiceDeps.Hammer 注入。
func (s *BidService) SetHammerService(hammer BidHammer) {
	if isNilInterface(hammer) {
		s.hammer = nil
		return
	}
	s.hammer = hammer
}

// SetLiveAgentHookService 仅保留给测试替换直播拍卖事件 hook；业务装配应通过 BidServiceDeps.LiveAgentHook 注入。
func (s *BidService) SetLiveAgentHookService(hook BidLiveAgentHook) {
	if isNilInterface(hook) {
		s.hook = nil
		return
	}
	s.hook = hook
}

// SetLiveAgentHookQuietPeriod 仅保留给测试缩短最高出价 hook 静默窗口；<=0 表示立即发送。
func (s *BidService) SetLiveAgentHookQuietPeriod(period time.Duration) {
	if s == nil {
		return
	}
	s.liveAgentHookMu.Lock()
	defer s.liveAgentHookMu.Unlock()
	s.liveAgentHookQuietPeriod = period
}

// SetConfigRepository 仅保留给测试替换动态配置仓储；业务装配应通过 BidServiceDeps.Configs 注入。
func (s *BidService) SetConfigRepository(configs adminports.ConfigRepository) {
	s.configs = configs
}

// SetRiskControlService 仅保留给测试替换风控运行时开关；业务装配应通过 BidServiceDeps.RiskControls 注入。
func (s *BidService) SetRiskControlService(controls BidRiskControlProvider) {
	if isNilInterface(controls) {
		s.controls = nil
		return
	}
	s.controls = controls
}

func (s *BidService) SetRiskService(risk BidRiskService) {
	if isNilInterface(risk) {
		s.risk = nil
		return
	}
	s.risk = risk
}

func (s *BidService) AuctionRepository() auctionports.AuctionRepository {
	if s == nil {
		return nil
	}
	return s.auctions
}

func (s *BidService) RealtimeStore() auctionports.AuctionRealtimeStore {
	if s == nil {
		return nil
	}
	return s.realtime
}

func (s *BidService) CachedBidRealtimeStateForTest(auctionID uint64, now time.Time) (domain.AuctionState, bool) {
	return s.cachedBidRealtimeState(auctionID, now)
}

func (s *BidService) StoreBidRealtimeStateForTest(state domain.AuctionState, expiresAt time.Time) {
	s.storeBidRealtimeState(state, expiresAt)
}

func (s *BidService) SamePriceGateCounterForTest(key string) *atomic.Int32 {
	return s.samePriceGateCounter(key)
}

func (s *BidService) SamePriceGateExistsForTest(key string) bool {
	return s.samePriceGateExists(key)
}

func (s *BidService) SweepSamePriceGateForTest(auctionID uint64, lastUsed, now time.Time) {
	if s == nil || auctionID == 0 {
		return
	}
	s.samePriceGateMu.Lock()
	defer s.samePriceGateMu.Unlock()
	if entry := s.samePriceInflight[samePriceGateAuctionKey(auctionID)]; entry != nil {
		entry.lastUsed = lastUsed
	}
	s.samePriceGateNextSweep = time.Time{}
	s.sweepSamePriceGateLocked(now)
}

// SetUserRepository 仅保留给测试替换用户仓储；业务装配应通过 BidServiceDeps.Users 注入。
func (s *BidService) SetUserRepository(users BidUserReader) {
	s.users = users
}

// SetAuctionSnapshotCache 仅保留给测试替换拍品运行快照缓存；业务装配应通过 BidServiceDeps.AuctionSnapshots 注入。
func (s *BidService) SetAuctionSnapshotCache(cache AuctionSnapshotCache) {
	s.auctionSnapshots = cache
}

// SetMetrics 仅保留给测试替换观测性 Registry。nil 安全：未注入时所有 Observe* 调用走 noop。
// 业务装配应通过 BidServiceDeps.Metrics 注入。
func (s *BidService) SetMetrics(reg AuctionMetrics) {
	s.metrics = reg
}

func (s *BidService) Place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	ctx, span := startAuctionSpan(ctx, s.tracer, "bid.place",
		Int64Attr("auction.id", int64(in.AuctionID)),
		StringAttr("bid.request_id", in.RequestID),
		StringAttr("bid.source", in.Source),
		Int64Attr("bid.price", in.Price),
	)
	defer span.End()
	start := time.Now()
	result, err := s.place(ctx, in)
	elapsed := time.Since(start)
	span.SetAttributes(
		BoolAttr("bid.accepted", result.Accepted),
		BoolAttr("bid.duplicate", result.Duplicate),
		StringAttr("bid.reject_reason", result.Reason),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(AuctionStatusError, err.Error())
	} else if !result.Accepted && !result.Duplicate {
		span.SetStatus(AuctionStatusError, result.Reason)
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

// bidCheckSnapshot 是 preCheck 的"冻结快照"：携带入队前全部校验的产物，
// 以及 arbitrate 执行 Lua PlaceBid 及其后续后处理所需的全部上下文。
// 同步路径下 preCheck 直接把它传给 arbitrate；异步路径下它被序列化成
// BidCommandSnapshot 投递到 Kafka，worker 侧再经 ArbitrateFromCommand 重建。
type bidCheckSnapshot struct {
	in                 PlaceBidInput
	auction            bidAuctionSnapshot
	liveSessionID      uint64
	now                time.Time
	rule               domain.IncrementRule
	minIncrement       int64
	freqLimitCount     int
	freqWindowMS       int64
	bidderNickname     string
	bidderAvatarURL    string
	riskControlEnabled bool
	blacklistStrategy  domain.BlacklistStrategyConfig
	streamEnabled      bool
	gateRelease        func()
}

// BidCommandSnapshot 是异步竞价的可序列化命令快照：由 PreCheckForAsync 产出、
// 经 Kafka 投递、再由 ArbitrateFromCommand 在 worker 侧消费。
// 它只携带 Lua PlaceBid 与后处理所需的"已冻结"字段，绝不携带闭包等不可序列化状态。
type BidCommandSnapshot struct {
	BidID                string                   `json:"bidId"`
	AuctionID            uint64                   `json:"auctionId"`
	LiveSessionID        uint64                   `json:"liveSessionId"`
	UserID               string                   `json:"userId"`
	SellerID             string                   `json:"sellerId"`
	Price                int64                    `json:"price"`
	ExpectedCurrentPrice *int64                   `json:"expectedCurrentPrice"`
	Source               string                   `json:"source"`
	MinIncrement         int64                    `json:"minIncrement"`
	AntiSnipingMS        int64                    `json:"antiSnipingMs"`
	AntiExtendMS         int64                    `json:"antiExtendMs"`
	AntiExtendMode       domain.AuctionExtendMode `json:"antiExtendMode"`
	MaxExtendCount       int                      `json:"maxExtendCount"`
	FreqLimitCount       int                      `json:"freqLimitCount"`
	FreqWindowMS         int64                    `json:"freqWindowMs"`
	StartPrice           int64                    `json:"startPrice"`
	CapPrice             int64                    `json:"capPrice"`
	IncrementRule        domain.IncrementRule     `json:"incrementRule"`
	BidderNickname       string                   `json:"bidderNickname"`
	BidderAvatarURL      string                   `json:"bidderAvatarUrl"`
}

func (s *BidService) place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	snap, terminal, err := s.preCheck(ctx, in)
	if err != nil {
		return domain.BidResult{}, err
	}
	if terminal != nil {
		return *terminal, nil
	}
	return s.arbitrate(ctx, snap)
}

// PreCheckForAsync 执行入队前全部同步校验，返回可序列化的命令快照。
// 当 terminal 非 nil 时表示已在前置阶段裁定（拒绝/重复/本地预拒/闸门拒绝），
// 调用方应直接回传该结果而不入队。snapshot 仅在 terminal==nil 时有效。
//
// 注意：异步路径不跨进程持有同价闸门 slot（worker 顺序消费已天然串行），
// 因此这里在 preCheck 之后立即释放闸门，避免占用 in-flight 名额。
func (s *BidService) PreCheckForAsync(ctx context.Context, in PlaceBidInput) (BidCommandSnapshot, *domain.BidResult, error) {
	snap, terminal, err := s.preCheck(ctx, in)
	if snap.gateRelease != nil {
		snap.gateRelease()
		snap.gateRelease = nil
	}
	if err != nil {
		return BidCommandSnapshot{}, nil, err
	}
	if terminal != nil {
		return BidCommandSnapshot{}, terminal, nil
	}
	return BidCommandSnapshot{
		BidID:                snap.in.RequestID,
		AuctionID:            snap.in.AuctionID,
		LiveSessionID:        snap.liveSessionID,
		UserID:               snap.in.BidderID,
		SellerID:             snap.auction.SellerID,
		Price:                snap.in.Price,
		ExpectedCurrentPrice: snap.in.ExpectedCurrentPrice,
		Source:               snap.in.Source,
		MinIncrement:         snap.minIncrement,
		AntiSnipingMS:        int64(snap.auction.AntiSnipingSec) * 1000,
		AntiExtendMS:         int64(snap.auction.AntiExtendSec) * 1000,
		AntiExtendMode:       domain.NormalizeAuctionExtendMode(snap.auction.AntiExtendMode),
		MaxExtendCount:       s.cfg.MaxExtendCount,
		FreqLimitCount:       snap.freqLimitCount,
		FreqWindowMS:         snap.freqWindowMS,
		StartPrice:           snap.auction.StartPrice,
		CapPrice:             snap.auction.CapPrice,
		IncrementRule:        snap.rule,
		BidderNickname:       snap.bidderNickname,
		BidderAvatarURL:      snap.bidderAvatarURL,
	}, nil, nil
}

// ArbitrateFromCommand 在 worker 侧复用原 Lua PlaceBid 链路执行裁决。
// 它基于命令快照重建 arbitrate 所需上下文（拍品快照由命令字段合成，
// 不再回源 DB；风控开关/黑名单策略走进程内缓存读取），保证与同步路径
// 完全一致的落库/广播/自动黑名单/hook 后处理。
func (s *BidService) ArbitrateFromCommand(ctx context.Context, cmd BidCommandSnapshot) (domain.BidResult, error) {
	if cmd.BidID == "" || cmd.AuctionID == 0 || cmd.UserID == "" {
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	source := cmd.Source
	if source == "" {
		source = "live_ws"
	}
	// 异步路径下 cmd 携带的是 ms 字段（来自 PreCheckForAsync 的 sec*1000），但
	// arbitrate 在向 Lua 传 ARGV 时统一从 bidAuctionSnapshot.AntiSnipingSec*1000 /
	// AntiExtendSec*1000 计算；这里把 ms 反算回 sec 写入 snapshot，避免 Lua 收到
	// 0 导致 anti_snipe_ms>0 && extend_ms>0 永远 false（不延长 endTime）。
	// PreCheckForAsync 写入的 ms 已是 sec*1000，整除可精确还原。
	auction := bidAuctionSnapshot{
		AuctionID:      cmd.AuctionID,
		SellerID:       cmd.SellerID,
		LiveSessionID:  cmd.LiveSessionID,
		StartPrice:     cmd.StartPrice,
		CapPrice:       cmd.CapPrice,
		ParsedRule:     cmd.IncrementRule,
		ParsedRuleOK:   true,
		AntiSnipingSec: int(cmd.AntiSnipingMS / 1000),
		AntiExtendSec:  int(cmd.AntiExtendMS / 1000),
		AntiExtendMode: cmd.AntiExtendMode,
	}
	riskControl := s.currentRiskControl(ctx)
	blacklistStrategy := domain.BlacklistStrategyConfig{}
	if riskControl.Enabled {
		blacklistStrategy = s.currentBlacklistStrategy(ctx)
	}
	snap := bidCheckSnapshot{
		in: PlaceBidInput{
			RequestID:            cmd.BidID,
			AuctionID:            cmd.AuctionID,
			BidderID:             cmd.UserID,
			UserRole:             domain.RoleBuyer,
			Price:                cmd.Price,
			ExpectedCurrentPrice: cmd.ExpectedCurrentPrice,
			Source:               source,
		},
		auction:            auction,
		liveSessionID:      cmd.LiveSessionID,
		now:                time.Now().UTC(),
		rule:               cmd.IncrementRule,
		minIncrement:       cmd.MinIncrement,
		freqLimitCount:     cmd.FreqLimitCount,
		freqWindowMS:       cmd.FreqWindowMS,
		bidderNickname:     cmd.BidderNickname,
		bidderAvatarURL:    cmd.BidderAvatarURL,
		riskControlEnabled: riskControl.Enabled,
		blacklistStrategy:  blacklistStrategy,
		streamEnabled:      bidStreamEnabled(s.realtime),
	}
	return s.arbitrate(ctx, snap)
}

func (s *BidService) preCheck(ctx context.Context, in PlaceBidInput) (bidCheckSnapshot, *domain.BidResult, error) {
	stageStart := time.Now()
	in.RequestID = strings.TrimSpace(in.RequestID)
	in.BidderID = strings.TrimSpace(in.BidderID)
	if in.RequestID == "" || in.AuctionID == 0 || in.BidderID == "" || in.Price <= 0 || in.UserRole != domain.RoleBuyer || in.ExpectedCurrentPrice == nil {
		s.observeBidStage("input_validate", "error", stageStart)
		return bidCheckSnapshot{}, nil, domain.ErrInvalidArgument
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
			result := bidResultFromRecord(record)
			return bidCheckSnapshot{}, &result, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			s.observeBidStage("mysql_idempotency", "error", stageStart)
			return bidCheckSnapshot{}, nil, err
		}
		s.observeBidStage("mysql_idempotency", "miss", stageStart)
	}
	stageStart = time.Now()
	auction, err := s.bidAuctionSnapshot(ctx, in.AuctionID)
	if err != nil {
		s.observeBidStage("auction_snapshot", "error", stageStart)
		return bidCheckSnapshot{}, nil, err
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
			return bidCheckSnapshot{}, nil, err
		}
		if isBlacklisted {
			s.observeBidStage("blacklist_lookup", "hit", stageStart)
			s.observeBidRoute("go_blacklist_reject", "BLACKLIST")
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
			return bidCheckSnapshot{}, &result, nil
		}
		s.observeBidStage("blacklist_lookup", "miss", stageStart)
	}
	if result, ok := s.preRedisLocalReject(ctx, in); ok {
		return bidCheckSnapshot{}, &result, nil
	}
	if !riskControlEnabled {
		s.observeBidStage("bid_prerequisites", "disabled", time.Now())
	} else if result, ok, err := s.preRedisPrerequisiteReject(ctx, in, auction); err != nil {
		return bidCheckSnapshot{}, nil, err
	} else if ok {
		s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
		if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.MissingDepositEnabled && s.risk != nil {
			stageStart = time.Now()
			s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_"+result.Reason, result)
			s.observeBidStage("schedule_auto_blacklist", "missing_deposit", stageStart)
		}
		return bidCheckSnapshot{}, &result, nil
	}
	stageStart = time.Now()
	var rule domain.IncrementRule
	if auction.ParsedRuleOK {
		rule = auction.ParsedRule
		s.observeBidStage("increment_rule", "cache_hit", stageStart)
	} else {
		parsed, err := domain.ParseIncrementRule(auction.IncrementRule)
		if err != nil {
			s.observeBidStage("increment_rule", "error", stageStart)
			return bidCheckSnapshot{}, nil, domain.ErrInvalidArgument
		}
		rule = parsed
		s.observeBidStage("increment_rule", "ok", stageStart)
	}
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
	bidderProfile := s.bidderProfile(ctx, in.BidderID)
	if bidderProfile.nickname == "" {
		s.observeBidStage("bidder_nickname", "empty", stageStart)
	} else {
		s.observeBidStage("bidder_nickname", "ok", stageStart)
	}
	gateRelease, gateRejected, gateResult := s.acquireSamePriceGate(ctx, in, auction)
	if gateRejected {
		return bidCheckSnapshot{}, &gateResult, nil
	}
	return bidCheckSnapshot{
		in:                 in,
		auction:            auction,
		liveSessionID:      liveSessionID,
		now:                now,
		rule:               rule,
		minIncrement:       minIncrement,
		freqLimitCount:     freqLimitCount,
		freqWindowMS:       freqWindowMS,
		bidderNickname:     bidderProfile.nickname,
		bidderAvatarURL:    bidderProfile.avatarURL,
		riskControlEnabled: riskControlEnabled,
		blacklistStrategy:  blacklistStrategy,
		streamEnabled:      streamEnabled,
		gateRelease:        gateRelease,
	}, nil, nil
}

// arbitrate 执行 Lua PlaceBid 及其后续后处理链路（落库 / 广播 / 自动黑名单 /
// hook / cap 落槌）。它消费 preCheck（或 ArbitrateFromCommand）产出的冻结快照，
// 行为与拆分前的 place 后半段完全一致。
func (s *BidService) arbitrate(ctx context.Context, snap bidCheckSnapshot) (domain.BidResult, error) {
	in := snap.in
	auction := snap.auction
	liveSessionID := snap.liveSessionID
	now := snap.now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rule := snap.rule
	riskControlEnabled := snap.riskControlEnabled
	blacklistStrategy := snap.blacklistStrategy
	streamEnabled := snap.streamEnabled
	gateRelease := snap.gateRelease
	if gateRelease != nil {
		defer func() {
			if gateRelease != nil {
				gateRelease()
			}
		}()
	}
	stageStart := time.Now()
	s.observeBidRoute("lua_enter", "attempt")
	result, err := s.realtime.PlaceBid(ctx, domain.BidInput{
		RequestID:            in.RequestID,
		AuctionID:            in.AuctionID,
		LiveSessionID:        liveSessionID,
		BidderID:             in.BidderID,
		BidderNickname:       snap.bidderNickname,
		BidderAvatarURL:      snap.bidderAvatarURL,
		Price:                in.Price,
		ExpectedCurrentPrice: in.ExpectedCurrentPrice,
		Now:                  now,
		Source:               in.Source,
		MinIncrement:         snap.minIncrement,
		AntiSnipingMS:        int64(auction.AntiSnipingSec) * 1000,
		AntiExtendMS:         int64(auction.AntiExtendSec) * 1000,
		AntiExtendMode:       domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		MaxExtendCount:       s.cfg.MaxExtendCount,
		FreqLimitCount:       snap.freqLimitCount,
		FreqWindowMS:         snap.freqWindowMS,
		IdempotencyTTL:       s.bidIdempotencyTTL(),
		StartPrice:           auction.StartPrice,
		CapPrice:             auction.CapPrice,
		IncrementRule:        rule,
	})
	if gateRelease != nil {
		gateRelease()
		gateRelease = nil
	}
	if err != nil {
		s.observeBidStage("realtime_place_bid", "error", stageStart)
		s.observeBidRoute("lua_error", "internal")
		return domain.BidResult{}, err
	}
	s.observeBidStage("realtime_place_bid", bidStageResult(result), stageStart)
	switch {
	case result.Duplicate:
		s.observeBidRoute("lua_duplicate", "duplicate")
	case result.Accepted:
		s.observeBidRoute("lua_accept", "ok")
	default:
		reason := strings.TrimSpace(result.Reason)
		if reason == "" {
			reason = "unknown"
		}
		s.observeBidRoute("lua_reject", reason)
	}
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
		s.scheduleHighestBidHook(auction.AuctionID, auction.SellerID, auction.LiveSessionID, result.BidderID, result.BidderNickname, result.CurrentPrice)
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
	ranking, err := s.realtime.TopN(ctx, auctionID, limit)
	if err != nil {
		return nil, err
	}
	return s.enrichRanking(ctx, ranking), nil
}

func isNilInterface(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func (s *BidService) bidIdempotencyTTL() time.Duration {
	if s == nil || s.cfg.BidIdempotencyTTL.Std() <= 0 {
		return defaultBidIdempotencyTTL
	}
	return s.cfg.BidIdempotencyTTL.Std()
}

// localBidState 是 Go 进程内基于 cachedBidRealtimeState + cachedBidAuction 合成的
// "保守状态"，用于 EVALSHA 之前的本地预拒。仅在两个 cache 都命中且字段可信时才生成；
// 任一字段缺失即视为不可用，调用方必须放行进 Lua。
type localBidState struct {
	auctionID      uint64
	liveSessionID  uint64
	status         domain.AuctionStatus
	currentPrice   int64
	leaderBidderID string
	endTime        time.Time
	extendCount    int
	version        int64
	startPrice     int64
	capPrice       int64
	rule           domain.IncrementRule
	auction        bidAuctionSnapshot
	state          domain.AuctionState
}

type samePriceGateEntry struct {
	priceCounts  map[int64]*samePriceGatePriceEntry
	highestPrice int64
	lastUsed     time.Time
}

type samePriceGatePriceEntry struct {
	count atomic.Int32
}

// buildLocalBidState 组合实时缓存与拍品快照成本地视图。返回 ok=false 表示安全门未通过，
// 调用方必须放行进 Lua。auction snapshot 命中时使用其预解析的 IncrementRule（避免热路径
// 重复 JSON 解析）；snapshot 未命中但 state 自带 IncrementRule 时退化为单源解析。
func (s *BidService) buildLocalBidState(auctionID uint64, now time.Time) (localBidState, bool) {
	if s == nil || auctionID == 0 {
		return localBidState{}, false
	}
	state, ok := s.cachedBidRealtimeState(auctionID, now)
	if !ok || state.AuctionID != auctionID {
		return localBidState{}, false
	}
	auction, auctionOK := s.cachedBidAuction(auctionID, now)
	var (
		rule          domain.IncrementRule
		ruleOK        bool
		startPrice    int64
		capPrice      int64
		liveSessionID uint64
	)
	if auctionOK {
		startPrice = auction.StartPrice
		capPrice = auction.CapPrice
		liveSessionID = auction.LiveSessionID
		if auction.ParsedRuleOK {
			rule = auction.ParsedRule
			ruleOK = true
		}
	}
	if startPrice <= 0 && state.StartPrice > 0 {
		startPrice = state.StartPrice
	}
	if capPrice == 0 && state.CapPrice > 0 {
		capPrice = state.CapPrice
	}
	if !ruleOK && len(state.IncrementRule) > 0 {
		if parsed, err := domain.ParseIncrementRule(state.IncrementRule); err == nil {
			rule = parsed
			ruleOK = true
		}
	}
	if state.LiveSessionID != 0 {
		liveSessionID = state.LiveSessionID
	}
	if !ruleOK || startPrice <= 0 {
		return localBidState{}, false
	}
	return localBidState{
		auctionID:      auctionID,
		liveSessionID:  liveSessionID,
		status:         state.Status,
		currentPrice:   state.CurrentPrice,
		leaderBidderID: state.LeaderBidderID,
		endTime:        state.EndTime,
		extendCount:    state.ExtendCount,
		version:        state.Version,
		startPrice:     startPrice,
		capPrice:       capPrice,
		rule:           rule,
		auction:        auction,
		state:          state,
	}, true
}

// preRedisLocalReject 是 Lua 之前唯一的本地预拒入口。基于 localBidState 做"必然拒"判断，
// 任一安全条件不满足则放行进 Lua（绝不在 Go 层做"接受"判断）。
//
// 仅在 fixed rule + auction RUNNING/EXTENDED + 两个 cache 都命中 + 字段非零 时启用，
// 覆盖以下拒因（基于单调性不变量：真实 current_price ≥ cached.currentPrice）：
//   - BELOW_START_PRICE：必然拒
//   - ABOVE_CAP_PRICE：必然拒
//   - ABOVE_EXPECTED_MAX_BID_STEPS：基于客户端传入 expected 计算，与真实 state 无关，必然拒
//   - PRICE_STEP_MISMATCH：基于 cached current 做模 amount 检查；真实 current 与 cached
//     差值必然是 amount 整数倍，模值不变，必然拒
//   - BELOW_MIN_INCREMENT：price < cached.currentPrice + step，真实 floor 只会更高，必然拒
//
// 不本地拦截 STALE_AUCTION_STATE（cached 可能落后）和 ABOVE_MAX_BID_STEPS（基于 cached
// 不安全）。
func (s *BidService) preRedisLocalReject(ctx context.Context, in PlaceBidInput) (domain.BidResult, bool) {
	stageStart := time.Now()
	if state, ok := s.cachedBidRealtimeState(in.AuctionID, stageStart); ok {
		if reason := conservativeLocalStateRejectReason(in, state); reason != "" {
			result := rejectBidFromState(in, state, reason)
			if auction, ok := s.cachedBidAuction(in.AuctionID, stageStart); ok {
				s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
			}
			s.observeBidStage("local_state_precheck", reason, stageStart)
			s.observeBidRoute("go_local_reject", reason)
			return result, true
		}
	}
	local, ok := s.buildLocalBidState(in.AuctionID, stageStart)
	if !ok {
		return domain.BidResult{}, false
	}
	if local.status != domain.AuctionStatusRunning && local.status != domain.AuctionStatusExtended {
		return domain.BidResult{}, false
	}
	if local.rule.Type != domain.IncrementRuleTypeFixed || local.rule.Amount <= 0 || local.rule.MaxBidSteps <= 0 {
		return domain.BidResult{}, false
	}
	if local.currentPrice <= 0 {
		return domain.BidResult{}, false
	}
	if in.ExpectedCurrentPrice == nil {
		return domain.BidResult{}, false
	}
	expectedCurrentPrice := *in.ExpectedCurrentPrice
	if expectedCurrentPrice < 0 {
		return domain.BidResult{}, false
	}
	reason := localRejectReason(in.Price, expectedCurrentPrice, local)
	if reason == "" {
		return domain.BidResult{}, false
	}
	result := rejectBidFromState(in, local.state, reason)
	s.enrichBidResult(ctx, &result, local.auction.liveSessionPtr())
	s.observeBidStage("pre_reject_local", reason, stageStart)
	s.observeBidRoute("go_local_reject", reason)
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

// localRejectReason 返回本地必然拒的拒因字符串；返回 "" 表示放行进 Lua。
func localRejectReason(price, expectedCurrentPrice int64, local localBidState) string {
	if price <= local.startPrice {
		return domain.BidRejectBelowStartPrice
	}
	if local.capPrice > 0 && price > local.capPrice {
		return domain.BidRejectAboveCapPrice
	}
	amount := local.rule.Amount
	isCapBid := local.capPrice > 0 && price == local.capPrice
	// expected 必须 <= cached（>cached 视为客户端拿了未来 state，交给 Lua 兜底；
	// 基于 expected 的 max bid steps 检查只有当 expected < cached 时才与真实 state
	// 等价拒）。
	if expectedCurrentPrice <= local.currentPrice {
		expectedAmount := local.rule.AmountForPrice(expectedCurrentPrice)
		if expectedAmount > 0 {
			expectedMaxAllowed := expectedCurrentPrice + expectedAmount*int64(local.rule.MaxBidSteps)
			if local.capPrice > 0 && expectedMaxAllowed > local.capPrice {
				expectedMaxAllowed = local.capPrice
			}
			if expectedCurrentPrice < local.currentPrice && price > expectedMaxAllowed {
				return domain.BidRejectAboveExpectedMaxBidSteps
			}
		}
	}
	if !isCapBid && (price-local.currentPrice)%amount != 0 {
		return domain.BidRejectStepMismatch
	}
	if isCapBid {
		if price <= local.currentPrice {
			return domain.BidRejectBelowMinIncrement
		}
		return ""
	}
	if price < local.currentPrice+amount {
		return domain.BidRejectBelowMinIncrement
	}
	return ""
}

// snapshotFloorPreRejectReason 兼容现有行级单测：基于显式传入的 (state, auction, rule)
// 做与 preRedisLocalReject 等价的"必然拒"判断。生产路径不再调用本函数。
func snapshotFloorPreRejectReason(in PlaceBidInput, state domain.AuctionState, stateOK bool, auction bidAuctionSnapshot, rule domain.IncrementRule) (string, bool) {
	if !stateOK || state.AuctionID == 0 {
		return "", false
	}
	if state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended {
		return "", false
	}
	if rule.Type != domain.IncrementRuleTypeFixed || rule.Amount <= 0 || rule.MaxBidSteps <= 0 {
		return "", false
	}
	if auction.StartPrice <= 0 {
		return "", false
	}
	if state.CurrentPrice <= 0 {
		return "", false
	}
	if in.ExpectedCurrentPrice == nil {
		return "", false
	}
	expectedCurrentPrice := *in.ExpectedCurrentPrice
	if expectedCurrentPrice < 0 {
		return "", false
	}
	local := localBidState{
		auctionID:    state.AuctionID,
		status:       state.Status,
		currentPrice: state.CurrentPrice,
		startPrice:   auction.StartPrice,
		capPrice:     auction.CapPrice,
		rule:         rule,
	}
	if reason := localRejectReason(in.Price, expectedCurrentPrice, local); reason != "" {
		return reason, true
	}
	return "", false
}

type BidAuctionSnapshot = bidAuctionSnapshot

func SnapshotFloorPreRejectReason(in PlaceBidInput, state domain.AuctionState, stateOK bool, auction BidAuctionSnapshot, rule domain.IncrementRule) (string, bool) {
	return snapshotFloorPreRejectReason(in, state, stateOK, auction, rule)
}

// acquireSamePriceGate 是 P3 最高价 in-flight 闸门：同一拍品内记录当前已进入 Lua
// 的最高报价，低于该最高价的请求直接返回 AUCTION_BUSY；高于或等于当前最高价的请求
// 按价格维度最多放行 samePriceInflightLimit 个并发进入 Lua。
//
// 触发条件（任一不满足则放行，不消耗 slot）：
//   - fixed rule + auction RUNNING/EXTENDED + 两个 cache 命中（与本地预拒共用安全门）
//   - currentPrice > 0
//
// 返回：release 用于在 Lua 调用之后递减 slot；rejected=true 表示已超 limit 必须直接返回
// rejectedResult。超 limit 的拒绝不写 Redis 幂等、不触发自动黑名单（由调用方保证）。
func (s *BidService) acquireSamePriceGate(ctx context.Context, in PlaceBidInput, auction bidAuctionSnapshot) (release func(), rejected bool, rejectedResult domain.BidResult) {
	if s == nil || in.ExpectedCurrentPrice == nil {
		return nil, false, domain.BidResult{}
	}
	now := time.Now()
	local, ok := s.buildLocalBidState(in.AuctionID, now)
	if !ok {
		return nil, false, domain.BidResult{}
	}
	if local.status != domain.AuctionStatusRunning && local.status != domain.AuctionStatusExtended {
		return nil, false, domain.BidResult{}
	}
	if local.rule.Type != domain.IncrementRuleTypeFixed || local.rule.Amount <= 0 {
		return nil, false, domain.BidResult{}
	}
	if local.currentPrice <= 0 {
		return nil, false, domain.BidResult{}
	}
	key := samePriceGateAuctionKey(in.AuctionID)
	release, acquired := s.tryAcquireSamePriceGateSlot(key, in.Price, now)
	if !acquired {
		s.observeBidStage("same_price_gate", "rejected", now)
		s.observeBidRoute("same_price_gate_reject", domain.BidRejectAuctionBusy)
		result := rejectBidFromState(in, local.state, domain.BidRejectAuctionBusy)
		s.enrichBidResult(ctx, &result, auction.liveSessionPtr())
		return nil, true, result
	}
	s.observeBidStage("same_price_gate", "acquired", now)
	s.observeBidRoute("same_price_gate_acquire", "ok")
	return release, false, domain.BidResult{}
}

func samePriceGateKey(auctionID uint64, expectedCurrentPrice, price int64) string {
	return strconv.FormatUint(auctionID, 10) + ":" +
		strconv.FormatInt(expectedCurrentPrice, 10) + ":" +
		strconv.FormatInt(price, 10)
}

func SamePriceGateKey(auctionID uint64, expectedCurrentPrice, price int64) string {
	return samePriceGateKey(auctionID, expectedCurrentPrice, price)
}

func samePriceGateAuctionKey(auctionID uint64) string {
	return strconv.FormatUint(auctionID, 10)
}

func parseSamePriceGateKey(key string) (string, int64, bool) {
	parts := strings.Split(key, ":")
	if len(parts) == 0 || parts[0] == "" {
		return "", 0, false
	}
	if len(parts) == 1 {
		return parts[0], 0, true
	}
	price, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return parts[0], price, true
}

func (s *BidService) tryAcquireSamePriceGateSlot(key string, price int64, now time.Time) (func(), bool) {
	if s == nil || key == "" || price <= 0 {
		return nil, true
	}
	s.samePriceGateMu.Lock()
	defer s.samePriceGateMu.Unlock()
	s.sweepSamePriceGateLocked(now)
	if s.samePriceInflight == nil {
		s.samePriceInflight = make(map[string]*samePriceGateEntry)
	}
	entry := s.samePriceInflight[key]
	if entry == nil {
		entry = &samePriceGateEntry{lastUsed: now, priceCounts: make(map[int64]*samePriceGatePriceEntry)}
		s.samePriceInflight[key] = entry
	}
	entry.lastUsed = now
	if entry.highestPrice > 0 && price < entry.highestPrice {
		return nil, false
	}
	if entry.priceCounts == nil {
		entry.priceCounts = make(map[int64]*samePriceGatePriceEntry)
	}
	priceEntry := entry.priceCounts[price]
	if priceEntry == nil {
		priceEntry = &samePriceGatePriceEntry{}
		entry.priceCounts[price] = priceEntry
	}
	if next := priceEntry.count.Add(1); next > samePriceInflightLimit {
		priceEntry.count.Add(-1)
		entry.lastUsed = now
		return nil, false
	}
	if price > entry.highestPrice {
		entry.highestPrice = price
	}
	return func() {
		s.releaseSamePriceGateSlot(key, price, priceEntry)
	}, true
}

func (s *BidService) releaseSamePriceGateSlot(key string, price int64, priceEntry *samePriceGatePriceEntry) {
	if s == nil || key == "" || price <= 0 || priceEntry == nil {
		return
	}
	s.samePriceGateMu.Lock()
	defer s.samePriceGateMu.Unlock()
	entry := s.samePriceInflight[key]
	if entry == nil {
		return
	}
	next := priceEntry.count.Add(-1)
	if next < 0 {
		priceEntry.count.Store(0)
		next = 0
	}
	if next == 0 {
		delete(entry.priceCounts, price)
		if price == entry.highestPrice {
			entry.highestPrice = samePriceGateHighestPrice(entry)
		}
	}
	entry.lastUsed = time.Now()
}

func (s *BidService) sweepSamePriceGateLocked(now time.Time) {
	if s == nil || len(s.samePriceInflight) == 0 {
		if s != nil && s.samePriceGateNextSweep.IsZero() {
			s.samePriceGateNextSweep = now.Add(samePriceGateSweepInterval)
		}
		return
	}
	if !s.samePriceGateNextSweep.IsZero() && now.Before(s.samePriceGateNextSweep) {
		return
	}
	for key, entry := range s.samePriceInflight {
		if entry == nil || (!samePriceGateHasActive(entry) && now.Sub(entry.lastUsed) >= samePriceGateIdleTTL) {
			delete(s.samePriceInflight, key)
		}
	}
	s.samePriceGateNextSweep = now.Add(samePriceGateSweepInterval)
}

func samePriceGateHasActive(entry *samePriceGateEntry) bool {
	if entry == nil {
		return false
	}
	for price, priceEntry := range entry.priceCounts {
		if priceEntry == nil || priceEntry.count.Load() <= 0 {
			delete(entry.priceCounts, price)
			continue
		}
		return true
	}
	entry.highestPrice = 0
	return false
}

func samePriceGateHighestPrice(entry *samePriceGateEntry) int64 {
	var highest int64
	if entry == nil {
		return 0
	}
	for price, priceEntry := range entry.priceCounts {
		if priceEntry == nil || priceEntry.count.Load() <= 0 {
			delete(entry.priceCounts, price)
			continue
		}
		if price > highest {
			highest = price
		}
	}
	return highest
}

func (s *BidService) samePriceGateCounter(key string) *atomic.Int32 {
	if s == nil || key == "" {
		return nil
	}
	auctionKey, price, ok := parseSamePriceGateKey(key)
	if !ok || price <= 0 {
		return nil
	}
	s.samePriceGateMu.Lock()
	defer s.samePriceGateMu.Unlock()
	if s.samePriceInflight == nil {
		s.samePriceInflight = make(map[string]*samePriceGateEntry)
	}
	entry := s.samePriceInflight[auctionKey]
	if entry == nil {
		entry = &samePriceGateEntry{lastUsed: time.Now(), priceCounts: make(map[int64]*samePriceGatePriceEntry)}
		s.samePriceInflight[auctionKey] = entry
	}
	if entry.priceCounts == nil {
		entry.priceCounts = make(map[int64]*samePriceGatePriceEntry)
	}
	priceEntry := entry.priceCounts[price]
	if priceEntry == nil {
		priceEntry = &samePriceGatePriceEntry{}
		entry.priceCounts[price] = priceEntry
	}
	if price > entry.highestPrice {
		entry.highestPrice = price
	}
	return &priceEntry.count
}

func (s *BidService) samePriceGateExists(key string) bool {
	if s == nil || key == "" {
		return false
	}
	auctionKey, price, ok := parseSamePriceGateKey(key)
	if !ok {
		return false
	}
	s.samePriceGateMu.Lock()
	defer s.samePriceGateMu.Unlock()
	entry, ok := s.samePriceInflight[auctionKey]
	if !ok || entry == nil {
		return false
	}
	if price <= 0 {
		return true
	}
	priceEntry, ok := entry.priceCounts[price]
	return ok && priceEntry != nil
}

type bidAuctionSnapshot struct {
	AuctionID      uint64
	SellerID       string
	LiveSessionID  uint64
	StartPrice     int64
	CapPrice       int64
	IncrementRule  json.RawMessage
	ParsedRule     domain.IncrementRule
	ParsedRuleOK   bool
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

type bidPrerequisiteCacheEntry struct {
	enrolled     bool
	depositReady bool
	expiresAt    time.Time
}

type bidRealtimeStateCell struct {
	value atomic.Pointer[cachedBidRealtimeState]
}

type cachedBidderProfile struct {
	nickname  string
	avatarURL string
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
		if s.auctionSnapshots != nil {
			cacheStart := time.Now()
			runtimeSnapshot, source, ok, err := s.auctionSnapshots.Get(ctx, auctionID)
			if err != nil {
				s.observeBidStage("auction_snapshot_cache", "error", cacheStart)
			} else if ok {
				if runtimeSnapshot.AuctionID != auctionID {
					s.observeBidStage("auction_snapshot_cache", "mismatch", cacheStart)
					_ = s.auctionSnapshots.Invalidate(ctx, auctionID)
				} else {
					snapshot := bidAuctionSnapshotFromRuntimeSnapshot(runtimeSnapshot)
					s.storeBidAuction(snapshot, time.Now().Add(s.effectiveAuctionCacheTTL()))
					return bidAuctionSnapshotLoad{value: snapshot, source: normalizeAuctionSnapshotCacheSource(source)}, nil
				}
			} else {
				s.observeBidStage("auction_snapshot_cache", "miss", cacheStart)
			}
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

func (s *BidService) cachedBidPrerequisites(auctionID uint64, bidderID string, now time.Time) (bool, bool, bool) {
	if s == nil || auctionID == 0 || bidderID == "" {
		return false, false, false
	}
	raw, ok := s.prerequisiteCache.Load(bidPrerequisiteCacheKey(auctionID, bidderID))
	if !ok {
		return false, false, false
	}
	entry, ok := raw.(bidPrerequisiteCacheEntry)
	if !ok || now.After(entry.expiresAt) {
		s.prerequisiteCache.Delete(bidPrerequisiteCacheKey(auctionID, bidderID))
		return false, false, false
	}
	return entry.enrolled, entry.depositReady, true
}

func (s *BidService) storeBidPrerequisites(auctionID uint64, bidderID string, enrolled, depositReady bool, expiresAt time.Time) {
	if s == nil || auctionID == 0 || bidderID == "" || !enrolled || !depositReady {
		return
	}
	s.prerequisiteCache.Store(bidPrerequisiteCacheKey(auctionID, bidderID), bidPrerequisiteCacheEntry{
		enrolled:     enrolled,
		depositReady: depositReady,
		expiresAt:    expiresAt,
	})
}

func bidPrerequisiteCacheKey(auctionID uint64, bidderID string) string {
	return strconv.FormatUint(auctionID, 10) + ":" + bidderID
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
	if rule, err := domain.ParseIncrementRule(snapshot.IncrementRule); err == nil {
		snapshot.ParsedRule = rule
		snapshot.ParsedRuleOK = true
	}
	return snapshot
}

func bidAuctionSnapshotFromRuntimeSnapshot(runtime AuctionRuntimeSnapshot) bidAuctionSnapshot {
	snapshot := bidAuctionSnapshot{
		AuctionID:      runtime.AuctionID,
		SellerID:       runtime.SellerID,
		LiveSessionID:  runtime.LiveSessionID,
		StartPrice:     runtime.StartPrice,
		CapPrice:       runtime.CapPrice,
		IncrementRule:  append(json.RawMessage(nil), runtime.IncrementRule...),
		AntiSnipingSec: runtime.AntiSnipingSec,
		AntiExtendSec:  runtime.AntiExtendSec,
		AntiExtendMode: domain.NormalizeAuctionExtendMode(runtime.AntiExtendMode),
		Status:         runtime.Status,
		StartTime:      runtime.StartTime,
		EndTime:        runtime.EndTime,
	}
	if rule, err := domain.ParseIncrementRule(snapshot.IncrementRule); err == nil {
		snapshot.ParsedRule = rule
		snapshot.ParsedRuleOK = true
	}
	return snapshot
}

func normalizeAuctionSnapshotCacheSource(source string) string {
	switch strings.TrimSpace(source) {
	case "l1":
		return "redis_cache_l1"
	case "l2":
		return "redis_cache_l2"
	default:
		return "redis_cache"
	}
}

func (a bidAuctionSnapshot) liveSessionPtr() *uint64 {
	if a.LiveSessionID == 0 {
		return nil
	}
	id := a.LiveSessionID
	return &id
}

func bidStreamEnabled(realtime auctionports.AuctionRealtimeStore) bool {
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

func (s *BidService) observeBidRoute(decision, reason string) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.IncBidRoute(decision, reason)
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

	cfg, err := adminports.ReadBlacklistStrategyConfig(ctx, s.configs)
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
	expiresAt := adminports.BlacklistExpiresAt(cfg, time.Now().UTC())
	if err := s.risk.AddBlacklist(ctx, bidderID, reason, adminports.SystemBlacklistActorID, expiresAt); err != nil {
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

func (s *BidService) preRedisPrerequisiteReject(ctx context.Context, in PlaceBidInput, auction bidAuctionSnapshot) (domain.BidResult, bool, error) {
	stageStart := time.Now()
	if enrolled, depositReady, ok := s.cachedBidPrerequisites(in.AuctionID, in.BidderID, stageStart); ok {
		s.observeBidStage("bid_prerequisites", "cache_hit", stageStart)
		if enrolled && depositReady {
			return domain.BidResult{}, false, nil
		}
	}
	enrolled, depositReady, err := s.realtime.BidPrerequisites(ctx, in.AuctionID, in.BidderID)
	if err != nil {
		s.observeBidStage("bid_prerequisites", "error", stageStart)
		return domain.BidResult{}, false, err
	}
	if enrolled && depositReady {
		s.storeBidPrerequisites(in.AuctionID, in.BidderID, enrolled, depositReady, time.Now().Add(bidPrerequisiteCacheTTL))
		s.observeBidStage("bid_prerequisites", "ok", stageStart)
		return domain.BidResult{}, false, nil
	}
	reason := "DEPOSIT_NOT_READY"
	resultLabel := "missing_deposit"
	if !enrolled {
		reason = "NOT_ENROLLED"
		resultLabel = "not_enrolled"
	}
	s.observeBidStage("bid_prerequisites", resultLabel, stageStart)
	s.observeBidRoute("go_prerequisite_reject", reason)
	state := domain.AuctionState{
		AuctionID:     in.AuctionID,
		LiveSessionID: auction.LiveSessionID,
		Status:        auction.Status,
		StartPrice:    auction.StartPrice,
		CapPrice:      auction.CapPrice,
		CurrentPrice:  auction.StartPrice,
		EndTime:       auction.EndTime,
		Source:        "go_prerequisite",
	}
	if cached, ok := s.cachedBidRealtimeState(in.AuctionID, time.Now()); ok {
		state.CurrentPrice = cached.CurrentPrice
		state.LeaderBidderID = cached.LeaderBidderID
		state.EndTime = cached.EndTime
		state.ExtendCount = cached.ExtendCount
		state.Version = cached.Version
		state.Status = cached.Status
		if state.LiveSessionID == 0 {
			state.LiveSessionID = cached.LiveSessionID
		}
		if state.StartPrice == 0 {
			state.StartPrice = cached.StartPrice
		}
		if state.CapPrice == 0 {
			state.CapPrice = cached.CapPrice
		}
	}
	return rejectBidFromState(in, state, reason), true, nil
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
			serverTime := time.Now().UTC()
			if result.ServerTime != nil {
				serverTime = result.ServerTime.UTC()
			}
			extendedPayload := map[string]interface{}{
				"auctionId":   result.AuctionID,
				"endTime":     result.EndTime,
				"extendCount": result.ExtendCount,
				"serverTime":  serverTime,
			}
			if result.LiveSessionID != 0 {
				extendedPayload["liveSessionId"] = result.LiveSessionID
			}
			broadcastJSON(s.publisher, result.AuctionID, "timer.extended", extendedPayload)
		}
		return
	}
}

func broadcastJSONWithSeq(publisher EventPublisher, auctionID uint64, eventType string, seq int64, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, auctionports.EventEnvelope{Type: eventType, Seq: seq, Payload: raw})
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

// NotifyRankingUpdated 在异步排行榜 worker 完成 ZSet 更新后触发合并广播。
func (s *BidService) NotifyRankingUpdated(auctionID uint64) {
	s.scheduleRankingBroadcast(auctionID)
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
	payload := map[string]interface{}{
		"auctionId": auctionID,
		"ranking":   ranking,
	}
	if liveSessionID := s.rankingLiveSessionID(ctx, auctionID); liveSessionID != 0 {
		payload["liveSessionId"] = liveSessionID
	}
	broadcastJSON(s.publisher, auctionID, "ranking.updated", payload)
}

func (s *BidService) rankingLiveSessionID(ctx context.Context, auctionID uint64) uint64 {
	if s == nil || auctionID == 0 {
		return 0
	}
	if s.realtime != nil {
		if state, ok, err := s.realtime.GetAuctionState(ctx, auctionID); err == nil && ok && state.LiveSessionID != 0 {
			return state.LiveSessionID
		}
	}
	if s.auctions != nil {
		if auction, err := s.auctions.FindByID(ctx, auctionID); err == nil && auction.LiveSessionID != nil {
			return *auction.LiveSessionID
		}
	}
	return 0
}

func (s *BidService) scheduleHighestBidHook(auctionID uint64, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64) {
	if s == nil || s.hook == nil || auctionID == 0 || strings.TrimSpace(merchantID) == "" || sessionID == 0 || strings.TrimSpace(bidderID) == "" || price <= 0 {
		return
	}
	period := s.highestBidHookQuietPeriod()
	if period <= 0 {
		s.emitHighestBidHook(context.Background(), merchantID, sessionID, bidderID, bidderName, price)
		return
	}
	// 以拍品(auctionID)为防抖维度：同一拍品的连续出价共享一个计时器，
	// 不同拍品互不影响、各自独立计时。
	key := strconv.FormatUint(auctionID, 10)
	s.liveAgentHookMu.Lock()
	if s.pendingLiveAgentHooks == nil {
		s.pendingLiveAgentHooks = make(map[string]*pendingBidLiveAgentHook)
	}
	pending := s.pendingLiveAgentHooks[key]
	if pending == nil {
		pending = &pendingBidLiveAgentHook{}
		s.pendingLiveAgentHooks[key] = pending
	}
	pending.merchantID = strings.TrimSpace(merchantID)
	pending.sessionID = sessionID
	pending.bidderID = strings.TrimSpace(bidderID)
	pending.bidderName = strings.TrimSpace(bidderName)
	pending.price = price
	pending.generation++
	generation := pending.generation
	if pending.timer != nil {
		pending.timer.Stop()
	}
	pending.timer = time.AfterFunc(period, func() {
		s.fireHighestBidHook(key, generation)
	})
	s.liveAgentHookMu.Unlock()
}

func (s *BidService) highestBidHookQuietPeriod() time.Duration {
	if s == nil {
		return 0
	}
	s.liveAgentHookMu.Lock()
	defer s.liveAgentHookMu.Unlock()
	return s.liveAgentHookQuietPeriod
}

func (s *BidService) fireHighestBidHook(key string, generation uint64) {
	if s == nil {
		return
	}
	s.liveAgentHookMu.Lock()
	pending := s.pendingLiveAgentHooks[key]
	if pending == nil || pending.generation != generation {
		s.liveAgentHookMu.Unlock()
		return
	}
	merchantID := pending.merchantID
	sessionID := pending.sessionID
	bidderID := pending.bidderID
	bidderName := pending.bidderName
	price := pending.price
	delete(s.pendingLiveAgentHooks, key)
	s.liveAgentHookMu.Unlock()

	s.emitHighestBidHook(context.Background(), merchantID, sessionID, bidderID, bidderName, price)
}

func (s *BidService) emitHighestBidHook(ctx context.Context, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64) {
	if s == nil || s.hook == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Error("live agent hook panic", "session_id", sessionID, "panic", r)
		}
	}()
	emitStart := time.Now()
	if immediate, ok := s.hook.(BidLiveAgentHookImmediate); ok {
		immediate.EmitHighestBidWithBidderNameNow(ctx, merchantID, sessionID, bidderID, bidderName, price)
	} else {
		s.hook.EmitHighestBidWithBidderName(ctx, merchantID, sessionID, bidderID, bidderName, price)
	}
	s.observeBidStage("live_agent_hook", "emit", emitStart)
}

func (s *BidService) enrichBidResult(ctx context.Context, result *domain.BidResult, liveSessionID *uint64) {
	if result == nil {
		return
	}
	if result.LiveSessionID == 0 && liveSessionID != nil {
		result.LiveSessionID = *liveSessionID
	}
	if result.ServerTime == nil {
		serverTime := time.Now().UTC()
		result.ServerTime = &serverTime
	}
	if result.Accepted {
		profile := s.bidderProfile(ctx, result.BidderID)
		if result.BidderNickname == "" {
			result.BidderNickname = profile.nickname
		}
		if result.BidderAvatarURL == "" {
			result.BidderAvatarURL = profile.avatarURL
		}
		if result.BidderNickname == "" {
			result.BidderNickname = fallbackBidderNickname(result.BidderID)
		}
		result.Nickname = result.BidderNickname
		result.AvatarURL = result.BidderAvatarURL
	}
}

func (s *BidService) enrichRanking(ctx context.Context, ranking []domain.RankingEntry) []domain.RankingEntry {
	if len(ranking) == 0 || s.users == nil {
		return ranking
	}
	out := make([]domain.RankingEntry, len(ranking))
	copy(out, ranking)
	cache := make(map[string]domain.User, len(out))
	for i := range out {
		id := strings.TrimSpace(out[i].BidderID)
		if id == "" {
			continue
		}
		if user, ok := cache[id]; ok {
			applyRankingUserProfile(&out[i], user)
			continue
		}
		user, err := s.users.FindByID(id)
		if err != nil {
			continue
		}
		cache[id] = user
		applyRankingUserProfile(&out[i], user)
	}
	return out
}

func applyRankingUserProfile(entry *domain.RankingEntry, user domain.User) {
	if entry == nil {
		return
	}
	nickname := strings.TrimSpace(user.Nickname)
	avatarURL := strings.TrimSpace(user.AvatarURL)
	if nickname == "" {
		nickname = fallbackBidderNickname(entry.BidderID)
	}
	entry.BidderNickname = nickname
	entry.Nickname = nickname
	entry.BidderAvatarURL = avatarURL
	entry.AvatarURL = avatarURL
}

func (s *BidService) bidderProfile(ctx context.Context, userID string) cachedBidderProfile {
	_ = ctx
	if s == nil || s.users == nil || strings.TrimSpace(userID) == "" {
		return cachedBidderProfile{}
	}
	userID = strings.TrimSpace(userID)
	if profile, ok := s.cachedBidderProfile(userID, time.Now()); ok {
		return profile
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return cachedBidderProfile{}
	}
	profile := cachedBidderProfile{nickname: strings.TrimSpace(user.Nickname), avatarURL: strings.TrimSpace(user.AvatarURL)}
	if profile.nickname == "" {
		profile.nickname = fallbackBidderNickname(userID)
	}
	s.storeBidderProfile(userID, profile, time.Now().Add(bidNicknameCacheTTL))
	return profile
}

func fallbackBidderNickname(userID string) string {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "用户"
	}
	suffix := userID
	if len([]rune(suffix)) > 4 {
		runes := []rune(suffix)
		suffix = string(runes[len(runes)-4:])
	}
	return "用户" + suffix
}

func (s *BidService) cachedBidderProfile(userID string, now time.Time) (cachedBidderProfile, bool) {
	s.profileCacheMu.RLock()
	defer s.profileCacheMu.RUnlock()
	if s.profileCache == nil {
		return cachedBidderProfile{}, false
	}
	cached, ok := s.profileCache[userID]
	if !ok || now.After(cached.expiresAt) {
		return cachedBidderProfile{}, false
	}
	return cached, true
}

func (s *BidService) storeBidderProfile(userID string, profile cachedBidderProfile, expiresAt time.Time) {
	s.profileCacheMu.Lock()
	defer s.profileCacheMu.Unlock()
	if s.profileCache == nil {
		s.profileCache = make(map[string]cachedBidderProfile)
	}
	profile.expiresAt = expiresAt
	s.profileCache[userID] = profile
}

func bidResultFromRecord(record domain.BidRecord) domain.BidResult {
	accepted := record.RejectReason == ""
	result := domain.BidResult{
		RequestID:      record.RequestID,
		AuctionID:      record.AuctionID,
		BidderID:       record.BidderID,
		BidderNickname: record.BidderNickname,
		Nickname:       record.BidderNickname,
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
