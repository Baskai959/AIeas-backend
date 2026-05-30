package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

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
	return &BidService{bids: bids, auctions: auctions, realtime: realtime, risk: risk, publisher: publisher, cfg: cfg}
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
	in.RequestID = strings.TrimSpace(in.RequestID)
	in.BidderID = strings.TrimSpace(in.BidderID)
	if in.RequestID == "" || in.AuctionID == 0 || in.BidderID == "" || in.Price <= 0 || in.UserRole != domain.RoleBuyer || in.ExpectedCurrentPrice == nil {
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	// P0-3：bidStreamEnabled=true 时跳过 MySQL `bid_record` 的 FindByRequestID 前置查询，
	// 直接走 Redis 幂等链路（realtimeBidResultByRequestID）。MySQL 路径仅在 stream 关闭时承担前置幂等。
	streamEnabled := bidStreamEnabled(s.realtime)
	if !streamEnabled && s.bids != nil {
		record, err := s.bids.FindByRequestID(ctx, in.RequestID)
		if err == nil {
			return bidResultFromRecord(record), nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return domain.BidResult{}, err
		}
	}
	if result, ok, err := realtimeBidResultByRequestID(ctx, s.realtime, in.AuctionID, in.RequestID); err != nil {
		return domain.BidResult{}, err
	} else if ok {
		result.Duplicate = true
		return result, nil
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.BidResult{}, err
	}
	now := time.Now().UTC()
	if in.Source == "" {
		in.Source = "live_ws"
	}
	riskControl := s.currentRiskControl(ctx)
	riskControlEnabled := riskControl.Enabled
	blacklistStrategy := domain.BlacklistStrategyConfig{}
	if riskControlEnabled {
		blacklistStrategy = s.currentBlacklistStrategy(ctx)
	}
	// v2 起黑名单完全在 service 层做前置门面拦截：MySQL（source of truth）+ LayeredCache，
	// 不再下沉到 Lua（避免把全局黑名单 key 复制到每个 RT shard）。
	// RiskService.IsBlacklisted 在 cache/repo 故障时 fail-open，由 cap-price 等下游约束兜底。
	blacklisted := false
	if s.risk != nil && riskControlEnabled {
		isBlacklisted, err := s.risk.IsBlacklisted(ctx, in.BidderID)
		if err != nil {
			return domain.BidResult{}, err
		}
		blacklisted = isBlacklisted
		if blacklisted {
			result := domain.BidResult{
				RequestID:    in.RequestID,
				AuctionID:    in.AuctionID,
				BidderID:     in.BidderID,
				Price:        in.Price,
				Accepted:     false,
				Reason:       "BLACKLIST",
				CurrentPrice: auction.StartPrice,
				EndTime:      auction.EndTime,
				Event:        "bid.rejected",
				RiskResult:   domain.BidRiskReject,
			}
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
	}
	state, stateOK, err := s.realtime.GetAuctionState(ctx, in.AuctionID)
	if err != nil {
		return domain.BidResult{}, err
	}
	rule, err := domain.ParseIncrementRule(auction.IncrementRule)
	if err != nil {
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	minIncrement := rule.AmountForPrice(auction.StartPrice)
	if minIncrement <= 0 {
		minIncrement = s.cfg.MinIncrementCent
	}
	if minIncrement <= 0 {
		minIncrement = 1
	}
	if stateOK && !blacklisted && (state.Status == domain.AuctionStatusRunning || state.Status == domain.AuctionStatusExtended) {
		if amount := rule.AmountForPrice(state.CurrentPrice); amount > 0 {
			minIncrement = amount
		}
		enrolled, depositReady, err := realtimeBidPrerequisites(ctx, s.realtime, in.AuctionID, in.BidderID)
		if err != nil {
			return domain.BidResult{}, err
		}
		prerequisitesReady := enrolled && depositReady
		if !prerequisitesReady {
			reason := "DEPOSIT_NOT_READY"
			if !enrolled {
				reason = "NOT_ENROLLED"
			}
			result := rejectBidFromState(in, state, reason)
			if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.MissingDepositEnabled && s.risk != nil {
				s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_"+reason, result)
			}
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
		if prerequisitesReady {
			if reason := validateBidExpectedState(in, state, auction.CapPrice, rule); reason != "" {
				result := rejectBidFromState(in, state, reason)
				s.persistBid(ctx, in, result, now)
				s.publishBidResult(ctx, result)
				return result, nil
			}
			if reason := domain.ValidateBidPrice(auction.StartPrice, state.CurrentPrice, auction.CapPrice, in.Price, rule); reason != "" {
				result := rejectBidFromState(in, state, reason)
				if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.UnreasonablePriceEnabled && s.risk != nil &&
					(isAutoBlacklistPriceReason(reason) || bidAboveAllowedMax(auction.StartPrice, state.CurrentPrice, auction.CapPrice, in.Price, rule)) {
					s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_"+reason, result)
				}
				s.persistBid(ctx, in, result, now)
				s.publishBidResult(ctx, result)
				return result, nil
			}
		}
	}
	if !stateOK && !blacklisted {
		state := domain.AuctionState{
			AuctionID:    auction.AuctionID,
			Status:       auction.Status,
			CurrentPrice: auction.StartPrice,
			StartTime:    auction.StartTime,
			EndTime:      auction.EndTime,
		}
		if reason := validateBidExpectedState(in, state, auction.CapPrice, rule); reason != "" {
			result := rejectBidFromState(in, state, reason)
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
		if reason := domain.ValidateBidPrice(auction.StartPrice, auction.StartPrice, auction.CapPrice, in.Price, rule); reason != "" {
			result := rejectBidFromState(in, state, reason)
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
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
	result, err := s.realtime.PlaceBid(ctx, domain.BidInput{
		RequestID:            in.RequestID,
		AuctionID:            in.AuctionID,
		BidderID:             in.BidderID,
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
		IdempotencyTTL:       24 * time.Hour,
		StartPrice:           auction.StartPrice,
		CapPrice:             auction.CapPrice,
		IncrementRule:        rule,
	})
	if err != nil {
		return domain.BidResult{}, err
	}
	if result.RiskResult == "" {
		if result.Accepted {
			result.RiskResult = domain.BidRiskAllow
		} else {
			result.RiskResult = domain.BidRiskReject
		}
	}
	if result.Accepted && !result.Duplicate && s.hook != nil && auction.LiveRoomID != 0 {
		s.hook.EmitHighestBid(ctx, auction.SellerID, auction.LiveRoomID, result.BidderID, result.CurrentPrice)
	}
	if !streamEnabled {
		s.persistBid(ctx, in, result, now)
	}
	if result.Accepted && !result.Duplicate && result.AutoClosed && s.hammer != nil {
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
			return domain.BidResult{}, err
		}
	}
	if result.Reason == "FREQ_LIMIT" && s.risk != nil {
		s.risk.RecordEvent(ctx, "BID_FREQ", in.BidderID, in.AuctionID, domain.RiskSeverityMid, result)
		if riskControlEnabled && blacklistStrategy.Enabled && blacklistStrategy.FrequencyEnabled {
			s.scheduleAutoBlacklist(ctx, blacklistStrategy, in.BidderID, in.AuctionID, "AUTO_BLACKLIST_FREQ_LIMIT", result)
		}
	}
	if !streamEnabled {
		s.publishBidResult(ctx, result)
	}
	return result, nil
}

func (s *BidService) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	if auctionID == 0 {
		return nil, domain.ErrInvalidArgument
	}
	return s.realtime.TopN(ctx, auctionID, limit)
}

func bidStreamEnabled(realtime repository.AuctionRealtimeStore) bool {
	type streamStore interface{ StreamEnabled() bool }
	store, ok := realtime.(streamStore)
	return ok && store.StreamEnabled()
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

func realtimeBidResultByRequestID(ctx context.Context, realtime repository.AuctionRealtimeStore, auctionID uint64, requestID string) (domain.BidResult, bool, error) {
	type bidResultReader interface {
		BidResultByRequestID(context.Context, uint64, string) (domain.BidResult, bool, error)
	}
	if strings.TrimSpace(requestID) == "" {
		return domain.BidResult{}, false, nil
	}
	reader, ok := realtime.(bidResultReader)
	if !ok {
		return domain.BidResult{}, false, nil
	}
	return reader.BidResultByRequestID(ctx, auctionID, requestID)
}

func realtimeBidPrerequisitesReady(ctx context.Context, realtime repository.AuctionRealtimeStore, auctionID uint64, bidderID string) (bool, error) {
	enrolled, depositReady, err := realtimeBidPrerequisites(ctx, realtime, auctionID, bidderID)
	if err != nil {
		return false, err
	}
	return enrolled && depositReady, nil
}

func realtimeBidPrerequisites(ctx context.Context, realtime repository.AuctionRealtimeStore, auctionID uint64, bidderID string) (bool, bool, error) {
	type prerequisiteReader interface {
		BidPrerequisites(context.Context, uint64, string) (bool, bool, error)
	}
	reader, ok := realtime.(prerequisiteReader)
	if !ok {
		return false, false, nil
	}
	enrolled, depositReady, err := reader.BidPrerequisites(ctx, auctionID, bidderID)
	if err != nil {
		return false, false, err
	}
	return enrolled, depositReady, nil
}

func validateBidExpectedState(in PlaceBidInput, state domain.AuctionState, capPrice int64, rule domain.IncrementRule) string {
	if in.ExpectedCurrentPrice == nil {
		return domain.BidRejectMissingExpectedState
	}
	return domain.ValidateBidExpectedCurrentPrice(*in.ExpectedCurrentPrice, state.CurrentPrice, capPrice, in.Price, rule)
}

func rejectBidFromState(in PlaceBidInput, state domain.AuctionState, reason string) domain.BidResult {
	return domain.BidResult{
		RequestID:      in.RequestID,
		AuctionID:      in.AuctionID,
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
	}
}

func (s *BidService) persistBid(ctx context.Context, in PlaceBidInput, result domain.BidResult, now time.Time) {
	if s.bids == nil {
		return
	}
	var sessionID *uint64
	if s.auctions != nil {
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
		if ranking, err := s.TopN(ctx, result.AuctionID, 10); err == nil {
			broadcastJSON(s.publisher, result.AuctionID, "ranking.updated", map[string]interface{}{
				"auctionId": result.AuctionID,
				"ranking":   ranking,
			})
		}
		if result.Extended {
			broadcastJSON(s.publisher, result.AuctionID, "timer.extended", map[string]interface{}{
				"auctionId":   result.AuctionID,
				"endTime":     result.EndTime,
				"extendCount": result.ExtendCount,
			})
		}
		return
	}
	broadcastJSONWithSeq(s.publisher, result.AuctionID, "bid.rejected", result.Seq, result)
}

func bidResultFromRecord(record domain.BidRecord) domain.BidResult {
	accepted := record.RejectReason == ""
	result := domain.BidResult{
		RequestID:  record.RequestID,
		AuctionID:  record.AuctionID,
		BidderID:   record.BidderID,
		Price:      record.BidPrice,
		Accepted:   accepted,
		Duplicate:  true,
		Reason:     record.RejectReason,
		RiskResult: record.RiskResult,
	}
	if accepted {
		result.CurrentPrice = record.BidPrice
		result.Event = "bid.accepted"
	} else {
		result.Event = "bid.rejected"
	}
	return result
}
