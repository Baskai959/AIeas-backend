package realtime

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

type MemoryRealtimeStore struct {
	mu       sync.RWMutex
	auctions map[uint64]*memoryRealtimeAuction
}

type memoryRealtimeAuction struct {
	state         domain.AuctionState
	minIncrement  int64
	incrementRule json.RawMessage
	startPrice    int64
	capPrice      int64
	rule          domain.IncrementRule
	enrolled      map[string]struct{}
	deposits      map[string]struct{}
	ranking       map[string]memoryRankingEntry
	nextBidSeq    int64
	idempotency   map[string]domain.BidResult
	hammerResult  *domain.HammerResult
	frequency     map[string]memoryFrequency
}

type memoryRankingEntry struct {
	price      int64
	acceptedAt int64
	seq        int64
}

type memoryFrequency struct {
	windowStart int64
	count       int
}

func NewMemoryRealtimeStore() *MemoryRealtimeStore {
	return &MemoryRealtimeStore{
		auctions: make(map[uint64]*memoryRealtimeAuction),
	}
}

func (s *MemoryRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	if minIncrement <= 0 {
		minIncrement = 1
	}
	rule, err := domain.ParseIncrementRule(auction.IncrementRule)
	if err != nil {
		rule = domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: minIncrement, MaxBidSteps: 1}
	} else {
		if amount := rule.AmountForPrice(auction.StartPrice); amount > 0 {
			minIncrement = amount
		}
	}
	state := domain.AuctionState{
		AuctionID:     auction.AuctionID,
		Status:        auction.Status,
		StartPrice:    auction.StartPrice,
		CapPrice:      auction.CapPrice,
		IncrementRule: append([]byte(nil), auction.IncrementRule...),
		CurrentPrice:  auction.StartPrice,
		StartTime:     auction.StartTime,
		EndTime:       auction.EndTime,
		Version:       time.Now().UTC().UnixMilli(),
		Source:        "redis",
		AntiSnipingMS: int64(auction.AntiSnipingSec) * 1000,
		AntiExtendMS:  int64(auction.AntiExtendSec) * 1000,
	}
	if auction.LiveSessionID != nil {
		state.LiveSessionID = *auction.LiveSessionID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.auctions[auction.AuctionID]
	if existing == nil {
		existing = &memoryRealtimeAuction{
			enrolled:    make(map[string]struct{}),
			deposits:    make(map[string]struct{}),
			ranking:     make(map[string]memoryRankingEntry),
			idempotency: make(map[string]domain.BidResult),
			frequency:   make(map[string]memoryFrequency),
		}
		s.auctions[auction.AuctionID] = existing
	}
	state.ParticipantCount = len(existing.enrolled)
	existing.state = state
	existing.minIncrement = minIncrement
	existing.incrementRule = append([]byte(nil), auction.IncrementRule...)
	existing.startPrice = auction.StartPrice
	existing.capPrice = auction.CapPrice
	existing.rule = rule
	existing.ranking = make(map[string]memoryRankingEntry)
	existing.idempotency = make(map[string]domain.BidResult)
	existing.frequency = make(map[string]memoryFrequency)
	existing.nextBidSeq = 0
	existing.hammerResult = nil
	return state, nil
}

func (s *MemoryRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	auction := s.auctions[auctionID]
	if auction == nil {
		return domain.AuctionState{}, false, nil
	}
	state := auction.state
	state.Source = "redis"
	return state, true, nil
}

func (s *MemoryRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return domain.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	auction := s.getOrCreateAuctionLocked(auctionID)
	if _, ok := auction.enrolled[userID]; !ok {
		auction.state.ParticipantCount++
		auction.state.Version++
	}
	auction.enrolled[userID] = struct{}{}
	auction.deposits[userID] = struct{}{}
	return nil
}

func (s *MemoryRealtimeStore) BidResultByRequestID(ctx context.Context, auctionID uint64, requestID string) (domain.BidResult, bool, error) {
	_ = ctx
	if requestID == "" {
		return domain.BidResult{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	auction := s.auctions[auctionID]
	if auction == nil {
		return domain.BidResult{}, false, nil
	}
	result, ok := auction.idempotency[requestID]
	return result, ok, nil
}

func (s *MemoryRealtimeStore) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	auction := s.auctions[auctionID]
	if auction == nil {
		return false, false, nil
	}
	_, enrolled := auction.enrolled[userID]
	_, depositReady := auction.deposits[userID]
	return enrolled, depositReady, nil
}

func (s *MemoryRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowMS := now.UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	auction := s.auctions[input.AuctionID]
	if auction == nil {
		return rejectBid(input, "AUCTION_NOT_READY", domain.AuctionState{AuctionID: input.AuctionID}), nil
	}
	if existing, ok := auction.idempotency[input.RequestID]; ok && input.RequestID != "" {
		existing.Duplicate = true
		return existing, nil
	}
	state := auction.state
	if state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended {
		result := rejectBid(input, "INVALID_STATE", state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}
	if _, ok := auction.enrolled[input.BidderID]; !ok {
		result := rejectBid(input, "NOT_ENROLLED", state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}
	if _, ok := auction.deposits[input.BidderID]; !ok {
		result := rejectBid(input, "DEPOSIT_NOT_READY", state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}
	rule := input.IncrementRule
	if rule.Type == "" || rule.MaxBidSteps <= 0 || rule.AmountForPrice(state.CurrentPrice) <= 0 {
		rule = auction.rule
	}
	if rule.AmountForPrice(state.CurrentPrice) <= 0 {
		rule = domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: auction.minIncrement, MaxBidSteps: 1}
	}
	startPrice := input.StartPrice
	if startPrice == 0 {
		startPrice = auction.startPrice
	}
	capPrice := input.CapPrice
	if capPrice == 0 {
		capPrice = auction.capPrice
	}
	if input.ExpectedCurrentPrice == nil {
		result := rejectBid(input, domain.BidRejectMissingExpectedState, state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}
	if reason := domain.ValidateBidExpectedCurrentPrice(*input.ExpectedCurrentPrice, state.CurrentPrice, capPrice, input.Price, rule); reason != "" {
		result := rejectBid(input, reason, state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}
	if input.FreqLimitCount > 0 && input.FreqWindowMS > 0 {
		freq := auction.frequency[input.BidderID]
		if freq.windowStart == 0 || nowMS-freq.windowStart >= input.FreqWindowMS {
			freq = memoryFrequency{windowStart: nowMS, count: 1}
		} else {
			freq.count++
		}
		auction.frequency[input.BidderID] = freq
		if freq.count > input.FreqLimitCount {
			result := rejectBid(input, "FREQ_LIMIT", state)
			auction.storeBidResult(input.RequestID, result)
			return result, nil
		}
	}
	if reason := domain.ValidateBidPrice(startPrice, state.CurrentPrice, capPrice, input.Price, rule); reason != "" {
		result := rejectBid(input, reason, state)
		auction.storeBidResult(input.RequestID, result)
		return result, nil
	}

	state.CurrentPrice = input.Price
	state.LeaderBidderID = input.BidderID
	state.BidCount++
	state.LastBidTSMS = nowMS
	state.Version++
	extended := false
	autoClosed := capPrice > 0 && input.Price == capPrice
	if autoClosed {
		state.Status = domain.AuctionStatusClosedWon
		state.EndTime = now
	} else if input.AntiSnipingMS > 0 && input.AntiExtendMS > 0 && input.MaxExtendCount > state.ExtendCount {
		endMS := state.EndTime.UnixMilli()
		if endMS-nowMS <= input.AntiSnipingMS {
			if domain.NormalizeAuctionExtendMode(input.AntiExtendMode) == domain.AuctionExtendModeReset {
				state.EndTime = time.UnixMilli(nowMS + input.AntiExtendMS).UTC()
			} else {
				state.EndTime = time.UnixMilli(endMS + input.AntiExtendMS).UTC()
			}
			state.ExtendCount++
			state.Status = domain.AuctionStatusExtended
			extended = true
		}
	}
	auction.state = state
	auction.nextBidSeq++
	auction.ranking[input.BidderID] = memoryRankingEntry{price: input.Price, acceptedAt: nowMS, seq: auction.nextBidSeq}
	result := domain.BidResult{
		RequestID:       input.RequestID,
		AuctionID:       input.AuctionID,
		LiveSessionID:   input.LiveSessionID,
		BidderID:        input.BidderID,
		BidderNickname:  input.BidderNickname,
		Nickname:        input.BidderNickname,
		BidderAvatarURL: input.BidderAvatarURL,
		AvatarURL:       input.BidderAvatarURL,
		Price:           input.Price,
		Accepted:        true,
		CurrentPrice:    state.CurrentPrice,
		LeaderBidderID:  state.LeaderBidderID,
		EndTime:         state.EndTime,
		Extended:        extended,
		ExtendCount:     state.ExtendCount,
		Version:         state.Version,
		Event:           "bid.accepted",
		RiskResult:      domain.BidRiskAllow,
		AuctionStatus:   state.Status,
		AutoClosed:      autoClosed,
	}
	auction.storeBidResult(input.RequestID, result)
	return result, nil
}

func (s *MemoryRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	auction := s.auctions[input.AuctionID]
	if auction == nil {
		return domain.HammerResult{}, domain.ErrNotFound
	}
	if auction.hammerResult != nil {
		result := *auction.hammerResult
		result.Duplicate = true
		return result, nil
	}
	state := auction.state
	if !input.Force && now.Before(state.EndTime) {
		return domain.HammerResult{}, domain.ErrInvalidState
	}
	result := domain.HammerResult{
		RequestID: input.RequestID,
		AuctionID: input.AuctionID,
		Status:    domain.AuctionStatusClosedFailed,
		ClosedAt:  now,
		Version:   state.Version + 1,
	}
	if state.LeaderBidderID != "" && state.CurrentPrice >= input.ReservePrice {
		result.Status = domain.AuctionStatusClosedWon
		result.WinnerID = state.LeaderBidderID
		result.Price = state.CurrentPrice
	}
	state.Status = result.Status
	state.Version = result.Version
	auction.state = state
	auction.hammerResult = &result
	return result, nil
}

func (s *MemoryRealtimeStore) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	_ = ctx
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	auction := s.auctions[auctionID]
	if auction == nil {
		return []domain.RankingEntry{}, nil
	}
	entries := make([]domain.RankingEntry, 0, len(auction.ranking))
	order := make(map[string]memoryRankingEntry, len(auction.ranking))
	for bidderID, ranking := range auction.ranking {
		order[bidderID] = ranking
		entries = append(entries, domain.RankingEntry{BidderID: bidderID, Price: ranking.price})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Price == entries[j].Price {
			left := order[entries[i].BidderID]
			right := order[entries[j].BidderID]
			if left.acceptedAt == right.acceptedAt {
				if left.seq == right.seq {
					return entries[i].BidderID < entries[j].BidderID
				}
				return left.seq < right.seq
			}
			return left.acceptedAt < right.acceptedAt
		}
		return entries[i].Price > entries[j].Price
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	for i := range entries {
		entries[i].Rank = i + 1
	}
	return entries, nil
}

func (s *MemoryRealtimeStore) getOrCreateAuctionLocked(auctionID uint64) *memoryRealtimeAuction {
	auction := s.auctions[auctionID]
	if auction != nil {
		return auction
	}
	auction = &memoryRealtimeAuction{
		state:       domain.AuctionState{AuctionID: auctionID, Source: "redis"},
		enrolled:    make(map[string]struct{}),
		deposits:    make(map[string]struct{}),
		ranking:     make(map[string]memoryRankingEntry),
		idempotency: make(map[string]domain.BidResult),
		frequency:   make(map[string]memoryFrequency),
	}
	s.auctions[auctionID] = auction
	return auction
}

func (a *memoryRealtimeAuction) storeBidResult(requestID string, result domain.BidResult) {
	if requestID == "" {
		return
	}
	a.idempotency[requestID] = result
}

func rejectBid(input domain.BidInput, reason string, state domain.AuctionState) domain.BidResult {
	return domain.BidResult{
		RequestID:       input.RequestID,
		AuctionID:       input.AuctionID,
		LiveSessionID:   input.LiveSessionID,
		BidderID:        input.BidderID,
		BidderNickname:  input.BidderNickname,
		Nickname:        input.BidderNickname,
		BidderAvatarURL: input.BidderAvatarURL,
		AvatarURL:       input.BidderAvatarURL,
		Price:           input.Price,
		Accepted:        false,
		Reason:          reason,
		CurrentPrice:    state.CurrentPrice,
		LeaderBidderID:  state.LeaderBidderID,
		EndTime:         state.EndTime,
		ExtendCount:     state.ExtendCount,
		Version:         state.Version,
		AuctionStatus:   state.Status,
		Event:           "bid.rejected",
		RiskResult:      domain.BidRiskReject,
	}
}
