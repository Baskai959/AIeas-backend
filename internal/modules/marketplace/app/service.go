package app

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"aieas_backend/internal/domain"
	marketplaceports "aieas_backend/internal/modules/marketplace/ports"
)

type MarketplaceService struct {
	auctions marketplaceports.AuctionRepository
	sessions marketplaceports.LiveSessionRepository
	deposits marketplaceports.DepositRepository
	orders   marketplaceports.OrderRepository
	users    marketplaceports.UserRepository
	realtime marketplaceports.AuctionRealtimeStore
	hub      marketplaceports.OnlineCounter
}

func NewMarketplaceService(auctions marketplaceports.AuctionRepository, sessions marketplaceports.LiveSessionRepository, deposits marketplaceports.DepositRepository, orders marketplaceports.OrderRepository, users marketplaceports.UserRepository) *MarketplaceService {
	return &MarketplaceService{auctions: auctions, sessions: sessions, deposits: deposits, orders: orders, users: users}
}

func (s *MarketplaceService) SetRealtime(realtime marketplaceports.AuctionRealtimeStore) {
	if s == nil {
		return
	}
	s.realtime = realtime
}

func (s *MarketplaceService) SetOnlineCounter(hub marketplaceports.OnlineCounter) {
	if s == nil {
		return
	}
	s.hub = hub
}

func (s *MarketplaceService) SearchLots(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error) {
	if s == nil || s.auctions == nil {
		return nil, 0, domain.ErrNotFound
	}
	filter.VisibleStatuses = publicAuctionStatuses()
	if filter.Status != "" && !publicAuctionStatus(filter.Status) {
		return []domain.AuctionLot{}, 0, nil
	}
	liveSessionIDs, err := s.liveSessionIDs(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(liveSessionIDs) == 0 {
		return []domain.AuctionLot{}, 0, nil
	}
	filter.LiveSessionIDs = liveSessionIDs
	filter.CategoryValues = categoryValuesForID(filter.CategoryID)
	lots, total, err := s.auctions.Search(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	return s.enrichLots(ctx, lots), total, nil
}

func (s *MarketplaceService) liveSessionIDs(ctx context.Context) ([]uint64, error) {
	if s == nil || s.sessions == nil {
		return nil, nil
	}
	const pageSize = 100
	ids := make([]uint64, 0)
	for offset := 0; ; offset += pageSize {
		sessions, err := s.sessions.List(ctx, domain.LiveSessionFilter{
			Status: domain.LiveSessionStatusLive,
			Limit:  pageSize,
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		for _, session := range sessions {
			if session.ID != 0 {
				ids = append(ids, session.ID)
			}
		}
		if len(sessions) < pageSize {
			break
		}
	}
	return ids, nil
}

func (s *MarketplaceService) GetLot(ctx context.Context, id uint64) (domain.AuctionLot, error) {
	if s == nil || s.auctions == nil || id == 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	lot, err := s.auctions.FindByID(ctx, id)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !publicAuctionStatus(lot.Status) {
		return domain.AuctionLot{}, domain.ErrNotFound
	}
	return s.enrichLot(ctx, lot), nil
}

func (s *MarketplaceService) MyParticipations(ctx context.Context, userID string, role domain.Role, limit, offset int) ([]domain.AuctionParticipationRecord, error) {
	userID = strings.TrimSpace(userID)
	if s == nil || s.deposits == nil || userID == "" || role != domain.RoleBuyer {
		return nil, domain.ErrForbidden
	}
	deposits, err := s.deposits.ListByUser(ctx, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	records := make([]domain.AuctionParticipationRecord, 0, len(deposits))
	for _, deposit := range deposits {
		record := domain.AuctionParticipationRecord{
			ID:            strconv.FormatUint(deposit.ID, 10),
			UserID:        deposit.UserID,
			DepositAmount: deposit.Amount,
			DepositStatus: deposit.Status,
			EnrolledAt:    deposit.CreatedAt,
		}
		if s.auctions != nil {
			if lot, err := s.auctions.FindByID(ctx, deposit.AuctionID); err == nil {
				enriched := s.enrichLot(ctx, lot)
				record.Lot = &enriched
				if lot.LiveSessionID != nil && s.sessions != nil {
					if session, err := s.sessions.Get(ctx, *lot.LiveSessionID); err == nil {
						view := s.LiveSessionView(ctx, session)
						record.Room = &view
					}
				}
			}
		}
		if order, ok := s.participationOrder(ctx, deposit, userID); ok {
			record.Order = &order
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *MarketplaceService) Categories(ctx context.Context) []domain.Category {
	_ = ctx
	return defaultCategories()
}

func (s *MarketplaceService) SearchMerchants(ctx context.Context, keyword string, limit, offset int) ([]domain.MerchantView, error) {
	if s == nil || s.users == nil {
		return nil, domain.ErrNotFound
	}
	users, err := s.users.List(domain.UserFilter{
		Role:    domain.RoleMerchant,
		Status:  domain.UserStatusActive,
		Keyword: strings.TrimSpace(keyword),
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		return nil, err
	}
	merchants := make([]domain.MerchantView, 0, len(users))
	for _, user := range users {
		merchants = append(merchants, s.merchantView(ctx, user))
	}
	return merchants, nil
}

func (s *MarketplaceService) GetMerchant(ctx context.Context, merchantID string) (domain.MerchantView, error) {
	if s == nil || s.users == nil {
		return domain.MerchantView{}, domain.ErrNotFound
	}
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" {
		return domain.MerchantView{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(merchantID)
	if err != nil {
		return domain.MerchantView{}, err
	}
	if user.Role != domain.RoleMerchant || user.Status != domain.UserStatusActive {
		return domain.MerchantView{}, domain.ErrNotFound
	}
	return s.merchantView(ctx, user), nil
}

func (s *MarketplaceService) LiveSessionView(ctx context.Context, session domain.LiveSession) domain.LiveSessionView {
	view := domain.LiveSessionView{
		LiveSession:  session,
		VideoSource:  "",
		VideoURL:     "",
		DigitalHuman: map[string]interface{}{},
	}
	if s != nil && s.users != nil {
		if user, err := s.users.FindByID(session.MerchantID); err == nil {
			view.MerchantName = strings.TrimSpace(user.Nickname)
		}
	}
	if s != nil && s.hub != nil {
		if sessionCounter, ok := s.hub.(marketplaceports.LiveSessionOnlineCounter); ok {
			view.OnlineCount = sessionCounter.LiveSessionOnlineCount(session.ID)
		} else if session.ActiveAuctionID != 0 {
			view.OnlineCount = s.hub.OnlineCount(session.ActiveAuctionID)
		}
	}
	return view
}

func (s *MarketplaceService) enrichLots(ctx context.Context, lots []domain.AuctionLot) []domain.AuctionLot {
	out := make([]domain.AuctionLot, len(lots))
	for i := range lots {
		out[i] = s.enrichLot(ctx, lots[i])
	}
	return out
}

func (s *MarketplaceService) enrichLot(ctx context.Context, lot domain.AuctionLot) domain.AuctionLot {
	lot.CategoryID = categoryIDForName(lot.Category)
	lot.ImageURL = firstNonEmpty(lot.CoverURL, firstString(lot.ImageURLs))
	if lot.CurrentPrice == 0 {
		lot.CurrentPrice = lot.StartPrice
	}
	if lot.DealPrice != nil {
		lot.CurrentPrice = *lot.DealPrice
	}
	if s != nil && s.realtime != nil {
		if state, ok, err := s.realtime.GetAuctionState(ctx, lot.AuctionID); err == nil && ok {
			lot.Status = state.Status
			lot.CurrentPrice = state.CurrentPrice
			lot.LeaderBidderID = state.LeaderBidderID
			lot.BidCount = state.BidCount
			if !state.StartTime.IsZero() {
				lot.StartTime = state.StartTime
			}
			if !state.EndTime.IsZero() {
				lot.EndTime = state.EndTime
			}
		}
	}
	if s != nil && s.deposits != nil {
		if deposits, err := s.deposits.ListByAuction(ctx, lot.AuctionID); err == nil {
			lot.ParticipantCount = len(deposits)
		}
	}
	return lot
}

func (s *MarketplaceService) participationOrder(ctx context.Context, deposit domain.DepositLedger, userID string) (domain.OrderDeal, bool) {
	if s == nil || s.orders == nil {
		return domain.OrderDeal{}, false
	}
	var (
		order domain.OrderDeal
		err   error
	)
	if deposit.RelatedOrderID != nil && *deposit.RelatedOrderID != 0 {
		order, err = s.orders.FindByID(ctx, *deposit.RelatedOrderID)
	} else {
		order, err = s.orders.FindByAuctionID(ctx, deposit.AuctionID)
	}
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return domain.OrderDeal{}, false
		}
		return domain.OrderDeal{}, false
	}
	if !sameUserID(order.WinnerID, userID) {
		return domain.OrderDeal{}, false
	}
	return order, true
}

func (s *MarketplaceService) merchantView(ctx context.Context, user domain.User) domain.MerchantView {
	view := domain.MerchantView{
		ID:            user.ID,
		Name:          strings.TrimSpace(user.Nickname),
		AvatarURL:     strings.TrimSpace(user.AvatarURL),
		FollowerCount: 0,
	}
	if s == nil || s.sessions == nil {
		return view
	}
	if session, err := s.sessions.GetActiveByMerchantID(ctx, user.ID); err == nil {
		sessionView := s.LiveSessionView(ctx, session)
		view.LiveSessionID = session.ID
		view.LiveRoomID = strconv.FormatUint(session.ID, 10)
		view.CurrentLiveSession = &sessionView
		return view
	}
	sessions, err := s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: user.ID, Limit: 1})
	if err != nil || len(sessions) == 0 {
		return view
	}
	view.LiveSessionID = sessions[0].ID
	view.LiveRoomID = strconv.FormatUint(sessions[0].ID, 10)
	return view
}

func publicAuctionStatuses() []domain.AuctionStatus {
	return []domain.AuctionStatus{
		domain.AuctionStatusReady,
		domain.AuctionStatusWarmingUp,
		domain.AuctionStatusRunning,
		domain.AuctionStatusExtended,
		domain.AuctionStatusHammerPending,
		domain.AuctionStatusClosedWon,
		domain.AuctionStatusClosedFailed,
		domain.AuctionStatusSettled,
	}
}

func publicAuctionStatus(status domain.AuctionStatus) bool {
	for _, visible := range publicAuctionStatuses() {
		if status == visible {
			return true
		}
	}
	return false
}

func sameUserID(a, b string) bool {
	return normalizeUserID(a) != "" && normalizeUserID(a) == normalizeUserID(b)
}

func normalizeUserID(id string) string {
	id = strings.TrimSpace(id)
	for _, prefix := range []string{"u_", "U_"} {
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix)
		}
	}
	return id
}

func defaultCategories() []domain.Category {
	return []domain.Category{
		{ID: "jewelry", Name: "珠宝玉石", IconName: "gem"},
		{ID: "watch", Name: "腕表钟表", IconName: "watch"},
		{ID: "craft", Name: "工艺收藏", IconName: "sparkles"},
		{ID: "fashion", Name: "潮流配饰", IconName: "shopping-bag"},
		{ID: "tea", Name: "茶酒滋补", IconName: "leaf"},
		{ID: "digital", Name: "数码潮玩", IconName: "badge"},
		{ID: "painting", Name: "书画篆刻", IconName: "sparkles"},
		{ID: "ceramic", Name: "瓷器陶艺", IconName: "badge"},
		{ID: "wine", Name: "名酒陈酿", IconName: "leaf"},
		{ID: "bag", Name: "箱包皮具", IconName: "shopping-bag"},
		{ID: "coin", Name: "钱币邮票", IconName: "badge"},
		{ID: "furniture", Name: "古典家具", IconName: "sparkles"},
		{ID: "camera", Name: "影像器材", IconName: "badge"},
		{ID: "music", Name: "乐器音响", IconName: "sparkles"},
		{ID: "outdoor", Name: "户外收藏", IconName: "badge"},
	}
}

func categoryValuesForID(categoryID string) []string {
	categoryID = strings.TrimSpace(categoryID)
	if categoryID == "" {
		return nil
	}
	values := []string{categoryID}
	for _, category := range defaultCategories() {
		if category.ID == categoryID {
			values = append(values, category.Name)
			break
		}
	}
	return values
}

func categoryIDForName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	for _, category := range defaultCategories() {
		if category.ID == name || category.Name == name {
			return category.ID
		}
	}
	return name
}

func firstString(values []string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
