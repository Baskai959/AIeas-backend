package service

import (
	"context"
	"encoding/json"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type ItemService struct {
	items    repository.ItemRepository
	auctions repository.AuctionRepository
}

type CreateItemInput struct {
	SellerID       string
	Title          string
	Category       string
	Brand          string
	ConditionGrade domain.ConditionGrade
	Images         []string
	Description    string
	Status         domain.ItemStatus
}

type UpdateItemInput struct {
	ActorID        string
	ActorRole      domain.Role
	Title          *string
	Category       *string
	Brand          *string
	ConditionGrade *domain.ConditionGrade
	Images         *[]string
	Description    *string
	Status         *domain.ItemStatus
}

func NewItemService(items repository.ItemRepository) *ItemService {
	return &ItemService{items: items}
}

func (s *ItemService) SetAuctionRepository(auctions repository.AuctionRepository) {
	s.auctions = auctions
}

func (s *ItemService) Create(ctx context.Context, in CreateItemInput) (domain.Item, error) {
	title := strings.TrimSpace(in.Title)
	category := strings.TrimSpace(in.Category)
	if title == "" || category == "" || strings.TrimSpace(in.SellerID) == "" {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	if in.ConditionGrade == "" {
		in.ConditionGrade = domain.ConditionNew
	}
	if !in.ConditionGrade.Valid() {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	if in.Status == "" {
		in.Status = domain.ItemStatusDraft
	}
	if !in.Status.Valid() {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	if in.Images == nil {
		in.Images = []string{}
	}
	images, err := json.Marshal(in.Images)
	if err != nil {
		return domain.Item{}, err
	}
	item := domain.Item{
		SellerID:       in.SellerID,
		Title:          title,
		Category:       category,
		Brand:          strings.TrimSpace(in.Brand),
		ConditionGrade: in.ConditionGrade,
		Images:         images,
		Description:    strings.TrimSpace(in.Description),
		Status:         in.Status,
	}
	if err := s.items.Create(ctx, &item); err != nil {
		return domain.Item{}, err
	}
	return item, nil
}

func (s *ItemService) Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.Item, error) {
	item, err := s.items.FindByID(ctx, id)
	if err != nil {
		return domain.Item{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, item.SellerID) {
		return domain.Item{}, domain.ErrForbidden
	}
	return item, nil
}

func (s *ItemService) List(ctx context.Context, filter domain.ItemFilter, actorID string, actorRole domain.Role) ([]domain.Item, error) {
	if actorRole == domain.RoleMerchant {
		filter.SellerID = actorID
	}
	return s.items.List(ctx, filter)
}

func (s *ItemService) Update(ctx context.Context, id uint64, in UpdateItemInput) (domain.Item, error) {
	item, err := s.items.FindByID(ctx, id)
	if err != nil {
		return domain.Item{}, err
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, item.SellerID) {
		return domain.Item{}, domain.ErrForbidden
	}
	if hasActiveAuction, err := s.hasActiveAuction(ctx, id); err != nil {
		return domain.Item{}, err
	} else if hasActiveAuction && hasCriticalItemPatch(in) {
		return domain.Item{}, domain.ErrInvalidState
	}
	if in.Title != nil {
		title := strings.TrimSpace(*in.Title)
		if title == "" {
			return domain.Item{}, domain.ErrInvalidArgument
		}
		item.Title = title
	}
	if in.Category != nil {
		category := strings.TrimSpace(*in.Category)
		if category == "" {
			return domain.Item{}, domain.ErrInvalidArgument
		}
		item.Category = category
	}
	if in.Brand != nil {
		item.Brand = strings.TrimSpace(*in.Brand)
	}
	if in.ConditionGrade != nil {
		if !in.ConditionGrade.Valid() {
			return domain.Item{}, domain.ErrInvalidArgument
		}
		item.ConditionGrade = *in.ConditionGrade
	}
	if in.Images != nil {
		images, err := json.Marshal(*in.Images)
		if err != nil {
			return domain.Item{}, err
		}
		item.Images = images
	}
	if in.Description != nil {
		item.Description = strings.TrimSpace(*in.Description)
	}
	if in.Status != nil {
		if !in.Status.Valid() {
			return domain.Item{}, domain.ErrInvalidArgument
		}
		item.Status = *in.Status
	}
	if err := s.items.Update(ctx, &item); err != nil {
		return domain.Item{}, err
	}
	return item, nil
}

func (s *ItemService) Delete(ctx context.Context, id uint64, actorID string, actorRole domain.Role) error {
	item, err := s.items.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if !canAccessSellerOwned(actorID, actorRole, item.SellerID) {
		return domain.ErrForbidden
	}
	if hasActiveAuction, err := s.hasActiveAuction(ctx, id); err != nil {
		return err
	} else if hasActiveAuction {
		return domain.ErrInvalidState
	}
	return s.items.Delete(ctx, id)
}

func (s *ItemService) hasActiveAuction(ctx context.Context, itemID uint64) (bool, error) {
	if s.auctions == nil {
		return false, nil
	}
	auctions, err := s.auctions.List(ctx, domain.AuctionFilter{ItemID: itemID, Limit: 100})
	if err != nil {
		return false, err
	}
	for _, auction := range auctions {
		if auctionStatusBlocksItemMutation(auction.Status) {
			return true, nil
		}
	}
	return false, nil
}

func auctionStatusBlocksItemMutation(status domain.AuctionStatus) bool {
	switch status {
	case domain.AuctionStatusWarmingUp, domain.AuctionStatusRunning, domain.AuctionStatusExtended, domain.AuctionStatusHammerPending:
		return true
	default:
		return false
	}
}

func hasCriticalItemPatch(in UpdateItemInput) bool {
	return in.Title != nil || in.Category != nil || in.ConditionGrade != nil || in.Images != nil || in.Status != nil
}

func canAccessSellerOwned(actorID string, actorRole domain.Role, sellerID string) bool {
	if actorRole == domain.RoleAdmin {
		return true
	}
	return actorRole == domain.RoleMerchant && actorID != "" && actorID == sellerID
}
