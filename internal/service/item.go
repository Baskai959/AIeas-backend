package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

// ItemCache 是 ItemService 期望的 Item 缓存接口；
// 通过接口而不是直接持有 *cache.LayeredCache 让 service 包不反向依赖 infra/cache，
// 同时也便于测试注入 mock。
//
// 语义约束：
//   - GetOrLoad 必须负责合并并发回源（singleflight）+ 三道防护；这里不做要求。
//   - GetOrLoad 在 loader 返回 found=false 时应返回 ErrItemNotCached（即 cache.ErrNegativeHit），
//     由 ItemService 转换为 domain.ErrNotFound。
//   - Invalidate 失败不应阻塞业务；调用方仅记日志。
type ItemCache interface {
	GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) (domain.Item, bool, error)) (domain.Item, error)
	Invalidate(ctx context.Context, keys ...string) error
}

// ErrItemNotCached 由 ItemCache 实现返回，表示缓存层已经知道该 key 在数据库中也不存在。
// service 层据此转 domain.ErrNotFound，避免反复回源。
var ErrItemNotCached = errors.New("service: item not cached (negative hit)")

const productAuditHookTimeout = 30 * time.Second

type ItemService struct {
	items                   repository.ItemRepository
	auctions                repository.AuctionRepository
	auditor                 ProductAuditor
	liveHook                *LiveAgentHookService
	productAuditCallbackURL string
	productAuditCallbackKey string
	cache                   ItemCache
}

type CreateItemInput struct {
	SellerID       string
	ActorRole      domain.Role
	Title          string
	Category       string
	Brand          string
	ConditionGrade domain.ConditionGrade
	Images         []string
	Description    string
	Status         domain.ItemStatus
	AuditImage     *ProductAuditImage
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
	AuditImage     *ProductAuditImage
}

type ProductAuditImage struct {
	ImageName   string
	ContentType string
	ImageSize   int64
	Image       []byte
}

type ProductAuditCallbackInput struct {
	ItemID          uint64
	Success         bool
	IsApproved      bool
	RejectReason    *string
	CallbackContext map[string]interface{}
}

func NewItemService(items repository.ItemRepository) *ItemService {
	return &ItemService{items: items}
}

func (s *ItemService) SetAuctionRepository(auctions repository.AuctionRepository) {
	s.auctions = auctions
}

func (s *ItemService) SetProductAuditor(auditor ProductAuditor) {
	s.auditor = auditor
}

func (s *ItemService) SetLiveAgentHookService(hook *LiveAgentHookService) {
	s.liveHook = hook
}

func (s *ItemService) SetProductAuditCallback(callbackURL, callbackAPIKey string) {
	s.productAuditCallbackURL = strings.TrimSpace(callbackURL)
	s.productAuditCallbackKey = strings.TrimSpace(callbackAPIKey)
}

// SetCache 注入 Item 缓存；nil 表示禁用缓存（直接走 repo）。
// 调用方一般通过 server.go 在装配阶段一次性设置。
func (s *ItemService) SetCache(c ItemCache) {
	s.cache = c
}

// itemCacheKey 返回 Item 在缓存层的 key（仅按 ID 维度）；当前只缓存 FindByID 这条
// 最热的查询，不缓存 List（List 受 filter 影响，命中率低且失效复杂）。
func itemCacheKey(id uint64) string {
	return strconv.FormatUint(id, 10)
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
		in.Status = domain.ItemStatusPendingAudit
	}
	if !in.Status.Valid() {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	if in.ActorRole == domain.RoleMerchant {
		in.Status = domain.ItemStatusPendingAudit
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
	if in.ActorRole == domain.RoleMerchant {
		item.Status = domain.ItemStatusPendingAudit
	}
	if err := s.items.Create(ctx, &item); err != nil {
		return domain.Item{}, err
	}
	if in.ActorRole == domain.RoleMerchant {
		s.triggerProductAuditHook(item, in.AuditImage)
	}
	return item, nil
}

func (s *ItemService) Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.Item, error) {
	item, err := s.findByID(ctx, id)
	if err != nil {
		return domain.Item{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, item.SellerID) {
		return domain.Item{}, domain.ErrForbidden
	}
	return item, nil
}

// findByID 优先走缓存（如果已注入），缓存层负责合并并发回源 + 三道防护；
// 缓存未注入时退化为直接 repo 查询。授权判定在更上层做，缓存里只放 raw 实体。
func (s *ItemService) findByID(ctx context.Context, id uint64) (domain.Item, error) {
	if s.cache == nil {
		return s.items.FindByID(ctx, id)
	}
	item, err := s.cache.GetOrLoad(ctx, itemCacheKey(id), func(ctx context.Context) (domain.Item, bool, error) {
		got, err := s.items.FindByID(ctx, id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return domain.Item{}, false, nil
			}
			return domain.Item{}, false, err
		}
		return got, true, nil
	})
	if err != nil {
		if errors.Is(err, ErrItemNotCached) {
			return domain.Item{}, domain.ErrNotFound
		}
		return domain.Item{}, err
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
	previousStatus := item.Status
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
	if in.ActorRole == domain.RoleMerchant {
		item.Status = domain.ItemStatusPendingAudit
	}
	if err := s.items.Update(ctx, &item); err != nil {
		return domain.Item{}, err
	}
	s.invalidateCache(ctx, id)
	s.emitItemStatusHook(ctx, item.SellerID, item.ID, previousStatus, item.Status)
	if in.ActorRole == domain.RoleMerchant {
		s.triggerProductAuditHook(item, in.AuditImage)
	}
	return item, nil
}

func (s *ItemService) emitItemStatusHook(ctx context.Context, sellerID string, itemID uint64, from, to domain.ItemStatus) {
	if s.liveHook == nil || from == to {
		return
	}
	switch to {
	case domain.ItemStatusListed:
		s.liveHook.EmitItemListed(ctx, sellerID, itemID)
	case domain.ItemStatusOffline:
		s.liveHook.EmitItemOffline(ctx, sellerID, itemID)
	}
}

func (s *ItemService) triggerProductAuditHook(item domain.Item, image *ProductAuditImage) {
	if s.auditor == nil || image == nil || len(image.Image) == 0 || s.productAuditCallbackURL == "" {
		return
	}
	auditItem := item
	auditImage := cloneProductAuditImage(image)
	callbackHeaders := map[string]string{}
	if s.productAuditCallbackKey != "" {
		callbackHeaders["X-Callback-Key"] = s.productAuditCallbackKey
		callbackHeaders["Authorization"] = "Bearer " + s.productAuditCallbackKey
	}
	go func() {
		defer func() { _ = recover() }()
		ctx, cancel := context.WithTimeout(context.Background(), productAuditHookTimeout)
		defer cancel()
		result, err := s.auditor.AuditProduct(ctx, ProductAuditInput{
			ProductText:     buildProductAuditText(auditItem),
			ImageName:       auditImage.ImageName,
			ContentType:     auditImage.ContentType,
			ImageSize:       auditImage.ImageSize,
			Image:           append([]byte(nil), auditImage.Image...),
			CallbackURL:     s.productAuditCallbackURL,
			CallbackHeaders: callbackHeaders,
			CallbackContext: buildProductAuditCallbackContext(auditItem),
		})
		if err != nil {
			if !errors.Is(err, ErrProductAuditUnavailable) {
				slog.Default().Warn("product audit hook failed", "item_id", auditItem.ID, "error", err)
			}
			return
		}
		if !result.Success {
			slog.Default().Warn("product audit hook not accepted", "item_id", auditItem.ID, "status", result.Status, "message", result.Message)
			return
		}
	}()
}

func (s *ItemService) HandleProductAuditCallback(ctx context.Context, in ProductAuditCallbackInput) (domain.Item, error) {
	itemID := in.ItemID
	if itemID == 0 {
		itemID = uint64FromCallbackContext(in.CallbackContext, "itemId", "item_id")
	}
	if itemID == 0 {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	item, err := s.items.FindByID(ctx, itemID)
	if err != nil {
		return domain.Item{}, err
	}
	if !in.Success {
		return item, nil
	}
	snapshot, ok := productAuditSnapshotFromCallbackContext(in.CallbackContext)
	if !ok {
		return domain.Item{}, domain.ErrInvalidArgument
	}
	if !sameProductAuditSnapshot(item, snapshot) {
		return item, nil
	}
	if item.Status != domain.ItemStatusPendingAudit {
		return item, nil
	}
	nextStatus := domain.ItemStatusRejected
	if in.IsApproved {
		nextStatus = domain.ItemStatusReady
	}
	item.Status = nextStatus
	if err := s.items.Update(ctx, &item); err != nil {
		return domain.Item{}, err
	}
	s.invalidateCache(ctx, item.ID)
	return item, nil
}

func cloneProductAuditImage(image *ProductAuditImage) ProductAuditImage {
	if image == nil {
		return ProductAuditImage{}
	}
	return ProductAuditImage{
		ImageName:   image.ImageName,
		ContentType: image.ContentType,
		ImageSize:   image.ImageSize,
		Image:       append([]byte(nil), image.Image...),
	}
}

func sameProductAuditSnapshot(current, snapshot domain.Item) bool {
	return strings.TrimSpace(current.Title) == strings.TrimSpace(snapshot.Title) &&
		strings.TrimSpace(current.Category) == strings.TrimSpace(snapshot.Category) &&
		strings.TrimSpace(current.Brand) == strings.TrimSpace(snapshot.Brand) &&
		current.ConditionGrade == snapshot.ConditionGrade &&
		strings.TrimSpace(current.Description) == strings.TrimSpace(snapshot.Description) &&
		string(current.Images) == string(snapshot.Images)
}

func buildProductAuditCallbackContext(item domain.Item) map[string]interface{} {
	return map[string]interface{}{
		"itemId":   item.ID,
		"sellerId": strings.TrimSpace(item.SellerID),
		"snapshot": map[string]interface{}{
			"title":          strings.TrimSpace(item.Title),
			"category":       strings.TrimSpace(item.Category),
			"brand":          strings.TrimSpace(item.Brand),
			"conditionGrade": item.ConditionGrade,
			"description":    strings.TrimSpace(item.Description),
			"images":         productAuditSnapshotImages(item.Images),
		},
	}
}

func productAuditSnapshotImages(raw json.RawMessage) []string {
	var images []string
	if len(raw) == 0 || json.Unmarshal(raw, &images) != nil {
		return []string{}
	}
	return images
}

func productAuditSnapshotFromCallbackContext(ctx map[string]interface{}) (domain.Item, bool) {
	raw, ok := ctx["snapshot"]
	if !ok || raw == nil {
		return domain.Item{}, false
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return domain.Item{}, false
	}
	var snapshot struct {
		Title          string                `json:"title"`
		Category       string                `json:"category"`
		Brand          string                `json:"brand"`
		ConditionGrade domain.ConditionGrade `json:"conditionGrade"`
		Images         []string              `json:"images"`
		Description    string                `json:"description"`
	}
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return domain.Item{}, false
	}
	if strings.TrimSpace(snapshot.Title) == "" || strings.TrimSpace(snapshot.Category) == "" || !snapshot.ConditionGrade.Valid() {
		return domain.Item{}, false
	}
	images, err := json.Marshal(snapshot.Images)
	if err != nil {
		return domain.Item{}, false
	}
	return domain.Item{
		Title:          strings.TrimSpace(snapshot.Title),
		Category:       strings.TrimSpace(snapshot.Category),
		Brand:          strings.TrimSpace(snapshot.Brand),
		ConditionGrade: snapshot.ConditionGrade,
		Images:         images,
		Description:    strings.TrimSpace(snapshot.Description),
	}, true
}

func buildProductAuditText(item domain.Item) string {
	parts := []string{
		"商品标题：" + strings.TrimSpace(item.Title),
		"类目：" + strings.TrimSpace(item.Category),
		"成色：" + string(item.ConditionGrade),
	}
	if brand := strings.TrimSpace(item.Brand); brand != "" {
		parts = append(parts, "品牌："+brand)
	}
	if description := strings.TrimSpace(item.Description); description != "" {
		parts = append(parts, "卖点："+description)
	}
	return strings.Join(parts, "；") + "。"
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
	if err := s.items.Delete(ctx, id); err != nil {
		return err
	}
	s.invalidateCache(ctx, id)
	return nil
}

// invalidateCache 在写路径成功后异步式失效；失败仅吞掉，避免业务被缓存层拖累。
// （cache.Invalidate 内部已经先清 L1 即时生效，再清 L2；L2 失败不会让进程数据错误。）
func (s *ItemService) invalidateCache(ctx context.Context, id uint64) {
	if s.cache == nil {
		return
	}
	_ = s.cache.Invalidate(ctx, itemCacheKey(id))
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
