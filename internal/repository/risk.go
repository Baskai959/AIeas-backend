package repository

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type RiskRepository interface {
	IsBlacklisted(ctx context.Context, userID string, now time.Time) (bool, error)
	CreateBlacklist(ctx context.Context, item *domain.Blacklist) error
	DeleteBlacklist(ctx context.Context, userID string) error
	ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error)
	CreateEvent(ctx context.Context, event *domain.RiskEvent) error
	ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
	UpdateEvent(ctx context.Context, event *domain.RiskEvent) error
	FindEventByID(ctx context.Context, id uint64) (domain.RiskEvent, error)
}

type MySQLRiskRepository struct {
	db *gorm.DB
}

func NewMySQLRiskRepository(db *gorm.DB) *MySQLRiskRepository {
	return &MySQLRiskRepository{db: db}
}

func (r *MySQLRiskRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLRiskRepository) IsBlacklisted(ctx context.Context, userID string, now time.Time) (bool, error) {
	var row blacklistRow
	err := r.dbFor(ctx).Table("blacklist").Where("user_id = ? AND (expires_at IS NULL OR expires_at > ?)", normalizeUserIDForDB(userID), now).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *MySQLRiskRepository) CreateBlacklist(ctx context.Context, item *domain.Blacklist) error {
	row := blacklistRowFromDomain(*item)
	if err := r.dbFor(ctx).Table("blacklist").Create(&row).Error; err != nil {
		return err
	}
	*item = row.toDomain()
	return nil
}

func (r *MySQLRiskRepository) DeleteBlacklist(ctx context.Context, userID string) error {
	res := r.dbFor(ctx).Table("blacklist").Where("user_id = ?", normalizeUserIDForDB(userID)).Delete(&blacklistRow{})
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (r *MySQLRiskRepository) ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var rows []blacklistRow
	if err := r.dbFor(ctx).Table("blacklist").Order("id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]domain.Blacklist, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.toDomain())
	}
	return items, nil
}

func (r *MySQLRiskRepository) CreateEvent(ctx context.Context, event *domain.RiskEvent) error {
	row := riskEventRowFromDomain(*event)
	if err := r.dbFor(ctx).Table("risk_event").Create(&row).Error; err != nil {
		return err
	}
	*event = row.toDomain()
	return nil
}

func (r *MySQLRiskRepository) FindEventByID(ctx context.Context, id uint64) (domain.RiskEvent, error) {
	var row riskEventRow
	err := r.dbFor(ctx).Table("risk_event").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.RiskEvent{}, domain.ErrNotFound
		}
		return domain.RiskEvent{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLRiskRepository) ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error) {
	query := r.dbFor(ctx).Table("risk_event").Order("id DESC")
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.EventType != "" {
		query = query.Where("event_type = ?", filter.EventType)
	}
	if filter.UserID != "" {
		query = query.Where("user_id = ?", normalizeUserIDForDB(filter.UserID))
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []riskEventRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	events := make([]domain.RiskEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, row.toDomain())
	}
	return events, nil
}

func (r *MySQLRiskRepository) UpdateEvent(ctx context.Context, event *domain.RiskEvent) error {
	row := riskEventRowFromDomain(*event)
	res := r.dbFor(ctx).Table("risk_event").Where("id = ?", event.ID).Updates(map[string]interface{}{
		"status":      row.Status,
		"reviewed_by": row.ReviewedBy,
		"reviewed_at": row.ReviewedAt,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindEventByID(ctx, event.ID)
	if err != nil {
		return err
	}
	*event = updated
	return nil
}

type blacklistRow struct {
	ID        uint64     `gorm:"column:id;primaryKey"`
	UserID    string     `gorm:"column:user_id"`
	Reason    string     `gorm:"column:reason"`
	CreatedBy string     `gorm:"column:created_by"`
	CreatedAt time.Time  `gorm:"column:created_at"`
	ExpiresAt *time.Time `gorm:"column:expires_at"`
}

func blacklistRowFromDomain(item domain.Blacklist) blacklistRow {
	return blacklistRow{
		ID:        item.ID,
		UserID:    normalizeUserIDForDB(item.UserID),
		Reason:    item.Reason,
		CreatedBy: normalizeUserIDForDB(item.CreatedBy),
		CreatedAt: item.CreatedAt,
		ExpiresAt: item.ExpiresAt,
	}
}

func (r blacklistRow) toDomain() domain.Blacklist {
	return domain.Blacklist{
		ID:        r.ID,
		UserID:    r.UserID,
		Reason:    r.Reason,
		CreatedBy: r.CreatedBy,
		CreatedAt: r.CreatedAt,
		ExpiresAt: r.ExpiresAt,
	}
}

type riskEventRow struct {
	ID         uint64                 `gorm:"column:id;primaryKey"`
	EventType  string                 `gorm:"column:event_type"`
	UserID     string                 `gorm:"column:user_id"`
	AuctionID  uint64                 `gorm:"column:auction_id"`
	Severity   domain.RiskSeverity    `gorm:"column:severity"`
	Payload    []byte                 `gorm:"column:payload"`
	Status     domain.RiskEventStatus `gorm:"column:status"`
	ReviewedBy string                 `gorm:"column:reviewed_by"`
	ReviewedAt *time.Time             `gorm:"column:reviewed_at"`
	CreatedAt  time.Time              `gorm:"column:created_at"`
}

func riskEventRowFromDomain(event domain.RiskEvent) riskEventRow {
	return riskEventRow{
		ID:         event.ID,
		EventType:  event.EventType,
		UserID:     normalizeUserIDForDB(event.UserID),
		AuctionID:  event.AuctionID,
		Severity:   event.Severity,
		Payload:    []byte(event.Payload),
		Status:     event.Status,
		ReviewedBy: normalizeUserIDForDB(event.ReviewedBy),
		ReviewedAt: event.ReviewedAt,
		CreatedAt:  event.CreatedAt,
	}
}

func (r riskEventRow) toDomain() domain.RiskEvent {
	return domain.RiskEvent{
		ID:         r.ID,
		EventType:  r.EventType,
		UserID:     r.UserID,
		AuctionID:  r.AuctionID,
		Severity:   r.Severity,
		Payload:    append([]byte(nil), r.Payload...),
		Status:     r.Status,
		ReviewedBy: r.ReviewedBy,
		ReviewedAt: r.ReviewedAt,
		CreatedAt:  r.CreatedAt,
	}
}

type MemoryRiskRepository struct {
	mu         sync.RWMutex
	nextBL     uint64
	nextEvent  uint64
	blacklist  map[string]domain.Blacklist
	riskEvents map[uint64]domain.RiskEvent
}

func NewMemoryRiskRepository() *MemoryRiskRepository {
	return &MemoryRiskRepository{
		nextBL:     1,
		nextEvent:  1,
		blacklist:  make(map[string]domain.Blacklist),
		riskEvents: make(map[uint64]domain.RiskEvent),
	}
}

func (r *MemoryRiskRepository) IsBlacklisted(ctx context.Context, userID string, now time.Time) (bool, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.blacklist[userID]
	if !ok {
		return false, nil
	}
	if item.ExpiresAt != nil && !item.ExpiresAt.After(now) {
		return false, nil
	}
	return true, nil
}

func (r *MemoryRiskRepository) CreateBlacklist(ctx context.Context, item *domain.Blacklist) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.blacklist[item.UserID]; ok {
		*item = existing
		return domain.ErrConflict
	}
	if item.ID == 0 {
		item.ID = r.nextBL
		r.nextBL++
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	r.blacklist[item.UserID] = *item
	return nil
}

func (r *MemoryRiskRepository) DeleteBlacklist(ctx context.Context, userID string) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.blacklist, userID)
	return nil
}

func (r *MemoryRiskRepository) ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]domain.Blacklist, 0, len(r.blacklist))
	for _, item := range r.blacklist {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	return paginateBlacklist(items, limit, offset), nil
}

func (r *MemoryRiskRepository) CreateEvent(ctx context.Context, event *domain.RiskEvent) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if event.ID == 0 {
		event.ID = r.nextEvent
		r.nextEvent++
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if event.Status == "" {
		event.Status = domain.RiskEventPending
	}
	r.riskEvents[event.ID] = cloneRiskEvent(*event)
	return nil
}

func (r *MemoryRiskRepository) FindEventByID(ctx context.Context, id uint64) (domain.RiskEvent, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	event, ok := r.riskEvents[id]
	if !ok {
		return domain.RiskEvent{}, domain.ErrNotFound
	}
	return cloneRiskEvent(event), nil
}

func (r *MemoryRiskRepository) ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	events := make([]domain.RiskEvent, 0, len(r.riskEvents))
	for _, event := range r.riskEvents {
		if filter.Status != "" && event.Status != filter.Status {
			continue
		}
		if filter.EventType != "" && event.EventType != filter.EventType {
			continue
		}
		if filter.UserID != "" && event.UserID != filter.UserID {
			continue
		}
		events = append(events, cloneRiskEvent(event))
	}
	sort.Slice(events, func(i, j int) bool { return events[i].ID > events[j].ID })
	return paginateRiskEvents(events, filter.Limit, filter.Offset), nil
}

func (r *MemoryRiskRepository) UpdateEvent(ctx context.Context, event *domain.RiskEvent) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.riskEvents[event.ID]; !ok {
		return domain.ErrNotFound
	}
	r.riskEvents[event.ID] = cloneRiskEvent(*event)
	return nil
}

func cloneRiskEvent(event domain.RiskEvent) domain.RiskEvent {
	event.Payload = append([]byte(nil), event.Payload...)
	return event
}

func paginateBlacklist(items []domain.Blacklist, limit, offset int) []domain.Blacklist {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(items) {
		return []domain.Blacklist{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func paginateRiskEvents(events []domain.RiskEvent, limit, offset int) []domain.RiskEvent {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(events) {
		return []domain.RiskEvent{}
	}
	end := offset + limit
	if end > len(events) {
		end = len(events)
	}
	return events[offset:end]
}
