package repository

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	livesessionports "aieas_backend/internal/modules/live_session/ports"

	"gorm.io/gorm"
)

type ContextDBResolver func(ctx context.Context, db *gorm.DB) *gorm.DB

type MySQLLiveSessionRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

type MemoryLiveSessionRepository struct {
	mu       sync.RWMutex
	nextID   uint64
	sessions map[uint64]domain.LiveSession
}

func NewMySQLLiveSessionRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLLiveSessionRepository {
	return &MySQLLiveSessionRepository{db: db, resolveDB: resolver}
}

func NewMemoryLiveSessionRepository() *MemoryLiveSessionRepository {
	return &MemoryLiveSessionRepository{nextID: 70001, sessions: make(map[uint64]domain.LiveSession)}
}

func (r *MySQLLiveSessionRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLLiveSessionRepository) Create(ctx context.Context, session *domain.LiveSession) error {
	row := liveSessionRowFromDomain(*session)
	if err := r.dbFor(ctx).Table("live_session").Create(&row).Error; err != nil {
		return err
	}
	*session = row.toDomain()
	return nil
}

func (r *MySQLLiveSessionRepository) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	var row liveSessionRow
	err := r.dbFor(ctx).Table("live_session").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveSession{}, domain.ErrNotFound
		}
		return domain.LiveSession{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveSessionRepository) GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error) {
	var row liveSessionRow
	err := r.dbFor(ctx).Table("live_session").
		Where("merchant_id = ? AND status = ?", normalizeUserIDForDB(merchantID), domain.LiveSessionStatusLive).
		Order("id DESC").
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveSession{}, domain.ErrNotFound
		}
		return domain.LiveSession{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveSessionRepository) Update(ctx context.Context, session *domain.LiveSession) error {
	row := liveSessionRowFromDomain(*session)
	updates := map[string]interface{}{
		"merchant_id":          row.MerchantID,
		"title":                row.Title,
		"description":          row.Description,
		"cover_url":            row.CoverURL,
		"status":               row.Status,
		"active_auction_id":    row.ActiveAuctionID,
		"opened_at":            row.OpenedAt,
		"closed_at":            row.ClosedAt,
		"scheduled_start_time": row.ScheduledStartTime,
		"planned_duration_sec": row.PlannedDurationSec,
		"lots_total":           row.LotsTotal,
		"lots_sold":            row.LotsSold,
		"lots_unsold":          row.LotsUnsold,
		"bid_count":            row.BidCount,
		"gmv_cent":             row.GMVCent,
		"viewer_peak":          row.ViewerPeak,
		"viewer_total":         row.ViewerTotal,
		"updated_at":           time.Now().UTC(),
	}
	res := r.dbFor(ctx).Table("live_session").Where("id = ?", session.ID).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.Get(ctx, session.ID)
	if err != nil {
		return err
	}
	*session = updated
	return nil
}

func (r *MySQLLiveSessionRepository) List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error) {
	query := r.dbFor(ctx).Table("live_session")
	if filter.MerchantID != "" {
		query = query.Where("merchant_id = ?", normalizeUserIDForDB(filter.MerchantID))
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where("title LIKE ? OR description LIKE ? OR merchant_id LIKE ?", like, like, like)
	}
	if filter.OpenedFrom != nil {
		query = query.Where("opened_at >= ?", *filter.OpenedFrom)
	}
	if filter.OpenedTo != nil {
		query = query.Where("opened_at <= ?", *filter.OpenedTo)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []liveSessionRow
	if err := query.Order(liveSessionListOrder(filter.Sort)).Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	sessions := make([]domain.LiveSession, 0, len(rows))
	for _, row := range rows {
		sessions = append(sessions, row.toDomain())
	}
	return sessions, nil
}

func (r *MemoryLiveSessionRepository) Create(ctx context.Context, session *domain.LiveSession) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if session.ID == 0 {
		session.ID = r.nextID
		r.nextID++
	} else if session.ID >= r.nextID {
		r.nextID = session.ID + 1
	}
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	session.UpdatedAt = now
	r.sessions[session.ID] = *session
	return nil
}

func (r *MemoryLiveSessionRepository) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[id]
	if !ok {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	return session, nil
}

func (r *MemoryLiveSessionRepository) GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	var (
		latest    domain.LiveSession
		latestSet bool
	)
	for _, session := range r.sessions {
		if session.MerchantID != merchantID || session.Status != domain.LiveSessionStatusLive {
			continue
		}
		if !latestSet || session.ID > latest.ID {
			latest = session
			latestSet = true
		}
	}
	if !latestSet {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	return latest, nil
}

func (r *MemoryLiveSessionRepository) Update(ctx context.Context, session *domain.LiveSession) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[session.ID]; !ok {
		return domain.ErrNotFound
	}
	session.UpdatedAt = time.Now().UTC()
	r.sessions[session.ID] = *session
	return nil
}

func (r *MemoryLiveSessionRepository) SnapshotLiveSessions() []domain.LiveSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.LiveSession, 0, len(r.sessions))
	for _, session := range r.sessions {
		out = append(out, session)
	}
	return out
}

func (r *MemoryLiveSessionRepository) List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := make([]domain.LiveSession, 0, len(r.sessions))
	keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
	for _, session := range r.sessions {
		if filter.MerchantID != "" && session.MerchantID != filter.MerchantID {
			continue
		}
		if filter.Status.Valid() && session.Status != filter.Status {
			continue
		}
		if keyword != "" && !liveSessionMatchesKeyword(session, keyword) {
			continue
		}
		if filter.OpenedFrom != nil && (session.OpenedAt == nil || session.OpenedAt.Before(*filter.OpenedFrom)) {
			continue
		}
		if filter.OpenedTo != nil && (session.OpenedAt == nil || session.OpenedAt.After(*filter.OpenedTo)) {
			continue
		}
		sessions = append(sessions, session)
	}
	sortLiveSessions(sessions, filter.Sort)
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset >= len(sessions) {
		return []domain.LiveSession{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(sessions) {
		end = len(sessions)
	}
	return sessions[filter.Offset:end], nil
}

type liveSessionRow struct {
	ID                 uint64                   `gorm:"column:id;primaryKey"`
	MerchantID         string                   `gorm:"column:merchant_id"`
	Title              string                   `gorm:"column:title"`
	Description        string                   `gorm:"column:description"`
	CoverURL           string                   `gorm:"column:cover_url"`
	Status             domain.LiveSessionStatus `gorm:"column:status"`
	ActiveAuctionID    uint64                   `gorm:"column:active_auction_id"`
	OpenedAt           *time.Time               `gorm:"column:opened_at"`
	ClosedAt           *time.Time               `gorm:"column:closed_at"`
	ScheduledStartTime *time.Time               `gorm:"column:scheduled_start_time"`
	PlannedDurationSec int                      `gorm:"column:planned_duration_sec"`
	LotsTotal          int                      `gorm:"column:lots_total"`
	LotsSold           int                      `gorm:"column:lots_sold"`
	LotsUnsold         int                      `gorm:"column:lots_unsold"`
	BidCount           int                      `gorm:"column:bid_count"`
	GMVCent            int64                    `gorm:"column:gmv_cent"`
	ViewerPeak         int                      `gorm:"column:viewer_peak"`
	ViewerTotal        int                      `gorm:"column:viewer_total"`
	CreatedAt          time.Time                `gorm:"column:created_at"`
	UpdatedAt          time.Time                `gorm:"column:updated_at"`
}

func liveSessionRowFromDomain(s domain.LiveSession) liveSessionRow {
	return liveSessionRow{
		ID:                 s.ID,
		MerchantID:         normalizeUserIDForDB(s.MerchantID),
		Title:              s.Title,
		Description:        s.Description,
		CoverURL:           s.CoverURL,
		Status:             s.Status,
		ActiveAuctionID:    s.ActiveAuctionID,
		OpenedAt:           cloneTimePtrUTC(s.OpenedAt),
		ClosedAt:           s.ClosedAt,
		ScheduledStartTime: s.ScheduledStartTime,
		PlannedDurationSec: s.PlannedDurationSec,
		LotsTotal:          s.LotsTotal,
		LotsSold:           s.LotsSold,
		LotsUnsold:         s.LotsUnsold,
		BidCount:           s.BidCount,
		GMVCent:            s.GMVCent,
		ViewerPeak:         s.ViewerPeak,
		ViewerTotal:        s.ViewerTotal,
		CreatedAt:          s.CreatedAt,
		UpdatedAt:          s.UpdatedAt,
	}
}

func (r liveSessionRow) toDomain() domain.LiveSession {
	return domain.LiveSession{
		ID:                 r.ID,
		MerchantID:         r.MerchantID,
		Title:              r.Title,
		Description:        r.Description,
		CoverURL:           r.CoverURL,
		Status:             r.Status,
		ActiveAuctionID:    r.ActiveAuctionID,
		OpenedAt:           cloneTimePtrUTC(r.OpenedAt),
		ClosedAt:           r.ClosedAt,
		ScheduledStartTime: r.ScheduledStartTime,
		PlannedDurationSec: r.PlannedDurationSec,
		LotsTotal:          r.LotsTotal,
		LotsSold:           r.LotsSold,
		LotsUnsold:         r.LotsUnsold,
		BidCount:           r.BidCount,
		GMVCent:            r.GMVCent,
		ViewerPeak:         r.ViewerPeak,
		ViewerTotal:        r.ViewerTotal,
		CreatedAt:          r.CreatedAt,
		UpdatedAt:          r.UpdatedAt,
	}
}

func liveSessionListOrder(sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case "oldest", "createdAtAsc":
		return "id ASC"
	case "startTimeAsc", "scheduledStartAsc":
		return "scheduled_start_time IS NULL ASC, scheduled_start_time ASC, id ASC"
	case "startTimeDesc", "scheduledStartDesc":
		return "scheduled_start_time IS NULL ASC, scheduled_start_time DESC, id DESC"
	case "openedAtAsc":
		return "opened_at IS NULL ASC, opened_at ASC, id ASC"
	case "openedAtDesc":
		return "opened_at IS NULL ASC, opened_at DESC, id DESC"
	case "gmvDesc":
		return "gmv_cent DESC, id DESC"
	case "viewerDesc", "viewerPeakDesc":
		return "viewer_peak DESC, id DESC"
	case "latest", "newest", "createdAtDesc":
		fallthrough
	default:
		return "id DESC"
	}
}

func liveSessionMatchesKeyword(session domain.LiveSession, keyword string) bool {
	return strings.Contains(strings.ToLower(session.Title), keyword) ||
		strings.Contains(strings.ToLower(session.Description), keyword) ||
		strings.Contains(strings.ToLower(session.MerchantID), keyword)
}

func sortLiveSessions(sessions []domain.LiveSession, sortBy string) {
	sort.SliceStable(sessions, func(i, j int) bool {
		left, right := sessions[i], sessions[j]
		switch strings.TrimSpace(sortBy) {
		case "oldest", "createdAtAsc":
			return left.ID < right.ID
		case "startTimeAsc", "scheduledStartAsc":
			return timeBeforePtr(left.ScheduledStartTime, right.ScheduledStartTime, true, left.ID, right.ID)
		case "startTimeDesc", "scheduledStartDesc":
			return timeAfterPtr(left.ScheduledStartTime, right.ScheduledStartTime, true, left.ID, right.ID)
		case "openedAtAsc":
			return timeBeforePtr(left.OpenedAt, right.OpenedAt, true, left.ID, right.ID)
		case "openedAtDesc":
			return timeAfterPtr(left.OpenedAt, right.OpenedAt, true, left.ID, right.ID)
		case "gmvDesc":
			if left.GMVCent != right.GMVCent {
				return left.GMVCent > right.GMVCent
			}
			return left.ID > right.ID
		case "viewerDesc", "viewerPeakDesc":
			if left.ViewerPeak != right.ViewerPeak {
				return left.ViewerPeak > right.ViewerPeak
			}
			return left.ID > right.ID
		case "latest", "newest", "createdAtDesc":
			fallthrough
		default:
			return left.ID > right.ID
		}
	})
}

func timeBeforePtr(left, right *time.Time, nullLast bool, leftID, rightID uint64) bool {
	if left == nil || right == nil {
		if left == nil && right == nil {
			return leftID < rightID
		}
		if nullLast {
			return left != nil
		}
		return left == nil
	}
	if !left.Equal(*right) {
		return left.Before(*right)
	}
	return leftID < rightID
}

func timeAfterPtr(left, right *time.Time, nullLast bool, leftID, rightID uint64) bool {
	if left == nil || right == nil {
		if left == nil && right == nil {
			return leftID > rightID
		}
		if nullLast {
			return right == nil
		}
		return left != nil
	}
	if !left.Equal(*right) {
		return left.After(*right)
	}
	return leftID > rightID
}

func cloneTimePtrUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}

func normalizeUserIDForDB(userID string) string {
	return strings.TrimSpace(userID)
}

var _ livesessionports.LiveSessionRepository = (*MySQLLiveSessionRepository)(nil)
var _ livesessionports.LiveSessionRepository = (*MemoryLiveSessionRepository)(nil)
