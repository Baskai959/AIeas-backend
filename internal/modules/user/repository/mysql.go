package repository

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	marketplaceports "aieas_backend/internal/modules/marketplace/ports"
	userports "aieas_backend/internal/modules/user/ports"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MySQLUserRepository struct {
	db *gorm.DB
}

func NewMySQLUserRepository(db *gorm.DB) *MySQLUserRepository {
	return &MySQLUserRepository{db: db}
}

func (r *MySQLUserRepository) FindByAccountAndRole(account string, role domain.Role) (domain.User, error) {
	var row userRow
	err := r.db.Table("user").
		Select("id, account, nickname, avatar_url, location, role, status, ai_permission, password_hash").
		Where("account = ? AND role IN ?", account, roleAliases(role)).
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.User{}, domain.ErrUserNotFound
		}
		return domain.User{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLUserRepository) FindByID(id string) (domain.User, error) {
	var row userRow
	err := r.db.Table("user").
		Select("id, account, nickname, avatar_url, location, role, status, ai_permission, password_hash").
		Where("id = ?", normalizeUserIDForDB(id)).
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.User{}, domain.ErrUserNotFound
		}
		return domain.User{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLUserRepository) List(filter domain.UserFilter) ([]domain.User, error) {
	query := r.db.Table("user").Select("id, account, nickname, avatar_url, location, role, status, ai_permission, password_hash").Order("id DESC")
	if filter.Role.Valid() {
		query = query.Where("role IN ?", roleAliases(filter.Role))
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where("id = ? OR account LIKE ? OR nickname LIKE ?", normalizeUserIDForDB(keyword), like, like)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []userRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	users := make([]domain.User, 0, len(rows))
	for _, row := range rows {
		users = append(users, row.toDomain())
	}
	return users, nil
}

func (r *MySQLUserRepository) Update(user *domain.User) error {
	res := r.db.Table("user").Where("id = ?", normalizeUserIDForDB(user.ID)).Updates(map[string]interface{}{
		"nickname":      user.Nickname,
		"avatar_url":    user.AvatarURL,
		"location":      user.Location,
		"status":        user.Status,
		"ai_permission": domain.NormalizeMerchantAIPermission(user.AIPermission),
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrUserNotFound
	}
	updated, err := r.FindByID(user.ID)
	if err != nil {
		return err
	}
	*user = updated
	return nil
}

func (r *MySQLUserRepository) FollowMerchant(ctx context.Context, buyerID, merchantID string) (domain.MerchantFollow, error) {
	buyerID = normalizeUserIDForDB(buyerID)
	merchantID = normalizeUserIDForDB(merchantID)
	if buyerID == "" || merchantID == "" {
		return domain.MerchantFollow{}, domain.ErrInvalidArgument
	}
	row := merchantFollowRow{BuyerID: buyerID, MerchantID: merchantID, CreatedAt: time.Now().UTC()}
	if err := r.db.WithContext(ctx).Table("merchant_follow").Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return domain.MerchantFollow{}, err
	}
	return r.findMerchantFollow(ctx, buyerID, merchantID)
}

func (r *MySQLUserRepository) UnfollowMerchant(ctx context.Context, buyerID, merchantID string) error {
	buyerID = normalizeUserIDForDB(buyerID)
	merchantID = normalizeUserIDForDB(merchantID)
	if buyerID == "" || merchantID == "" {
		return domain.ErrInvalidArgument
	}
	return r.db.WithContext(ctx).
		Table("merchant_follow").
		Where("buyer_id = ? AND merchant_id = ?", buyerID, merchantID).
		Delete(&merchantFollowRow{}).Error
}

func (r *MySQLUserRepository) IsFollowingMerchant(ctx context.Context, buyerID, merchantID string) (bool, error) {
	buyerID = normalizeUserIDForDB(buyerID)
	merchantID = normalizeUserIDForDB(merchantID)
	if buyerID == "" || merchantID == "" {
		return false, nil
	}
	var count int64
	if err := r.db.WithContext(ctx).Table("merchant_follow").Where("buyer_id = ? AND merchant_id = ?", buyerID, merchantID).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *MySQLUserRepository) CountMerchantFollowers(ctx context.Context, merchantID string) (int, error) {
	merchantID = normalizeUserIDForDB(merchantID)
	if merchantID == "" {
		return 0, nil
	}
	var count int64
	if err := r.db.WithContext(ctx).Table("merchant_follow").Where("merchant_id = ?", merchantID).Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

func (r *MySQLUserRepository) ListMerchantFollowsByBuyer(ctx context.Context, buyerID string, limit, offset int) ([]domain.MerchantFollow, error) {
	buyerID = normalizeUserIDForDB(buyerID)
	if buyerID == "" {
		return nil, domain.ErrInvalidArgument
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var rows []merchantFollowRow
	if err := r.db.WithContext(ctx).
		Table("merchant_follow").
		Where("buyer_id = ?", buyerID).
		Order("created_at DESC, merchant_id DESC").
		Limit(limit).
		Offset(offset).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]domain.MerchantFollow, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.toDomain())
	}
	return out, nil
}

func (r *MySQLUserRepository) CountMerchantFollowsByBuyer(ctx context.Context, buyerID string) (int64, error) {
	buyerID = normalizeUserIDForDB(buyerID)
	if buyerID == "" {
		return 0, domain.ErrInvalidArgument
	}
	var count int64
	if err := r.db.WithContext(ctx).Table("merchant_follow").Where("buyer_id = ?", buyerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *MySQLUserRepository) findMerchantFollow(ctx context.Context, buyerID, merchantID string) (domain.MerchantFollow, error) {
	var row merchantFollowRow
	err := r.db.WithContext(ctx).
		Table("merchant_follow").
		Where("buyer_id = ? AND merchant_id = ?", normalizeUserIDForDB(buyerID), normalizeUserIDForDB(merchantID)).
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.MerchantFollow{}, domain.ErrNotFound
		}
		return domain.MerchantFollow{}, err
	}
	return row.toDomain(), nil
}

type userRow struct {
	ID           string                      `gorm:"column:id"`
	Account      string                      `gorm:"column:account"`
	Nickname     string                      `gorm:"column:nickname"`
	AvatarURL    sql.NullString              `gorm:"column:avatar_url"`
	Location     sql.NullString              `gorm:"column:location"`
	Role         string                      `gorm:"column:role"`
	Status       string                      `gorm:"column:status"`
	AIPermission domain.MerchantAIPermission `gorm:"column:ai_permission"`
	PasswordHash string                      `gorm:"column:password_hash"`
}

type merchantFollowRow struct {
	BuyerID    string    `gorm:"column:buyer_id"`
	MerchantID string    `gorm:"column:merchant_id"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

func (r merchantFollowRow) toDomain() domain.MerchantFollow {
	return domain.MerchantFollow{
		BuyerID:    r.BuyerID,
		MerchantID: r.MerchantID,
		CreatedAt:  r.CreatedAt,
	}
}

func (u userRow) toDomain() domain.User {
	return domain.User{
		ID:           u.ID,
		Account:      u.Account,
		Nickname:     u.Nickname,
		AvatarURL:    nullableStringValue(u.AvatarURL),
		Location:     nullableStringValue(u.Location),
		Role:         normalizeRole(u.Role),
		Status:       normalizeStatus(u.Status),
		AIPermission: domain.NormalizeMerchantAIPermission(u.AIPermission),
		PasswordHash: u.PasswordHash,
	}
}

func nullableStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func normalizeRole(role string) domain.Role {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "user", "buyer":
		return domain.RoleBuyer
	case "merchant":
		return domain.RoleMerchant
	case "admin":
		return domain.RoleAdmin
	default:
		return domain.Role(role)
	}
}

func normalizeStatus(status string) domain.UserStatus {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return domain.UserStatusActive
	case "DISABLED", "FROZEN", "DELETED":
		return domain.UserStatusDisabled
	default:
		return domain.UserStatus(status)
	}
}

func roleAliases(role domain.Role) []string {
	if role == domain.RoleBuyer {
		return []string{string(domain.RoleBuyer), "user"}
	}
	return []string{string(role)}
}

var _ userports.UserRepository = (*MySQLUserRepository)(nil)
var _ marketplaceports.MerchantFollowRepository = (*MySQLUserRepository)(nil)

func normalizeUserIDForDB(id string) string {
	id = strings.TrimSpace(id)
	for _, prefix := range []string{"u_", "U_"} {
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix)
		}
	}
	return id
}
