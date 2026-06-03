package repository

import (
	"database/sql"
	"errors"
	"strings"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
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
		Select("id, account, nickname, avatar_url, role, status, ai_permission, password_hash").
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
		Select("id, account, nickname, avatar_url, role, status, ai_permission, password_hash").
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
	query := r.db.Table("user").Select("id, account, nickname, avatar_url, role, status, ai_permission, password_hash").Order("id DESC")
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

type userRow struct {
	ID           string                      `gorm:"column:id"`
	Account      string                      `gorm:"column:account"`
	Nickname     string                      `gorm:"column:nickname"`
	AvatarURL    sql.NullString              `gorm:"column:avatar_url"`
	Role         string                      `gorm:"column:role"`
	Status       string                      `gorm:"column:status"`
	AIPermission domain.MerchantAIPermission `gorm:"column:ai_permission"`
	PasswordHash string                      `gorm:"column:password_hash"`
}

func (u userRow) toDomain() domain.User {
	return domain.User{
		ID:           u.ID,
		Account:      u.Account,
		Nickname:     u.Nickname,
		AvatarURL:    nullableStringValue(u.AvatarURL),
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
