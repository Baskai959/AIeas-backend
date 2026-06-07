package ports

import "aieas_backend/internal/domain"

// UserRepository 是用户账号资料的持久化端口。
type UserRepository interface {
	FindByAccountAndRole(account string, role domain.Role) (domain.User, error)
	FindByID(id string) (domain.User, error)
	List(filter domain.UserFilter) ([]domain.User, error)
	Update(user *domain.User) error
}
