package http

import (
	"errors"

	"aieas_backend/internal/domain"
)

func HTTPStatusAndCode(err error) (int, int, string) {
	switch {
	case errors.Is(err, domain.ErrTokenMissing):
		return 401, 10001, "缺少访问令牌"
	case errors.Is(err, domain.ErrTokenInvalid):
		return 401, 10002, "访问令牌无效或已过期"
	case errors.Is(err, domain.ErrForbidden):
		return 403, 10003, "无访问权限"
	case errors.Is(err, domain.ErrAccountDisabled):
		return 403, 10005, "账号已停用"
	case errors.Is(err, domain.ErrInvalidPassword):
		return 401, 10004, "登录失败"
	case errors.Is(err, domain.ErrInvalidArgument):
		return 400, 20001, "参数不合法"
	case errors.Is(err, domain.ErrUserNotFound):
		return 404, 20004, "资源不存在"
	case errors.Is(err, domain.ErrNotFound):
		return 404, 20004, "资源不存在"
	case errors.Is(err, domain.ErrConflict):
		return 409, 20009, "资源冲突"
	case errors.Is(err, domain.ErrOptimisticConflict):
		return 409, 20009, "资源冲突"
	case errors.Is(err, domain.ErrInvalidState):
		return 409, 20010, "状态不允许"
	case errors.Is(err, domain.ErrIdempotencyKey):
		return 400, 20011, "缺少幂等键"
	default:
		return 500, 90001, "系统内部错误"
	}
}
