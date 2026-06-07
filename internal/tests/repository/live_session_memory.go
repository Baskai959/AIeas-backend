package repository

import livesessionrepo "aieas_backend/internal/modules/live_session/repository"

// MemoryLiveSessionRepository 是 live_session 的内存实现，用于单测与 NewServerWithDependencies 兜底。
type MemoryLiveSessionRepository = livesessionrepo.MemoryLiveSessionRepository

func NewMemoryLiveSessionRepository() *MemoryLiveSessionRepository {
	return livesessionrepo.NewMemoryLiveSessionRepository()
}
