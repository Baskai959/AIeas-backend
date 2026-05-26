package repository

import (
	"context"
	"sync"

	"aieas_backend/internal/domain"
)

// LiveSessionRealtimeStore 提供直播场次计数的多实例一致存储。
//
// 实现方需保证：
//   - IncrCounters 中的所有 delta 字段都使用原子的 HINCRBY；
//   - BumpViewerPeak 通过 Lua / CAS 保证 max 语义，不丢更新；
//   - LoadCounters 返回当前累计值的快照；
//   - Reset 在场次关闭后清理 redis key（可选，便于测试与释放空间）。
type LiveSessionRealtimeStore interface {
	IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error
	BumpViewerPeak(ctx context.Context, sessionID uint64, value int) (int, error)
	LoadCounters(ctx context.Context, sessionID uint64) (domain.LiveSessionCounters, int, error)
	Reset(ctx context.Context, sessionID uint64) error
}

// MemoryLiveSessionRealtimeStore 是 LiveSessionRealtimeStore 的内存实现，
// 仅用于单测 / 单实例 fallback。
type MemoryLiveSessionRealtimeStore struct {
	mu     sync.Mutex
	rows   map[uint64]domain.LiveSessionCounters
	peaks  map[uint64]int
	totals map[uint64]int // viewer_total 累加
}

func NewMemoryLiveSessionRealtimeStore() *MemoryLiveSessionRealtimeStore {
	return &MemoryLiveSessionRealtimeStore{
		rows:   make(map[uint64]domain.LiveSessionCounters),
		peaks:  make(map[uint64]int),
		totals: make(map[uint64]int),
	}
}

func (s *MemoryLiveSessionRealtimeStore) IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error {
	_ = ctx
	if sessionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.rows[sessionID]
	row.LotsTotalDelta += c.LotsTotalDelta
	row.LotsSoldDelta += c.LotsSoldDelta
	row.LotsUnsoldDelta += c.LotsUnsoldDelta
	row.BidCountDelta += c.BidCountDelta
	row.GMVCentDelta += c.GMVCentDelta
	row.ViewerTotalAdd += c.ViewerTotalAdd
	s.rows[sessionID] = row
	return nil
}

func (s *MemoryLiveSessionRealtimeStore) BumpViewerPeak(ctx context.Context, sessionID uint64, value int) (int, error) {
	_ = ctx
	if sessionID == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.peaks[sessionID]
	if value > cur {
		s.peaks[sessionID] = value
		return value, nil
	}
	return cur, nil
}

func (s *MemoryLiveSessionRealtimeStore) LoadCounters(ctx context.Context, sessionID uint64) (domain.LiveSessionCounters, int, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.rows[sessionID]
	peak := s.peaks[sessionID]
	return row, peak, nil
}

func (s *MemoryLiveSessionRealtimeStore) Reset(ctx context.Context, sessionID uint64) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, sessionID)
	delete(s.peaks, sessionID)
	delete(s.totals, sessionID)
	return nil
}
