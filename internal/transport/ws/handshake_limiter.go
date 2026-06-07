// handshake_limiter.go 实现 WebSocket 握手阶段的多维度令牌桶限流：
// 按 IP / userID / auctionID 各自维护独立桶，超出桶容量返回带分类原因的拒绝。
//
// 选型说明：项目本身没有引入 golang.org/x/time/rate，这里实现一个
// 极简的 per-key token bucket（capacity=rpm，refill 速率 rpm/分钟）。
// 不依赖第三方 timer 协程：每次 Allow 时按 elapsed 计算 refill。
//
// key 自清理：超过 5 分钟未访问的 key 在下次 Allow 时被淘汰；同时设
// 置 100k 上限，超出时把 oldest 视作过期清掉。
package ws

import (
	"strconv"
	"sync"
	"time"
)

const (
	handshakeLimiterIdleTTL = 5 * time.Minute
	handshakeLimiterMaxKeys = 100000
)

// HandshakeLimiter 是按 IP / user / auction 三维度独立限流的握手限流器。
type HandshakeLimiter struct {
	perIP      *limiterBucket
	perUser    *limiterBucket
	perAuction *limiterBucket
}

// NewHandshakeLimiter 构造限流器。任一参数 <=0 表示该维度不限流（永远 allow）。
func NewHandshakeLimiter(perIPRPM, perUserRPM, perAuctionRPM int) *HandshakeLimiter {
	return &HandshakeLimiter{
		perIP:      newLimiterBucket(perIPRPM),
		perUser:    newLimiterBucket(perUserRPM),
		perAuction: newLimiterBucket(perAuctionRPM),
	}
}

// Allow 综合三个维度判断本次握手是否放行：
//   - userID 为空或 "anonymous" 时跳过 perUser 维度（接口约定不暴露匿名用户基数）；
//   - 任一维度被拒绝立即返回 (false, reason)，不消耗其他维度的令牌。
func (l *HandshakeLimiter) Allow(ip, userID string, auctionID uint64) (bool, string) {
	if l == nil {
		return true, ""
	}
	now := time.Now()
	if l.perIP != nil && l.perIP.enabled() && ip != "" {
		if !l.perIP.allow(ip, now) {
			return false, "rate_limit_ip"
		}
	}
	if l.perUser != nil && l.perUser.enabled() && userID != "" && userID != "anonymous" {
		if !l.perUser.allow(userID, now) {
			return false, "rate_limit_user"
		}
	}
	if l.perAuction != nil && l.perAuction.enabled() && auctionID != 0 {
		if !l.perAuction.allow(strconv.FormatUint(auctionID, 10), now) {
			return false, "rate_limit_auction"
		}
	}
	return true, ""
}

// limiterBucket 是按 key 隔离的令牌桶集合。
type limiterBucket struct {
	rpm   int
	mu    sync.Mutex
	state map[string]*tokenState
}

type tokenState struct {
	tokens   float64
	lastSeen time.Time
}

func newLimiterBucket(rpm int) *limiterBucket {
	return &limiterBucket{rpm: rpm, state: make(map[string]*tokenState)}
}

func (b *limiterBucket) enabled() bool { return b != nil && b.rpm > 0 }

// allow 对单个 key 做一次 refill+扣减。
//
// 模型：capacity=rpm（即每分钟最多发放 rpm 个令牌，初始满桶），refill 速率
// rpm/60 个/秒。一次握手扣 1 个令牌；不足 1 时拒绝，state 仍记录 lastSeen
// 用于淘汰判断。
func (b *limiterBucket) allow(key string, now time.Time) bool {
	if !b.enabled() {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.state[key]
	rate := float64(b.rpm) / 60.0
	capacity := float64(b.rpm)
	if !ok {
		st = &tokenState{tokens: capacity, lastSeen: now}
		b.state[key] = st
	} else {
		elapsed := now.Sub(st.lastSeen).Seconds()
		if elapsed > 0 {
			st.tokens += elapsed * rate
			if st.tokens > capacity {
				st.tokens = capacity
			}
		}
		st.lastSeen = now
	}
	allowed := false
	if st.tokens >= 1 {
		st.tokens -= 1
		allowed = true
	}
	b.maybeEvictLocked(now)
	return allowed
}

// maybeEvictLocked 清理 5min 未访问的 key，并在超过 100k 上限时淘汰更老的 key。
// 调用方持有 b.mu。
func (b *limiterBucket) maybeEvictLocked(now time.Time) {
	if len(b.state) == 0 {
		return
	}
	// idle 清理：每次 Allow 都做一次 cheap pass；总量大的话也只是 O(n)，
	// 但不会无限增长。如果担心放大可改为概率触发（这里保持简单）。
	for k, st := range b.state {
		if now.Sub(st.lastSeen) > handshakeLimiterIdleTTL {
			delete(b.state, k)
		}
	}
	if len(b.state) <= handshakeLimiterMaxKeys {
		return
	}
	// 超出硬上限：把最久未访问的若干 key 删掉，直到回到上限。
	type aged struct {
		key  string
		seen time.Time
	}
	all := make([]aged, 0, len(b.state))
	for k, st := range b.state {
		all = append(all, aged{key: k, seen: st.lastSeen})
	}
	// 简单的 partial-sort：找出 oldest 一批
	overflow := len(all) - handshakeLimiterMaxKeys
	for i := 0; i < overflow; i++ {
		oldestIdx := i
		for j := i + 1; j < len(all); j++ {
			if all[j].seen.Before(all[oldestIdx].seen) {
				oldestIdx = j
			}
		}
		all[i], all[oldestIdx] = all[oldestIdx], all[i]
		delete(b.state, all[i].key)
	}
}
