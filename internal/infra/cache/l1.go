// Package cache 中的 l1.go 实现进程内 LRU + TTL 的一级缓存。
//
// 设计目标：
//   - 容量上限固定（防止无限增长把内存吃满），命中后维护 LRU 顺序，写满淘汰最久未用项。
//   - 每个条目带绝对过期时间戳；Get 检测到过期立即删除并视为 miss。
//   - 全部并发安全；锁粒度为整张表（条目数有限，大多数访问是命中路径，简单 mu 足够）。
//   - 不参与序列化：上层缓存把 []byte（或负缓存的 nil）原样写入，让 L1/L2 共用同一份 codec。
//   - 不绑定具体业务类型；通过 string→entry 的统一存储满足所有 LayeredCache 实例。
//
// 故意不引入第三方 LRU 库：依赖闭环更小，且我们对外的语义只用到 4 个方法。
package cache

import (
	"container/list"
	"sync"
	"time"
)

// l1Entry 是 L1 链表节点上的负载。raw 为 nil 表示负缓存命中。
type l1Entry struct {
	key       string
	raw       []byte
	negative  bool
	expiresAt time.Time
}

// l1Cache 是按 LRU + 绝对过期时间淘汰的进程内缓存。
//
// 零值不可用：必须通过 newL1Cache(capacity) 构造。
type l1Cache struct {
	mu       sync.Mutex
	capacity int
	order    *list.List               // value 是 *l1Entry，front 最新
	index    map[string]*list.Element // key → list 元素
}

// newL1Cache 构造容量为 capacity 的 L1 缓存。capacity<=0 时退化为一个 1 容量
// 的最小缓存，避免对调用方造成 panic 或 disable 语义二义性。
func newL1Cache(capacity int) *l1Cache {
	if capacity <= 0 {
		capacity = 1
	}
	return &l1Cache{
		capacity: capacity,
		order:    list.New(),
		index:    make(map[string]*list.Element, capacity),
	}
}

// get 返回 (raw, negative, ok)。ok=false 表示 miss 或已过期；ok=true 时 raw=nil
// 仅在 negative=true 时有效（负缓存命中）。
func (c *l1Cache) get(key string) (raw []byte, negative bool, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, hit := c.index[key]
	if !hit {
		return nil, false, false
	}
	entry := elem.Value.(*l1Entry)
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		c.removeElement(elem)
		return nil, false, false
	}
	c.order.MoveToFront(elem)
	return entry.raw, entry.negative, true
}

// set 写入或更新一个条目；ttl<=0 表示 "永不过期"（仍受容量约束淘汰）。
func (c *l1Cache) set(key string, raw []byte, negative bool, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	if elem, ok := c.index[key]; ok {
		entry := elem.Value.(*l1Entry)
		entry.raw = raw
		entry.negative = negative
		entry.expiresAt = expiresAt
		c.order.MoveToFront(elem)
		return
	}
	entry := &l1Entry{key: key, raw: raw, negative: negative, expiresAt: expiresAt}
	elem := c.order.PushFront(entry)
	c.index[key] = elem
	if c.order.Len() > c.capacity {
		c.evictOldest()
	}
}

// invalidate 删除若干 key；不存在的 key 直接忽略。
func (c *l1Cache) invalidate(keys ...string) {
	if len(keys) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		if elem, ok := c.index[key]; ok {
			c.removeElement(elem)
		}
	}
}

// evictOldest 淘汰链表尾部条目；调用方需持锁。
func (c *l1Cache) evictOldest() {
	tail := c.order.Back()
	if tail == nil {
		return
	}
	c.removeElement(tail)
}

// removeElement 从链表 + 索引同步删除一个元素；调用方需持锁。
func (c *l1Cache) removeElement(elem *list.Element) {
	entry := elem.Value.(*l1Entry)
	c.order.Remove(elem)
	delete(c.index, entry.key)
}
