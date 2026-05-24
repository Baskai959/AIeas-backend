package idgen

import (
	"fmt"
	"sync"
	"time"
)

const (
	timestampBits = 31
	typeBits      = 5
	workerBits    = 8
	sequenceBits  = 9

	maxIDType  = 1<<typeBits - 1
	maxWorker  = 1<<workerBits - 1
	maxSeq     = 1<<sequenceBits - 1
	maxElapsed = 1<<timestampBits - 1

	workerShift    = sequenceBits
	typeShift      = workerBits + sequenceBits
	timestampShift = typeBits + workerBits + sequenceBits
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// IDType marks the business entity type in generated IDs.
type IDType uint8

const (
	IDTypeAuction IDType = 1
	IDTypeItem    IDType = 2
	IDTypeOrder   IDType = 3
	IDTypeRoom    IDType = 4
)

// Snowflake generates 53-bit sortable IDs that are safe as JSON numbers.
//
// Layout:
//   - 31 bits: seconds since 2026-01-01T00:00:00Z
//   - 5 bits: entity type
//   - 8 bits: worker ID
//   - 9 bits: per-second sequence
type Snowflake struct {
	mu       sync.Mutex
	workerID uint64
	now      func() time.Time
	sleep    func(time.Duration)
	lastSec  int64
	seq      uint64
}

// NewSnowflake creates a generator for one process/worker.
func NewSnowflake(workerID int) (*Snowflake, error) {
	return NewSnowflakeWithClock(workerID, time.Now, time.Sleep)
}

// NewSnowflakeWithClock creates a generator with injectable clock hooks for tests.
func NewSnowflakeWithClock(workerID int, now func() time.Time, sleep func(time.Duration)) (*Snowflake, error) {
	if workerID < 0 || workerID > maxWorker {
		return nil, fmt.Errorf("idgen workerID must be between 0 and %d", maxWorker)
	}
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	return &Snowflake{workerID: uint64(workerID), now: now, sleep: sleep, lastSec: -1}, nil
}

// Next returns the next ID for idType.
func (g *Snowflake) Next(idType IDType) (uint64, error) {
	if idType == 0 || idType > maxIDType {
		return 0, fmt.Errorf("idgen type must be between 1 and %d", maxIDType)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	nowSecond := g.currentSecond()
	if nowSecond < g.lastSec {
		rollback := g.lastSec - nowSecond
		if rollback > 5 {
			return 0, fmt.Errorf("clock moved backwards by %ds", rollback)
		}
		nowSecond = g.waitUntil(g.lastSec)
	}

	if nowSecond == g.lastSec {
		g.seq++
		if g.seq > maxSeq {
			nowSecond = g.waitUntil(g.lastSec + 1)
			g.seq = 0
		}
	} else {
		g.seq = 0
	}
	g.lastSec = nowSecond

	elapsed := nowSecond - epoch.Unix()
	if elapsed < 0 {
		return 0, fmt.Errorf("current time is before idgen epoch")
	}
	if elapsed > maxElapsed {
		return 0, fmt.Errorf("idgen timestamp overflow")
	}

	id := uint64(elapsed)<<timestampShift |
		uint64(idType)<<typeShift |
		g.workerID<<workerShift |
		g.seq
	return id, nil
}

// NextAuctionID returns the next auction lot ID.
func (g *Snowflake) NextAuctionID() (uint64, error) {
	return g.Next(IDTypeAuction)
}

// NextOrderID returns the next order ID.
func (g *Snowflake) NextOrderID() (uint64, error) {
	return g.Next(IDTypeOrder)
}

// ExtractType returns the entity type bits embedded in an ID.
func ExtractType(id uint64) IDType {
	return IDType((id >> typeShift) & maxIDType)
}

// ExtractWorkerID returns the worker bits embedded in an ID.
func ExtractWorkerID(id uint64) uint64 {
	return (id >> workerShift) & maxWorker
}

func (g *Snowflake) currentSecond() int64 {
	return g.now().UTC().Unix()
}

func (g *Snowflake) waitUntil(targetSec int64) int64 {
	nowSec := g.currentSecond()
	for nowSec < targetSec {
		g.sleep(10 * time.Millisecond)
		nowSec = g.currentSecond()
	}
	return nowSec
}
