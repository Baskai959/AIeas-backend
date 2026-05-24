package idgen

import (
	"testing"
	"time"
)

func TestSnowflakeNextEmbedsTypeAndWorker(t *testing.T) {
	base := epoch.Add(123 * time.Millisecond)
	gen, err := NewSnowflakeWithClock(7, func() time.Time { return base }, func(time.Duration) {})
	if err != nil {
		t.Fatalf("NewSnowflakeWithClock: %v", err)
	}

	id, err := gen.NextAuctionID()
	if err != nil {
		t.Fatalf("NextAuctionID: %v", err)
	}
	if id > 1<<53-1 {
		t.Fatalf("expected JSON-safe 53-bit ID, got %d", id)
	}
	if got := ExtractType(id); got != IDTypeAuction {
		t.Fatalf("expected type %d, got %d", IDTypeAuction, got)
	}
	if got := ExtractWorkerID(id); got != 7 {
		t.Fatalf("expected worker 7, got %d", got)
	}

	orderID, err := gen.NextOrderID()
	if err != nil {
		t.Fatalf("NextOrderID: %v", err)
	}
	if got := ExtractType(orderID); got != IDTypeOrder {
		t.Fatalf("expected order type %d, got %d", IDTypeOrder, got)
	}
}

func TestSnowflakeNextIsMonotonicWithinSameMillisecond(t *testing.T) {
	base := epoch.Add(time.Second)
	gen, err := NewSnowflakeWithClock(1, func() time.Time { return base }, func(time.Duration) {})
	if err != nil {
		t.Fatalf("NewSnowflakeWithClock: %v", err)
	}

	first, err := gen.NextAuctionID()
	if err != nil {
		t.Fatalf("first NextAuctionID: %v", err)
	}
	second, err := gen.NextAuctionID()
	if err != nil {
		t.Fatalf("second NextAuctionID: %v", err)
	}
	if second <= first {
		t.Fatalf("expected monotonic IDs, first=%d second=%d", first, second)
	}
}

func TestNewSnowflakeRejectsInvalidWorker(t *testing.T) {
	if _, err := NewSnowflake(-1); err == nil {
		t.Fatal("expected negative worker ID to fail")
	}
	if _, err := NewSnowflake(256); err == nil {
		t.Fatal("expected worker ID above 255 to fail")
	}
}
