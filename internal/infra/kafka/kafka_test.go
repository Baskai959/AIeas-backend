package kafka

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestBidCommandMessageKeyUsesBidDimension(t *testing.T) {
	ctx := context.Background()
	cmd := BidCommand{
		BidID:     "req-1",
		AuctionID: 10001,
		UserID:    "buyer-1",
		Price:     1200,
	}

	msg, err := bidCommandMessage(ctx, cmd)
	if err != nil {
		t.Fatalf("bidCommandMessage returned error: %v", err)
	}

	key := string(msg.Key)
	if key == strconv.FormatUint(cmd.AuctionID, 10) {
		t.Fatalf("bid command key must not be raw auctionId, got %q", key)
	}
	if key != "bid:10001:req-1" {
		t.Fatalf("unexpected bid command key: %q", key)
	}

	var decoded BidCommand
	if err := json.Unmarshal(msg.Value, &decoded); err != nil {
		t.Fatalf("unmarshal bid command value: %v", err)
	}
	if decoded.EnqueuedAtMS == 0 {
		t.Fatalf("expected EnqueuedAtMS to be filled before publish")
	}
	if decoded.AuctionID != cmd.AuctionID || decoded.BidID != cmd.BidID {
		t.Fatalf("unexpected decoded command: %+v", decoded)
	}
}

func TestBidCommandKeyDispersesSameAuctionDifferentBids(t *testing.T) {
	cmd1 := BidCommand{BidID: "req-1", AuctionID: 10001, UserID: "buyer-1", Price: 1200}
	cmd2 := BidCommand{BidID: "req-2", AuctionID: 10001, UserID: "buyer-2", Price: 1300}

	key1 := bidCommandKey(cmd1)
	key2 := bidCommandKey(cmd2)
	if key1 == key2 {
		t.Fatalf("same auction different bid commands should produce different keys: %q", key1)
	}
	if key1 == strconv.FormatUint(cmd1.AuctionID, 10) || key2 == strconv.FormatUint(cmd2.AuctionID, 10) {
		t.Fatalf("bid command keys must not collapse to auctionId: key1=%q key2=%q", key1, key2)
	}
}

func TestBidCommandKeyFallbackAvoidsEmptyBidIDHotKey(t *testing.T) {
	cmd1 := BidCommand{AuctionID: 10001, UserID: "buyer-1", Price: 1200, EnqueuedAtMS: 100}
	cmd2 := BidCommand{AuctionID: 10001, UserID: "buyer-2", Price: 1300, EnqueuedAtMS: 101}

	key1 := bidCommandKey(cmd1)
	key2 := bidCommandKey(cmd2)
	if key1 == key2 {
		t.Fatalf("fallback keys should include non-auction bid dimensions: %q", key1)
	}
	if key1 == strconv.FormatUint(cmd1.AuctionID, 10) || key2 == strconv.FormatUint(cmd2.AuctionID, 10) {
		t.Fatalf("fallback keys must not collapse to auctionId: key1=%q key2=%q", key1, key2)
	}
}

func TestProducerWriterKeepsHashBalancerForCommandKeys(t *testing.T) {
	p := &Producer{
		brokers: []string{"127.0.0.1:9092"},
		writers: make(map[string]*kafkago.Writer),
	}

	writer := p.writer("aieas.bid.commands")
	if _, ok := writer.Balancer.(*kafkago.Hash); !ok {
		t.Fatalf("expected Hash balancer for command-key based partitioning, got %T", writer.Balancer)
	}
}
