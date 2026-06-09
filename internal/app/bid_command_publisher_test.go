package app

import (
	"context"
	"errors"
	"testing"

	auctionapp "aieas_backend/internal/modules/auction/app"
)

// TestKafkaBidCommandPublisherGateRejects 验证 publisher 闸门关闭时返回 ErrHammerPending。
// 注意：这里不实际触发 kafka producer（producer 字段为 nil 时直接 return nil），
// 因此只测试闸门拒绝路径的行为，不依赖 kafka 连通性。
func TestKafkaBidCommandPublisherGateRejects(t *testing.T) {
	pub := &kafkaBidCommandPublisher{producer: nil}
	gate := auctionapp.NewHammerPublisherGate(0)
	pub.SetGate(gate)
	auctionID := uint64(8101)

	// 闸门未关：因 producer=nil 直接 return nil（业务上视为 noop publisher）。
	if err := pub.PublishBidCommand(context.Background(), auctionapp.BidCommandSnapshot{AuctionID: auctionID}); err != nil {
		t.Fatalf("publish (gate open, nil producer) = %v, want nil", err)
	}

	// 关闸：必须返回 ErrHammerPending，且不调用底层 producer。
	gate.Close(auctionID)
	err := pub.PublishBidCommand(context.Background(), auctionapp.BidCommandSnapshot{AuctionID: auctionID})
	if !errors.Is(err, auctionapp.ErrHammerPending) {
		t.Fatalf("publish (gate closed) = %v, want ErrHammerPending", err)
	}

	// 开闸：恢复 noop。
	gate.Open(auctionID)
	if err := pub.PublishBidCommand(context.Background(), auctionapp.BidCommandSnapshot{AuctionID: auctionID}); err != nil {
		t.Fatalf("publish (gate reopened) = %v, want nil", err)
	}
}

// fakePublishRejectMetrics 验证拒绝时确实打了 IncBidCommandPublishReject 指标。
type fakePublishRejectMetrics struct {
	calls []string
}

func (f *fakePublishRejectMetrics) IncBidCommandPublishReject(reason string) {
	f.calls = append(f.calls, reason)
}

func TestKafkaBidCommandPublisherGateMetrics(t *testing.T) {
	pub := &kafkaBidCommandPublisher{producer: nil}
	gate := auctionapp.NewHammerPublisherGate(0)
	pub.SetGate(gate)
	m := &fakePublishRejectMetrics{}
	pub.SetMetrics(m)
	auctionID := uint64(8102)
	gate.Close(auctionID)
	_ = pub.PublishBidCommand(context.Background(), auctionapp.BidCommandSnapshot{AuctionID: auctionID})
	if len(m.calls) != 1 || m.calls[0] != "hammer_pending" {
		t.Fatalf("expected one IncBidCommandPublishReject(hammer_pending), got %v", m.calls)
	}
}
