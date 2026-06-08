package http

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	corews "aieas_backend/internal/transport/ws"
)

// fakeAsyncBids 实现 WSAsyncBidUseCase。
type fakeAsyncBids struct {
	terminal *domain.BidResult
	err      error
	snapshot BidCommandSnapshot
}

func (f fakeAsyncBids) PreCheckForAsync(ctx context.Context, in PlaceBidInput) (BidCommandSnapshot, *domain.BidResult, error) {
	if f.err != nil {
		return BidCommandSnapshot{}, nil, f.err
	}
	if f.terminal != nil {
		return BidCommandSnapshot{}, f.terminal, nil
	}
	snap := f.snapshot
	snap.BidID = in.RequestID
	snap.AuctionID = in.AuctionID
	snap.UserID = in.BidderID
	return snap, nil, nil
}

// fakeCmdPublisher 实现 BidCommandPublisher。
type fakeCmdPublisher struct {
	published []BidCommandSnapshot
	err       error
}

func (p *fakeCmdPublisher) PublishBidCommand(ctx context.Context, cmd BidCommandSnapshot) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, cmd)
	return nil
}

func newAsyncHandler(t *testing.T, bids WSAsyncBidUseCase, publisher BidCommandPublisher, syncBids WSBidUseCase) (*WSHandler, *corews.BidAsyncCoordinator) {
	t.Helper()
	if syncBids == nil {
		// async 分支仍要求 h.bids 非 nil（用于发布失败时的同步兜底）。
		syncBids = stubSyncBids{result: domain.BidResult{Accepted: true, CurrentPrice: 1100}}
	}
	hub := corews.NewHub()
	coord := corews.NewBidAsyncCoordinator(hub, 1, time.Hour, 3)
	handler := NewWSHandler(hub, syncBids, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	handler.SetAsyncBidDependencies(bids, publisher, coord)
	handler.SetBidPlaceMode(WSBidPlaceAsync)
	return handler, coord
}

func bidPlaceEnvelope(reqID string, auctionID uint64) corews.Envelope {
	payload, _ := json.Marshal(map[string]interface{}{"auctionId": auctionID, "price": 1100, "expectedCurrentPrice": 1000})
	return corews.Envelope{Type: "bid.place", RequestID: reqID, Payload: payload}
}

func decodeAck(t *testing.T, env corews.Envelope) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(env.Payload, &m); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	return m
}

func TestWSHandlerAsyncQueuesAndAcksQueued(t *testing.T) {
	publisher := &fakeCmdPublisher{}
	handler, coord := newAsyncHandler(t, fakeAsyncBids{}, publisher, nil)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	responses := handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	if len(responses) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(responses))
	}
	ack := decodeAck(t, responses[0])
	if ack["mode"] != "ASYNC" || ack["status"] != "QUEUED" {
		t.Fatalf("expected ASYNC/QUEUED ack, got %+v", ack)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected 1 published command, got %d", len(publisher.published))
	}
	if coord.PendingQueueSize() != 1 {
		t.Fatalf("expected pending size 1, got %d", coord.PendingQueueSize())
	}
}

func TestWSHandlerAsyncPreCheckTerminalDoesNotEnqueue(t *testing.T) {
	publisher := &fakeCmdPublisher{}
	terminal := &domain.BidResult{Accepted: false, Reason: "NOT_ENROLLED"}
	handler, coord := newAsyncHandler(t, fakeAsyncBids{terminal: terminal}, publisher, nil)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	responses := handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	ack := decodeAck(t, responses[0])
	if ack["mode"] != "ASYNC" || ack["status"] != "REJECTED" || ack["reason"] != "NOT_ENROLLED" {
		t.Fatalf("expected ASYNC/REJECTED/NOT_ENROLLED, got %+v", ack)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("terminal reject should not enqueue")
	}
	if coord.PendingQueueSize() != 0 {
		t.Fatalf("terminal reject should not register pending")
	}
}

func TestWSHandlerAsyncQueueProtectionUserPending(t *testing.T) {
	publisher := &fakeCmdPublisher{}
	handler, _ := newAsyncHandler(t, fakeAsyncBids{}, publisher, nil)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	// 第一条 QUEUED。
	_ = handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	// 同用户同拍品第二条 → USER_BID_ALREADY_PENDING。
	responses := handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b2", 10))
	ack := decodeAck(t, responses[0])
	if ack["status"] != "REJECTED" || ack["reason"] != corews.BidQueueRejectUserPending {
		t.Fatalf("expected USER_BID_ALREADY_PENDING, got %+v", ack)
	}
}

func TestWSHandlerAsyncPublishFailureFallsBackToSync(t *testing.T) {
	publisher := &fakeCmdPublisher{err: errors.New("kafka down")}
	syncBids := stubSyncBids{result: domain.BidResult{Accepted: true, CurrentPrice: 1100}}
	handler, coord := newAsyncHandler(t, fakeAsyncBids{}, publisher, syncBids)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	responses := handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	ack := decodeAck(t, responses[0])
	// 回退同步：ack 为同步形态（无 mode 字段），accepted=true。
	if _, hasMode := ack["mode"]; hasMode {
		t.Fatalf("sync fallback ack should not carry mode: %+v", ack)
	}
	if ack["accepted"] != true {
		t.Fatalf("expected accepted sync fallback, got %+v", ack)
	}
	if coord.PendingQueueSize() != 0 {
		t.Fatalf("publish failure should release pending, got %d", coord.PendingQueueSize())
	}
}

func TestWSHandlerAsyncDowngradesToSyncWhenDepsMissing(t *testing.T) {
	hub := corews.NewHub()
	syncBids := stubSyncBids{result: domain.BidResult{Accepted: true, CurrentPrice: 1100}}
	handler := NewWSHandler(hub, syncBids, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	// 设为 async，但未注入 async 依赖 → 应降级同步。
	handler.SetBidPlaceMode(WSBidPlaceAsync)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	responses := handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	ack := decodeAck(t, responses[0])
	if _, hasMode := ack["mode"]; hasMode {
		t.Fatalf("downgraded sync ack should not carry mode: %+v", ack)
	}
	if ack["accepted"] != true {
		t.Fatalf("expected sync accepted, got %+v", ack)
	}
}

func TestWSHandlerBidResultAckReleasesPending(t *testing.T) {
	publisher := &fakeCmdPublisher{}
	handler, coord := newAsyncHandler(t, fakeAsyncBids{}, publisher, nil)
	client := corews.NewClientWithSession("c1", "u1", 10, 900, 8)
	_ = handler.handleInbound(context.Background(), client, bidPlaceEnvelope("b1", 10))
	if coord.PendingQueueSize() != 1 {
		t.Fatalf("expected pending 1")
	}
	ackPayload, _ := json.Marshal(map[string]interface{}{"bidId": "b1"})
	handler.handleInbound(context.Background(), client, corews.Envelope{Type: "bid.result.ack", Payload: ackPayload})
	if coord.PendingQueueSize() != 0 {
		t.Fatalf("expected pending released after ack, got %d", coord.PendingQueueSize())
	}
}

// stubSyncBids 实现 WSBidUseCase 用于同步降级断言。
type stubSyncBids struct {
	result domain.BidResult
	err    error
}

func (s stubSyncBids) Place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	return s.result, s.err
}
