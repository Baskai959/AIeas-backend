package service

import (
	"context"
	"testing"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	auctionapp "aieas_backend/internal/modules/auction/app"
)

// BidCommandSnapshot 别名，便于测试构造异步命令快照。
type BidCommandSnapshot = auctionapp.BidCommandSnapshot

// TestPreCheckArbitrateEquivalence 验证异步拆分（PreCheckForAsync + ArbitrateFromCommand）
// 与同步 Place 在 happy path 上的等价性：同一报价被接受、最终价一致。
func TestPreCheckArbitrateEquivalence(t *testing.T) {
	cfg := appconfig.Default().Auction
	ctx := context.Background()

	// 同步基线：直接 Place。
	syncFixture := newRealtimeAuctionFixtureWithTiming(t, cfg, 0, 1000)
	mustEnroll(t, syncFixture, "u_3001")
	expected := int64(1000)
	syncResult, err := syncFixture.bids.Place(ctx, PlaceBidInput{
		RequestID:            "sync-1",
		AuctionID:            syncFixture.auctionID,
		BidderID:             "u_3001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &expected,
		Source:               "live_ws",
	})
	if err != nil {
		t.Fatalf("sync place: %v", err)
	}
	if !syncResult.Accepted {
		t.Fatalf("sync bid not accepted: reason=%s", syncResult.Reason)
	}

	// 异步路径：preCheck → arbitrate（同一套仓储另起一个 fixture，避免幂等串扰）。
	asyncFixture := newRealtimeAuctionFixtureWithTiming(t, cfg, 0, 1000)
	mustEnroll(t, asyncFixture, "u_3001")
	exp2 := int64(1000)
	snapshot, terminal, err := asyncFixture.bids.PreCheckForAsync(ctx, PlaceBidInput{
		RequestID:            "async-1",
		AuctionID:            asyncFixture.auctionID,
		BidderID:             "u_3001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &exp2,
		Source:               "live_ws",
	})
	if err != nil {
		t.Fatalf("preCheck: %v", err)
	}
	if terminal != nil {
		t.Fatalf("preCheck terminated early: reason=%s", terminal.Reason)
	}
	if snapshot.BidID != "async-1" || snapshot.AuctionID != asyncFixture.auctionID {
		t.Fatalf("snapshot mismatch: %+v", snapshot)
	}
	asyncResult, err := asyncFixture.bids.ArbitrateFromCommand(ctx, snapshot)
	if err != nil {
		t.Fatalf("arbitrate: %v", err)
	}
	if !asyncResult.Accepted {
		t.Fatalf("async bid not accepted: reason=%s", asyncResult.Reason)
	}
	if asyncResult.CurrentPrice != syncResult.CurrentPrice {
		t.Fatalf("current price mismatch: sync=%d async=%d", syncResult.CurrentPrice, asyncResult.CurrentPrice)
	}
}

// TestPreCheckForAsyncTerminalReject 验证 preCheck 在前置校验失败时返回 terminal（不入队）。
func TestPreCheckForAsyncTerminalReject(t *testing.T) {
	cfg := appconfig.Default().Auction
	ctx := context.Background()
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, 0, 1000)
	// 未报名/未交押金：preCheck 应在 prerequisite 阶段裁定为 terminal 拒绝。
	expected := int64(1000)
	_, terminal, err := fixture.bids.PreCheckForAsync(ctx, PlaceBidInput{
		RequestID:            "noenroll-1",
		AuctionID:            fixture.auctionID,
		BidderID:             "u_9999",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &expected,
		Source:               "live_ws",
	})
	if err != nil {
		t.Fatalf("preCheck: %v", err)
	}
	if terminal == nil {
		t.Fatalf("expected terminal rejection for unenrolled bidder")
	}
	if terminal.Accepted {
		t.Fatalf("unenrolled bidder should not be accepted")
	}
}

// TestArbitrateFromCommandInvalid 验证命令缺关键字段时直接返回参数错误。
func TestArbitrateFromCommandInvalid(t *testing.T) {
	cfg := appconfig.Default().Auction
	ctx := context.Background()
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, 0, 1000)
	_, err := fixture.bids.ArbitrateFromCommand(ctx, BidCommandSnapshot{})
	if err == nil {
		t.Fatalf("expected error for empty command")
	}
}
