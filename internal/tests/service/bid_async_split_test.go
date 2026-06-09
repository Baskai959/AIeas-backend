package service

import (
	"context"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
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

// TestArbitrateFromCommandRejectIncrementsBidRejectTotal 验证异步路径下
// ArbitrateFromCommand 在 Lua 返回 Accepted=false（拒绝）时，会把拒因
// 写入 aieas_bid_reject_total{reason}，让 dashboard 能 group by reason 看
// 拒因细分。
//
// 历史 bug：异步路径只把 outcome 写到 bid_decision_outcome_total{outcome}，
// 而 aieas_bid_reject_total 的埋点被遗漏，导致压测窗口里该指标始终为 0。
func TestArbitrateFromCommandRejectIncrementsBidRejectTotal(t *testing.T) {
	cfg := appconfig.Default().Auction
	ctx := context.Background()
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, time.Hour, 1000)
	mustEnroll(t, fixture, "u_3001")

	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "test"})
	fixture.bids.SetMetrics(reg)

	// 报价低于 startPrice：Lua 会返回 BELOW_START_PRICE。expected=1000 通过基础校验。
	expected := int64(1000)
	cmd := BidCommandSnapshot{
		BidID:                "rej-async-1",
		AuctionID:            fixture.auctionID,
		LiveSessionID:        0,
		UserID:               "u_3001",
		Price:                500, // 低于 startPrice=1000
		ExpectedCurrentPrice: &expected,
		Source:               "live_ws",
		MinIncrement:         100,
		AntiSnipingMS:        60_000,
		AntiExtendMS:         30_000,
		AntiExtendMode:       domain.AuctionExtendModeAdd,
		MaxExtendCount:       cfg.MaxExtendCount,
		StartPrice:           1000,
		CapPrice:             2000,
		IncrementRule:        domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: 100, MaxBidSteps: 10},
	}
	result, err := fixture.bids.ArbitrateFromCommand(ctx, cmd)
	if err != nil {
		t.Fatalf("arbitrate: %v", err)
	}
	if result.Accepted || result.Duplicate {
		t.Fatalf("expected rejection, got %+v", result)
	}
	if result.Reason == "" {
		t.Fatalf("expected non-empty reject reason, got %+v", result)
	}
	// 同 reason 的 counter 应当 +1。
	got := counterValueByLabel(t, reg, "test_auction_bid_reject_total", "reason", result.Reason)
	if got < 1 {
		t.Fatalf("expected aieas_bid_reject_total{reason=%q} >= 1, got %v", result.Reason, got)
	}
}

// counterValueByLabel 读取指定 counter vec 中匹配某 label 的样本值。
func counterValueByLabel(t *testing.T, reg *metrics.Registry, name, labelName, labelValue string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return 0
}

// 保留 auctionapp 引用，避免 import 在仅添加测试时被误删。
var _ = auctionapp.BidCommandSnapshot{}

// TestArbitrateFromCommandPreservesAntiSniping 验证异步路径下 ArbitrateFromCommand
// 重建 bidAuctionSnapshot 时正确把 cmd.AntiSnipingMS / AntiExtendMS 反算回 sec，
// 让 arbitrate 向 Lua 传递正确的 ARGV 7/8，从而触发 anti-sniping 延长 endTime。
//
// 历史 bug：ArbitrateFromCommand 没有写 AntiSnipingSec/AntiExtendSec，导致 Lua 收到
// 0/0、anti_snipe_ms>0 && extend_ms>0 永远 false、endTime 不被推后。本测试基线就是
// 出价被接受 + endTime 被推后（Extended=true）。
func TestArbitrateFromCommandPreservesAntiSniping(t *testing.T) {
	cfg := appconfig.Default().Auction
	ctx := context.Background()
	// fixture 默认 AntiSnipingSec=60s, AntiExtendSec=30s, endOffset=30s。
	// 30s 剩余 ≤ 60s anti-sniping 窗口，触发 endTime 延长。
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, 30*time.Second, 1000)
	mustEnroll(t, fixture, "u_3001")

	expected := int64(1000)
	snapshot, terminal, err := fixture.bids.PreCheckForAsync(ctx, PlaceBidInput{
		RequestID:            "anti-snipe-async-1",
		AuctionID:            fixture.auctionID,
		BidderID:             "u_3001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &expected,
		Source:               "live_ws",
	})
	if err != nil {
		t.Fatalf("preCheck: %v", err)
	}
	if terminal != nil {
		t.Fatalf("preCheck unexpectedly terminal: %+v", terminal)
	}
	// 命令快照里必须带上 ms 字段（PreCheckForAsync 已正确 sec*1000）。
	if snapshot.AntiSnipingMS != 60_000 || snapshot.AntiExtendMS != 30_000 {
		t.Fatalf("expected snapshot AntiSnipingMS=60000 AntiExtendMS=30000, got %+v", snapshot)
	}
	endBefore, ok, err := fixture.realtime.GetAuctionState(ctx, fixture.auctionID)
	if err != nil || !ok {
		t.Fatalf("read state before bid: ok=%v err=%v", ok, err)
	}
	result, err := fixture.bids.ArbitrateFromCommand(ctx, snapshot)
	if err != nil {
		t.Fatalf("arbitrate: %v", err)
	}
	if !result.Accepted {
		t.Fatalf("async bid not accepted: %+v", result)
	}
	if !result.Extended {
		t.Fatalf("expected anti-sniping to extend endTime, got %+v", result)
	}
	// endTime 必须严格晚于出价前的 endTime（被 +AntiExtendMS=30s 推后）。
	if !result.EndTime.After(endBefore.EndTime) {
		t.Fatalf("expected new endTime > old, old=%s new=%s", endBefore.EndTime, result.EndTime)
	}
}
