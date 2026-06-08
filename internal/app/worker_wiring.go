package app

import (
	"context"
	"log/slog"
	"time"

	appruntime "aieas_backend/internal/app/runtime"
	appconfig "aieas_backend/internal/config"
	orderapp "aieas_backend/internal/modules/order/app"
	wstransport "aieas_backend/internal/transport/ws"
)

type appWorkerShutdown struct {
	stopWorkers       context.CancelFunc
	depositReconciler *appruntime.DepositReconciler
}

type orderTimeoutWorker interface {
	StartTimeoutWorker(ctx context.Context, interval time.Duration, batchSize int)
}

func startAppWorkers(cfg appconfig.Config, deps ServerDependencies, services appServices) appWorkerShutdown {
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	startBusinessWorkers := shouldStartBusinessWorkers(cfg)
	startWSConsumers := shouldStartWSConsumers(cfg)

	if startBusinessWorkers {
		// Feature flag invalidation only feeds REST/MCP/admin business decisions; ws-gateway
		// does not evaluate those flags on the long-connection hot path.
		deps.FeatureFlags.StartInvalidationSubscriber(workerCtx)
	}
	if startBusinessWorkers && deps.OrderTimeoutWorkerEnabled {
		services.orderTimeout.StartTimeoutWorker(workerCtx, orderapp.DefaultOrderTimeoutScanInterval, orderapp.DefaultOrderTimeoutScanBatchSize)
	}
	if startBusinessWorkers && deps.StartupExpiredAuctionCleanupEnabled {
		go func() {
			cleanupCtx, cancel := context.WithTimeout(workerCtx, 30*time.Second)
			defer cancel()
			expiredAuctions, ok := deps.AuctionRepo.(appruntime.ExpiredAuctionLister)
			if !ok {
				return
			}
			cleaner := appruntime.NewExpiredAuctionCleaner(expiredAuctions, services.hammer, appruntime.DefaultExpiredAuctionCleanupBatchSize)
			result, err := cleaner.Cleanup(cleanupCtx)
			if err != nil {
				slog.Default().Warn("startup expired auction cleanup failed", "error", err, "scanned", result.Scanned, "closed", result.Closed, "skipped", result.Skipped, "failed", result.Failed)
				return
			}
			if result.Scanned > 0 || result.Closed > 0 || result.Failed > 0 {
				slog.Default().Info("startup expired auction cleanup finished", "scanned", result.Scanned, "closed", result.Closed, "skipped", result.Skipped, "failed", result.Failed)
			}
		}()
	}
	if startBusinessWorkers && deps.ScheduledAuctionStarterEnabled {
		if scheduledAuctions, ok := deps.AuctionRepo.(appruntime.ScheduledAuctionLister); ok {
			appruntime.NewScheduledAuctionStarter(scheduledAuctions, services.liveSession, appruntime.DefaultScheduledAuctionStartInterval, appruntime.DefaultScheduledAuctionStartBatchSize).Start(workerCtx)
		}
	}
	if startWSConsumers && deps.EventLog != nil && deps.EventLog.Enabled() {
		wstransport.NewEventRelay(deps.EventLog, deps.Hub, 200*time.Millisecond).Start(workerCtx)
	}
	if startWSConsumers {
		for _, pubSubClient := range deps.PubSubClients {
			broadcaster := wstransport.NewPubSubBroadcaster(pubSubClient, deps.Hub)
			broadcaster.SetBidAsyncCoordinator(deps.BidAsyncCoordinator)
			broadcaster.Start(workerCtx)
		}
	}
	if startBusinessWorkers && deps.EventLog != nil && deps.EventLog.Enabled() {
		if deps.BidEventKafkaProducer != nil && deps.BidEventKafkaConsumer != nil {
			bridge := appruntime.NewRedisBidEventKafkaBridge(deps.EventLog, deps.BidEventKafkaProducer, cfg.Kafka.BidBridgeGroup, "")
			bridge.SetMetrics(deps.MetricsRegistry)
			bridge.Start(workerCtx)
			writer := appruntime.NewKafkaBidRecordWriter(deps.BidRepo, deps.BidEventKafkaConsumer)
			writer.SetMetrics(deps.MetricsRegistry)
			writer.Start(workerCtx)
		} else {
			writer := appruntime.NewBidRecordWriter(deps.BidRepo, deps.EventLog, "")
			writer.SetMetrics(deps.MetricsRegistry)
			writer.Start(workerCtx)
		}
		// 排行榜更新独立 worker：与 BidRecordWriter 共享同一 stream，但用独立 consumer
		// group `bid-ranking-updaters`，避免 MySQL 慢拖慢排行榜可见性。
		rankingWorker := appruntime.NewBidRankingWorker(deps.EventLog, "")
		if services.bid != nil {
			rankingWorker.SetRankingUpdatedCallback(services.bid.NotifyRankingUpdated)
		}
		rankingWorker.SetMetrics(deps.MetricsRegistry)
		rankingWorker.Start(workerCtx)
		streamTrimWorker := appruntime.NewBidStreamTrimWorker(deps.EventLog, 10000)
		streamTrimWorker.Start(workerCtx, time.Second)
		reconciler := appruntime.NewBidRecordReconciler(deps.BidRepo, deps.EventLog)
		reconciler.SetMetrics(deps.MetricsRegistry)
		reconciler.Start(workerCtx, time.Minute)
	}
	if startWSConsumers && deps.OnlineCounter != nil {
		deps.OnlineCounter.StartJanitor(workerCtx, time.Minute)
	}

	// 异步竞价裁决 worker：从 aieas.bid.commands 顺序消费，复用 Lua 裁决，
	// 再通过实时事件总线按 liveSessionId + userId 定向推 bid.result 给 ws-gateway。
	if startBusinessWorkers && deps.BidCommandConsumer != nil && services.bid != nil {
		delivery := bidResultDelivery{coordinator: deps.BidAsyncCoordinator, eventPublisher: deps.RealtimeEventPublisher}
		decisionWorker := appruntime.NewBidDecisionWorker(deps.BidCommandConsumer, services.bid, delivery)
		decisionWorker.SetMetrics(deps.MetricsRegistry)
		decisionWorker.Start(workerCtx)
	}

	// P1-A：押金一致性巡检。仅在显式 enable 时启动；测试场景默认关闭，
	// 避免内存夹具里多跑一根 goroutine 干扰断言。
	var depositReconciler *appruntime.DepositReconciler
	if startBusinessWorkers && deps.DepositReconcilerEnabled {
		depositReconciler = appruntime.NewDepositReconciler(deps.AuctionRepo, deps.DepositRepo, deps.RealtimeStore, 30*time.Second)
		depositReconciler.SetMetrics(deps.MetricsRegistry)
		depositReconciler.Start(context.Background())
	}

	return appWorkerShutdown{stopWorkers: stopWorkers, depositReconciler: depositReconciler}
}

func (s appWorkerShutdown) stop(ctx context.Context) {
	_ = ctx
	if s.stopWorkers != nil {
		s.stopWorkers()
	}
	if s.depositReconciler != nil {
		s.depositReconciler.Stop()
	}
}
