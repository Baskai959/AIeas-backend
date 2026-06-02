// Package metrics 提供进程级 Prometheus 指标 Registry，以及面向 HTTP / 出价 /
// 落槌 / 报名 / Redis / Worker / WebSocket / Agent 各业务领域的指标收集器。
//
// 注意基数约束：所有 label 必须是有限低基数维度（route/method/status/result/
// reason/script/operation/event_type/...）。**严禁** 把 user_id / auction_id /
// request_id / idempotency_key / trace_id / order_id / stream_id 作为 label
// 维度，这些信息只能进入 trace attribute 或日志字段。
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry 封装 Prometheus 注册表与各业务收集器，是可观测性指标的唯一入口。
//
// 当 enabled=false 时，所有 Observe/Inc 方法走 noop，不会触碰 Prometheus 全局
// 状态，方便在禁用 metrics 的部署形态下完全旁路指标流水。
type Registry struct {
	enabled   bool
	namespace string
	reg       *prometheus.Registry

	// HTTP
	httpRequestsTotal    *prometheus.CounterVec
	httpRequestDuration  *prometheus.HistogramVec
	httpInflight         *prometheus.GaugeVec
	httpRequestBodyBytes *prometheus.HistogramVec
	httpResponseBytes    *prometheus.HistogramVec

	// Bid
	bidTotal          *prometheus.CounterVec
	bidDuration       prometheus.Histogram
	bidStageDuration  *prometheus.HistogramVec
	bidRejectTotal    *prometheus.CounterVec
	bidDuplicateTotal prometheus.Counter
	bidFreqLimitTotal prometheus.Counter

	// Hammer
	hammerTotal                   *prometheus.CounterVec
	hammerDuration                prometheus.Histogram
	hammerMySQLTxDuration         prometheus.Histogram
	hammerDuplicateTotal          prometheus.Counter
	hammerOptimisticConflictTotal prometheus.Counter
	hammerMySQLFailTotal          prometheus.Counter

	// Enroll & Deposit
	enrollTotal                *prometheus.CounterVec
	enrollDuration             prometheus.Histogram
	depositReadyTotal          prometheus.Counter
	depositSyncRedisFailTotal  prometheus.Counter
	depositReconcileTotal      *prometheus.CounterVec
	depositReconcileLagSeconds prometheus.Histogram

	// Redis
	redisCommandDuration *prometheus.HistogramVec
	redisCommandErrors   *prometheus.CounterVec
	redisLuaDuration     *prometheus.HistogramVec
	redisLuaErrors       *prometheus.CounterVec

	// Redis connection pool
	redisPoolTotalConns        *prometheus.GaugeVec
	redisPoolIdleConns         *prometheus.GaugeVec
	redisPoolStaleConns        *prometheus.GaugeVec
	redisPoolWaitCount         *prometheus.GaugeVec
	redisPoolWaitDurationTotal *prometheus.GaugeVec
	redisPoolTimeouts          *prometheus.GaugeVec
	redisPoolHits              *prometheus.GaugeVec
	redisPoolMisses            *prometheus.GaugeVec

	// Worker (Redis Stream)
	workerBidRecordConsumeTotal  *prometheus.CounterVec
	workerBidRecordWriteDuration prometheus.Histogram
	workerBidRecordDLQTotal      *prometheus.CounterVec
	workerTaskTotal              *prometheus.CounterVec

	// WebSocket
	wsConnections          prometheus.Gauge
	wsConnectionTotal      *prometheus.CounterVec
	wsBroadcastDuration    prometheus.Histogram
	wsBroadcastFanoutTotal prometheus.Counter
	wsSlowClientDisconnect prometheus.Counter

	// Agent
	agentToolCallTotal   *prometheus.CounterVec
	agentToolCallLatency *prometheus.HistogramVec
}

// Options 控制 Registry 的开关与命名空间前缀。
type Options struct {
	Enabled   bool
	Namespace string
}

var (
	defaultOnce     sync.Once
	defaultRegistry *Registry
)

// NewNoop 返回一个所有方法都 no-op 的 Registry。
// nil 安全：所有方法都对 nil 接收者做兜底。
func NewNoop() *Registry {
	return &Registry{enabled: false, reg: prometheus.NewRegistry()}
}

// Default 返回进程级共享的 noop Registry，便于无注入场景兜底。
func Default() *Registry {
	defaultOnce.Do(func() {
		defaultRegistry = NewNoop()
	})
	return defaultRegistry
}

// New 构造一个启用状态的 Registry。命名空间允许为空。
func New(opts Options) *Registry {
	if !opts.Enabled {
		return NewNoop()
	}
	r := &Registry{
		enabled:   true,
		namespace: opts.Namespace,
		reg:       prometheus.NewRegistry(),
	}
	r.register()
	return r
}

// Enabled 报告 Registry 是否启用。
func (r *Registry) Enabled() bool {
	return r != nil && r.enabled
}

// Handler 返回 Prometheus text-format /metrics handler。禁用时返回 503。
func (r *Registry) Handler() http.Handler {
	if !r.Enabled() {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics disabled", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: false,
	})
}

// Gatherer 暴露底层 prometheus.Gatherer，便于测试断言。
func (r *Registry) Gatherer() prometheus.Gatherer {
	if r == nil || r.reg == nil {
		return prometheus.NewRegistry()
	}
	return r.reg
}

func (r *Registry) register() {
	ns := r.namespace
	durBucketsFast := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	durBucketsHTTP := prometheus.DefBuckets
	durBucketsRedis := []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5}
	bytesBuckets := []float64{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}

	// HTTP -----------------------------------------------------------------
	r.httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "http_requests_total", Help: "HTTP requests count",
	}, []string{"method", "route", "status"})
	r.httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds",
		Help: "HTTP request duration in seconds", Buckets: durBucketsHTTP,
	}, []string{"method", "route", "status"})
	r.httpInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "http_inflight_requests", Help: "In-flight HTTP requests",
	}, []string{"route"})
	r.httpRequestBodyBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_body_bytes", Help: "HTTP request body size in bytes",
		Buckets: bytesBuckets,
	}, []string{"route"})
	r.httpResponseBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_response_body_bytes", Help: "HTTP response body size in bytes",
		Buckets: bytesBuckets,
	}, []string{"route"})

	// Bid ------------------------------------------------------------------
	r.bidTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_bid_total", Help: "Auction bid attempts",
	}, []string{"result", "reason"})
	r.bidDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_bid_duration_seconds", Help: "Bid handler duration",
		Buckets: durBucketsFast,
	})
	r.bidStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_bid_stage_duration_seconds", Help: "Bid handler stage duration",
		Buckets: durBucketsFast,
	}, []string{"stage", "result"})
	r.bidRejectTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_bid_reject_total", Help: "Bid rejection by reason",
	}, []string{"reason"})
	r.bidDuplicateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_bid_duplicate_total", Help: "Duplicate bid attempts",
	})
	r.bidFreqLimitTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_bid_freq_limit_total", Help: "Bids hitting frequency limit",
	})

	// Hammer ---------------------------------------------------------------
	r.hammerTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_hammer_total", Help: "Hammer attempts",
	}, []string{"result"})
	r.hammerDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_hammer_duration_seconds", Help: "Hammer duration",
		Buckets: durBucketsFast,
	})
	r.hammerMySQLTxDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_hammer_mysql_tx_duration_seconds", Help: "Hammer MySQL tx duration",
		Buckets: durBucketsFast,
	})
	r.hammerDuplicateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_hammer_duplicate_total", Help: "Duplicate hammer requests",
	})
	r.hammerOptimisticConflictTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_hammer_optimistic_conflict_total", Help: "Hammer optimistic-lock conflicts",
	})
	r.hammerMySQLFailTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_hammer_mysql_fail_total", Help: "Hammer MySQL persistence failures",
	})

	// Enroll & deposit -----------------------------------------------------
	r.enrollTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_enroll_total", Help: "Enrollment outcome",
	}, []string{"result"})
	r.enrollDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_enroll_duration_seconds", Help: "Enroll handler duration",
		Buckets: durBucketsFast,
	})
	r.depositReadyTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_deposit_ready_total", Help: "Deposit ledger transitions to READY",
	})
	r.depositSyncRedisFailTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_deposit_sync_redis_fail_total", Help: "Deposit Redis sync failures",
	})
	r.depositReconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "auction_deposit_reconcile_total", Help: "Deposit reconcile outcome",
	}, []string{"result"})
	r.depositReconcileLagSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "auction_deposit_reconcile_lag_seconds", Help: "Deposit reconcile lag",
		Buckets: []float64{0.5, 1, 5, 10, 30, 60, 300, 600},
	})

	// Redis ----------------------------------------------------------------
	r.redisCommandDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "redis_command_duration_seconds", Help: "Redis command duration",
		Buckets: durBucketsRedis,
	}, []string{"instance", "op"})
	r.redisCommandErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "redis_command_errors_total", Help: "Redis command errors",
	}, []string{"instance", "op"})
	r.redisLuaDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "redis_lua_duration_seconds", Help: "Redis Lua script duration",
		Buckets: durBucketsRedis,
	}, []string{"script"})
	r.redisLuaErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "redis_lua_errors_total", Help: "Redis Lua script errors",
	}, []string{"script", "error"})

	// Redis connection pool --------------------------------------------------
	r.redisPoolTotalConns = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_total_conns", Help: "Redis pool total connections",
	}, []string{"instance"})
	r.redisPoolIdleConns = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_idle_conns", Help: "Redis pool idle connections",
	}, []string{"instance"})
	r.redisPoolStaleConns = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_stale_conns", Help: "Redis pool stale connections",
	}, []string{"instance"})
	r.redisPoolWaitCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_wait_count_total", Help: "Redis pool cumulative wait count",
	}, []string{"instance"})
	r.redisPoolWaitDurationTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_wait_duration_seconds_total", Help: "Redis pool cumulative wait duration in seconds",
	}, []string{"instance"})
	r.redisPoolTimeouts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_timeouts_total", Help: "Redis pool cumulative timeouts",
	}, []string{"instance"})
	r.redisPoolHits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_hits_total", Help: "Redis pool cumulative connection hits",
	}, []string{"instance"})
	r.redisPoolMisses = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "redis_pool_misses_total", Help: "Redis pool cumulative connection misses",
	}, []string{"instance"})

	// Worker ---------------------------------------------------------------
	r.workerBidRecordConsumeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "worker_bid_record_consume_total", Help: "Bid record worker consume outcome",
	}, []string{"result"})
	r.workerBidRecordWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "worker_bid_record_write_duration_seconds", Help: "Bid record write duration",
		Buckets: durBucketsFast,
	})
	r.workerBidRecordDLQTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "worker_bid_record_dlq_total", Help: "Bid record dead-letter queue writes",
	}, []string{"reason"})
	r.workerTaskTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "worker_task_total", Help: "Background worker task outcome",
	}, []string{"worker", "result"})

	// WebSocket ------------------------------------------------------------
	r.wsConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "ws_connections", Help: "Current WebSocket connections",
	})
	r.wsConnectionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_connection_total", Help: "WebSocket connection lifecycle events",
	}, []string{"event"})
	r.wsBroadcastDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "ws_broadcast_duration_seconds", Help: "WebSocket broadcast duration",
		Buckets: durBucketsFast,
	})
	r.wsBroadcastFanoutTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_broadcast_fanout_total", Help: "WebSocket broadcast fan-out total",
	})
	r.wsSlowClientDisconnect = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "ws_slow_client_disconnect_total", Help: "WebSocket slow-client disconnects",
	})

	// Agent ----------------------------------------------------------------
	r.agentToolCallTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "agent_tool_call_total", Help: "Agent tool call outcome",
	}, []string{"tool", "status"})
	r.agentToolCallLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "agent_tool_call_duration_seconds", Help: "Agent tool call duration",
		Buckets: durBucketsHTTP,
	}, []string{"tool"})

	collectors := []prometheus.Collector{
		r.httpRequestsTotal, r.httpRequestDuration, r.httpInflight,
		r.httpRequestBodyBytes, r.httpResponseBytes,
		r.bidTotal, r.bidDuration, r.bidStageDuration, r.bidRejectTotal,
		r.bidDuplicateTotal, r.bidFreqLimitTotal,
		r.hammerTotal, r.hammerDuration,
		r.hammerMySQLTxDuration, r.hammerDuplicateTotal,
		r.hammerOptimisticConflictTotal, r.hammerMySQLFailTotal,
		r.enrollTotal, r.enrollDuration,
		r.depositReadyTotal, r.depositSyncRedisFailTotal,
		r.depositReconcileTotal, r.depositReconcileLagSeconds,
		r.redisCommandDuration, r.redisCommandErrors, r.redisLuaDuration,
		r.redisLuaErrors,
		r.redisPoolTotalConns, r.redisPoolIdleConns, r.redisPoolStaleConns,
		r.redisPoolWaitCount, r.redisPoolWaitDurationTotal, r.redisPoolTimeouts,
		r.redisPoolHits, r.redisPoolMisses,
		r.workerBidRecordConsumeTotal, r.workerBidRecordWriteDuration,
		r.workerBidRecordDLQTotal,
		r.workerTaskTotal,
		r.wsConnections, r.wsConnectionTotal, r.wsBroadcastDuration,
		r.wsBroadcastFanoutTotal, r.wsSlowClientDisconnect,
		r.agentToolCallTotal, r.agentToolCallLatency,
	}
	for _, c := range collectors {
		r.reg.MustRegister(c)
	}
	r.reg.MustRegister(prometheus.NewGoCollector())
	r.reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
}
