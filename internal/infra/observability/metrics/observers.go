package metrics

import (
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

// statusBucket 把 HTTP 状态码归入低基数桶，避免使用具体的 400/401/... 字符串。
func statusBucket(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	case status >= 100:
		return "1xx"
	default:
		return "unknown"
	}
}

// HTTPStatusLabel 暴露给 middleware 使用，让 label 维度统一收敛在 metrics 包。
func HTTPStatusLabel(status int) string { return statusBucket(status) }

// ----- HTTP -----

// ObserveHTTP 记录单次 HTTP 请求的 method/route/status 组合及耗时与体积。
func (r *Registry) ObserveHTTP(method, route string, status int, elapsed time.Duration, reqBytes, respBytes int) {
	if !r.Enabled() {
		return
	}
	st := statusBucket(status)
	r.httpRequestsTotal.WithLabelValues(method, route, st).Inc()
	r.httpRequestDuration.WithLabelValues(method, route, st).Observe(elapsed.Seconds())
	if reqBytes > 0 {
		r.httpRequestBodyBytes.WithLabelValues(route).Observe(float64(reqBytes))
	}
	if respBytes > 0 {
		r.httpResponseBytes.WithLabelValues(route).Observe(float64(respBytes))
	}
}

// IncHTTPInflight / DecHTTPInflight 在请求开始/结束时调整 inflight gauge。
func (r *Registry) IncHTTPInflight(route string) {
	if !r.Enabled() {
		return
	}
	r.httpInflight.WithLabelValues(route).Inc()
}

func (r *Registry) DecHTTPInflight(route string) {
	if !r.Enabled() {
		return
	}
	r.httpInflight.WithLabelValues(route).Dec()
}

// ----- Bid -----

func (r *Registry) ObserveBid(result, reason string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.bidTotal.WithLabelValues(result, reason).Inc()
	r.bidDuration.Observe(elapsed.Seconds())
}

func (r *Registry) ObserveBidStage(stage, result string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	if stage == "" {
		stage = "unknown"
	}
	if result == "" {
		result = "ok"
	}
	r.bidStageDuration.WithLabelValues(stage, result).Observe(elapsed.Seconds())
}

func (r *Registry) IncBidRoute(decision, reason string) {
	if !r.Enabled() {
		return
	}
	if decision == "" {
		decision = "unknown"
	}
	if reason == "" {
		reason = "none"
	}
	r.bidRouteTotal.WithLabelValues(decision, reason).Inc()
}

func (r *Registry) IncBidReject(reason string) {
	if !r.Enabled() {
		return
	}
	r.bidRejectTotal.WithLabelValues(reason).Inc()
}

func (r *Registry) IncBidDuplicate() {
	if !r.Enabled() {
		return
	}
	r.bidDuplicateTotal.Inc()
}

func (r *Registry) IncBidFreqLimit() {
	if !r.Enabled() {
		return
	}
	r.bidFreqLimitTotal.Inc()
}

// ----- Hammer -----

func (r *Registry) ObserveHammer(result string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.hammerTotal.WithLabelValues(result).Inc()
	r.hammerDuration.Observe(elapsed.Seconds())
}

func (r *Registry) ObserveHammerMySQLTx(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.hammerMySQLTxDuration.Observe(elapsed.Seconds())
}

func (r *Registry) IncHammerDuplicate() {
	if !r.Enabled() {
		return
	}
	r.hammerDuplicateTotal.Inc()
}

func (r *Registry) IncHammerOptimisticConflict() {
	if !r.Enabled() {
		return
	}
	r.hammerOptimisticConflictTotal.Inc()
}

func (r *Registry) IncHammerMySQLFail() {
	if !r.Enabled() {
		return
	}
	r.hammerMySQLFailTotal.Inc()
}

// ----- Enroll & deposit -----

func (r *Registry) ObserveEnroll(result string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.enrollTotal.WithLabelValues(result).Inc()
	r.enrollDuration.Observe(elapsed.Seconds())
}

func (r *Registry) IncDepositReady() {
	if !r.Enabled() {
		return
	}
	r.depositReadyTotal.Inc()
}

func (r *Registry) IncDepositSyncRedisFail() {
	if !r.Enabled() {
		return
	}
	r.depositSyncRedisFailTotal.Inc()
}

func (r *Registry) IncDepositReconcile(result string) {
	if !r.Enabled() {
		return
	}
	r.depositReconcileTotal.WithLabelValues(result).Inc()
}

func (r *Registry) ObserveDepositReconcileLag(d time.Duration) {
	if !r.Enabled() {
		return
	}
	r.depositReconcileLagSeconds.Observe(d.Seconds())
}

// ----- Redis -----

// ObserveRedisCommand 记录单条 Redis 命令的耗时（带 instance 维度）。
// instance 用于区分多 Redis 实例（默认 "default"）；空串时回退到 "default"，
// 以保证标签值永远存在，避免 promQL 选择器漏命中。
func (r *Registry) ObserveRedisCommand(instance, op string, elapsed time.Duration, err error) {
	if !r.Enabled() {
		return
	}
	if instance == "" {
		instance = "default"
	}
	r.redisCommandDuration.WithLabelValues(instance, op).Observe(elapsed.Seconds())
	if err != nil {
		r.redisCommandErrors.WithLabelValues(instance, op).Inc()
	}
}

func (r *Registry) ObserveRedisLua(script string, elapsed time.Duration, errClass string) {
	if !r.Enabled() {
		return
	}
	r.redisLuaDuration.WithLabelValues(script).Observe(elapsed.Seconds())
	if errClass != "" {
		r.redisLuaErrors.WithLabelValues(script, errClass).Inc()
	}
}

// ObserveRedisPoolStats 把某个 Redis 实例的连接池快照写入 gauge（带 instance 维度）。
// WaitDurationNs 换算为秒；instance 空串时回退到 "default"。
func (r *Registry) ObserveRedisPoolStats(instance string, st *redisgo.PoolStats) {
	if !r.Enabled() || st == nil {
		return
	}
	if instance == "" {
		instance = "default"
	}
	r.redisPoolTotalConns.WithLabelValues(instance).Set(float64(st.TotalConns))
	r.redisPoolIdleConns.WithLabelValues(instance).Set(float64(st.IdleConns))
	r.redisPoolStaleConns.WithLabelValues(instance).Set(float64(st.StaleConns))
	r.redisPoolWaitCount.WithLabelValues(instance).Set(float64(st.WaitCount))
	r.redisPoolWaitDurationTotal.WithLabelValues(instance).Set(float64(st.WaitDurationNs) / 1e9)
	r.redisPoolTimeouts.WithLabelValues(instance).Set(float64(st.Timeouts))
	r.redisPoolHits.WithLabelValues(instance).Set(float64(st.Hits))
	r.redisPoolMisses.WithLabelValues(instance).Set(float64(st.Misses))
}

// ----- Worker -----

func (r *Registry) IncWorkerBidRecordConsume(result string) {
	if !r.Enabled() {
		return
	}
	r.workerBidRecordConsumeTotal.WithLabelValues(result).Inc()
}

func (r *Registry) ObserveWorkerBidRecordWrite(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.workerBidRecordWriteDuration.Observe(elapsed.Seconds())
}

func (r *Registry) IncWorkerBidRecordDLQ(reason string) {
	if !r.Enabled() {
		return
	}
	r.workerBidRecordDLQTotal.WithLabelValues(reason).Inc()
}

func (r *Registry) IncWorkerBidRankingConsume(result string) {
	if !r.Enabled() {
		return
	}
	r.workerBidRankingConsumeTotal.WithLabelValues(result).Inc()
}

func (r *Registry) IncWorkerTask(worker, result string) {
	if !r.Enabled() {
		return
	}
	r.workerTaskTotal.WithLabelValues(worker, result).Inc()
}

// ----- WebSocket -----

func (r *Registry) IncWSConnect() {
	if !r.Enabled() {
		return
	}
	r.wsConnections.Inc()
	r.wsConnectionTotal.WithLabelValues("connect").Inc()
}

func (r *Registry) IncWSDisconnect(reason string) {
	if !r.Enabled() {
		return
	}
	r.wsConnections.Dec()
	if reason == "" {
		reason = "normal"
	}
	r.wsConnectionTotal.WithLabelValues("disconnect_" + reason).Inc()
}

func (r *Registry) ObserveWSBroadcast(elapsed time.Duration, fanout int) {
	if !r.Enabled() {
		return
	}
	r.wsBroadcastDuration.Observe(elapsed.Seconds())
	if fanout > 0 {
		r.wsBroadcastFanoutTotal.Add(float64(fanout))
	}
}

func (r *Registry) IncWSSlowClientDisconnect() {
	if !r.Enabled() {
		return
	}
	r.wsSlowClientDisconnect.Inc()
}

// IncWSHandshakeReject 记录一次握手被拒绝的事件。reason 限定于封闭集合：
// rate_limit_ip / rate_limit_user / rate_limit_auction / draining / auth /
// bad_request；其他值归一为 unknown，避免 label 基数失控。
func (r *Registry) IncWSHandshakeReject(reason string) {
	if !r.Enabled() {
		return
	}
	switch reason {
	case "rate_limit_ip", "rate_limit_user", "rate_limit_auction", "draining", "auth", "bad_request":
	default:
		reason = "unknown"
	}
	r.wsHandshakeRejectTotal.WithLabelValues(reason).Inc()
}

// IncWSDraining 记录一次 BeginDrain 触发。
func (r *Registry) IncWSDraining() {
	if !r.Enabled() {
		return
	}
	r.wsDrainingTotal.Inc()
}

// ----- Agent -----

func (r *Registry) ObserveAgentToolCall(tool, status string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.agentToolCallTotal.WithLabelValues(tool, status).Inc()
	r.agentToolCallLatency.WithLabelValues(tool).Observe(elapsed.Seconds())
}
