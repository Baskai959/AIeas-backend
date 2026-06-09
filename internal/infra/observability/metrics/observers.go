package metrics

import (
	"strconv"
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

// ----- Async bid (异步竞价裁决) -----

// IncBidPlaceMode 记录一次出价走的路由模式（sync/async/async_fallback_sync）。
func (r *Registry) IncBidPlaceMode(mode string) {
	if !r.Enabled() {
		return
	}
	if mode == "" {
		mode = "unknown"
	}
	r.bidPlaceModeTotal.WithLabelValues(mode).Inc()
}

// ObserveBidAckDuration 记录 WS bid.place 到 bid.ack 入出站处理完成的耗时。
func (r *Registry) ObserveBidAckDuration(mode, result string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	if mode == "" {
		mode = "unknown"
	}
	if result == "" {
		result = "unknown"
	}
	r.bidAckDuration.WithLabelValues(mode, result).Observe(elapsed.Seconds())
}

// ObserveBidKafkaEnqueue 记录异步命令入队（PublishBidCommand）耗时。
func (r *Registry) ObserveBidKafkaEnqueue(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.bidKafkaEnqueueDuration.Observe(elapsed.Seconds())
}

// ObserveBidDecisionDuration 记录 worker 裁决（ArbitrateFromCommand）耗时。
func (r *Registry) ObserveBidDecisionDuration(result string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	if result == "" {
		result = "unknown"
	}
	r.bidDecisionDuration.WithLabelValues(result).Observe(elapsed.Seconds())
}

// SetBidPendingQueueSize 设置当前全局待裁决队列长度。
func (r *Registry) SetBidPendingQueueSize(size int) {
	if !r.Enabled() {
		return
	}
	r.bidPendingQueueSize.Set(float64(size))
}

// ObserveBidResultPush 记录 bid.result 定向推送耗时。
func (r *Registry) ObserveBidResultPush(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.bidResultPushDuration.Observe(elapsed.Seconds())
}

// IncBidWorkerPoolInflight 增加并发裁决 goroutine 计数（worker handle 入口）。
func (r *Registry) IncBidWorkerPoolInflight() {
	if !r.Enabled() {
		return
	}
	r.bidWorkerPoolInflight.Inc()
}

// DecBidWorkerPoolInflight 递减并发裁决 goroutine 计数（worker handle 出口，
// recover 路径也必须保证调用，避免泄漏）。
func (r *Registry) DecBidWorkerPoolInflight() {
	if !r.Enabled() {
		return
	}
	r.bidWorkerPoolInflight.Dec()
}

// ObserveBidWorkerCommitLag 记录命令从 fetch 到实际 commit 的耗时。
func (r *Registry) ObserveBidWorkerCommitLag(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.bidWorkerCommitLag.Observe(elapsed.Seconds())
}

// SetBidKafkaPartitionLag 设置某个 partition 当前消费滞后量（字节/条数）。
func (r *Registry) SetBidKafkaPartitionLag(partition int, lag int64) {
	if !r.Enabled() {
		return
	}
	r.bidKafkaPartitionLag.WithLabelValues(strconv.Itoa(partition)).Set(float64(lag))
}

// ObserveBidResultDuration 记录异步出价从 pending 入队到 bid.result 定向推送的端到端耗时。
func (r *Registry) ObserveBidResultDuration(outcome string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	if outcome == "" {
		outcome = "unknown"
	}
	r.bidResultDuration.WithLabelValues(outcome).Observe(elapsed.Seconds())
}

// IncBidResultAckTimeout 记录一次 bid.result ack 超时（触发重发或超限）。
func (r *Registry) IncBidResultAckTimeout() {
	if !r.Enabled() {
		return
	}
	r.bidResultAckTimeoutTot.Inc()
}

// IncBidQueueReject 记录一次队列保护拒绝。
func (r *Registry) IncBidQueueReject(reason string) {
	if !r.Enabled() {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	r.bidQueueRejectTotal.WithLabelValues(reason).Inc()
}

// IncBidDecisionOutcome 记录一次裁决终态（accepted/rejected/duplicate/error）。
func (r *Registry) IncBidDecisionOutcome(outcome string) {
	if !r.Enabled() {
		return
	}
	if outcome == "" {
		outcome = "unknown"
	}
	r.bidDecisionOutcomeTotal.WithLabelValues(outcome).Inc()
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

// IncHammerPending 记录一次 HAMMER_PENDING 过渡，trigger 维度限定低基数：
// timer / cap_price / expired / manual / system / unknown。
func (r *Registry) IncHammerPending(trigger string) {
	if !r.Enabled() {
		return
	}
	switch trigger {
	case "timer", "cap_price", "expired", "manual", "system":
	default:
		trigger = "unknown"
	}
	r.hammerPendingTotal.WithLabelValues(trigger).Inc()
}

// ObserveHammerDrain 记录一次屏障实际等待时长（不论 ok/timeout 都记录）。
func (r *Registry) ObserveHammerDrain(elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	r.hammerDrainDuration.Observe(elapsed.Seconds())
}

// IncHammerDrainTimeout 记录一次屏障超时 fallback。
func (r *Registry) IncHammerDrainTimeout() {
	if !r.Enabled() {
		return
	}
	r.hammerDrainTimeoutTotal.Inc()
}

// IncBidCommandPublishReject 记录一次 publisher 闸门拒绝（reason=hammer_pending）。
func (r *Registry) IncBidCommandPublishReject(reason string) {
	if !r.Enabled() {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	r.bidCommandPublishRejectTotal.WithLabelValues(reason).Inc()
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

// ObserveRedisCommand 记录单条 Redis 命令的耗时（带 instance + command 维度）。
// instance 用于区分多 Redis 实例（默认 "default"）；空串时回退到 "default"，
// 以保证标签值永远存在，避免 promQL 选择器漏命中。
// command 取自 redis.Cmder.Name() 大写后的命令名（GET/SET/EVALSHA/HMGET/...），
// Redis 命令集合是低基数封闭集，安全。空串归一为 "UNKNOWN"。
func (r *Registry) ObserveRedisCommand(instance, command string, elapsed time.Duration, err error) {
	if !r.Enabled() {
		return
	}
	if instance == "" {
		instance = "default"
	}
	if command == "" {
		command = "UNKNOWN"
	}
	r.redisCommandDuration.WithLabelValues(instance, command).Observe(elapsed.Seconds())
	if err != nil {
		r.redisCommandErrors.WithLabelValues(instance, command).Inc()
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

// IncBidLuaQueueDepth / DecBidLuaQueueDepth 记录出价 Lua 仲裁在客户端侧的
// in-flight 深度。shard / instance / script 均为低基数封闭集合；空值兜底到
// default / bid_place，避免生成缺标签时间序列。
func (r *Registry) IncBidLuaQueueDepth(shard, instance, script string) {
	if !r.Enabled() {
		return
	}
	shard, instance, script = normalizeBidLuaLabels(shard, instance, script)
	r.bidLuaQueueDepth.WithLabelValues(shard, instance, script).Inc()
}

func (r *Registry) DecBidLuaQueueDepth(shard, instance, script string) {
	if !r.Enabled() {
		return
	}
	shard, instance, script = normalizeBidLuaLabels(shard, instance, script)
	r.bidLuaQueueDepth.WithLabelValues(shard, instance, script).Dec()
}

// ObserveBidLuaRoundTrip 记录出价 Lua 仲裁的客户端观测往返耗时。
// 该指标不能拆分 Redis 服务端排队时间与执行时间，因此命名为 roundtrip。
func (r *Registry) ObserveBidLuaRoundTrip(shard, instance, script, status string, elapsed time.Duration) {
	if !r.Enabled() {
		return
	}
	shard, instance, script = normalizeBidLuaLabels(shard, instance, script)
	status = normalizeBidLuaStatus(status)
	r.bidLuaRoundTrip.WithLabelValues(shard, instance, script, status).Observe(elapsed.Seconds())
}

func normalizeBidLuaLabels(shard, instance, script string) (string, string, string) {
	if shard == "" {
		shard = "default"
	}
	if instance == "" {
		instance = "default"
	}
	if script == "" {
		script = "bid_place"
	}
	return shard, instance, script
}

func normalizeBidLuaStatus(status string) string {
	switch status {
	case "ok", "noscript", "busy", "timeout", "connection", "error", "panic":
		return status
	case "":
		return "ok"
	default:
		return "error"
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
