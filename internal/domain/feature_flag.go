package domain

import "time"

// FeatureFlag 描述一个具名特性开关。
//
// 用于将运行时可调的"模块/路径开关"从代码常量解放到配置存储 + 本地缓存：
//   - Enabled：全局总开关，false 时其余字段无效；
//   - RolloutPercentage：灰度比例 0-100；UserID hash 落在 [0, P) 之间视为命中；
//   - Allowlist：白名单，命中即直接通过（不受 RolloutPercentage 限制）；
//   - UpdatedBy/UpdatedAt：审计字段，由 service 层填充。
type FeatureFlag struct {
	Key               string    `json:"key"`
	Enabled           bool      `json:"enabled"`
	RolloutPercentage int       `json:"rolloutPercentage"`
	Allowlist         []string  `json:"allowlist,omitempty"`
	Description       string    `json:"description,omitempty"`
	UpdatedBy         string    `json:"updatedBy,omitempty"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// 特性开关 key 常量。新增 flag 时统一在此登记，避免散落在调用点。
const (
	// FeatureFlagEventRelay 控制 EventRelay (XRANGE 拉流→Hub 广播) 是否启动。
	FeatureFlagEventRelay = "feature.event_relay.enabled"
	// FeatureFlagBidRecordWriter 控制 BidRecordWriter（XReadGroup 落库）是否启动。
	FeatureFlagBidRecordWriter = "feature.bid_record_writer.enabled"
	// FeatureFlagDepositReconciler 控制 DepositReconciler 巡检是否启动。
	FeatureFlagDepositReconciler = "feature.deposit_reconciler.enabled"
	// FeatureFlagPubSubBroadcast 控制 Pub/Sub 广播 (bid.lua PUBLISH + 订阅转发) 是否启用。
	FeatureFlagPubSubBroadcast = "feature.pubsub.broadcast_enabled"
	// FeatureFlagDistributedRateLimit 控制 HTTP L2 分布式限流是否启用。
	FeatureFlagDistributedRateLimit = "feature.ratelimit.distributed_enabled"
)

// DefaultFeatureFlags 返回所有已知 feature flag 的默认值。
// FeatureFlagService 在 FindByKey 命中 ErrNotFound 时回退到这里，保证
// 新发布版本在配置项尚未写入存储时仍有合理默认行为。
func DefaultFeatureFlags() map[string]FeatureFlag {
	return map[string]FeatureFlag{
		FeatureFlagEventRelay: {
			Key: FeatureFlagEventRelay, Enabled: true, RolloutPercentage: 100,
			Description: "EventRelay (XRANGE → WebSocket Hub) 总开关",
		},
		FeatureFlagBidRecordWriter: {
			Key: FeatureFlagBidRecordWriter, Enabled: true, RolloutPercentage: 100,
			Description: "BidRecordWriter (XReadGroup → MySQL) 总开关",
		},
		FeatureFlagDepositReconciler: {
			Key: FeatureFlagDepositReconciler, Enabled: true, RolloutPercentage: 100,
			Description: "押金一致性巡检 DepositReconciler 总开关",
		},
		FeatureFlagPubSubBroadcast: {
			Key: FeatureFlagPubSubBroadcast, Enabled: false, RolloutPercentage: 0,
			Description: "Pub/Sub 广播（bid.lua PUBLISH + Hub 订阅转发） 总开关",
		},
		FeatureFlagDistributedRateLimit: {
			Key: FeatureFlagDistributedRateLimit, Enabled: false, RolloutPercentage: 0,
			Description: "HTTP L2 分布式限流（Redis 令牌桶）总开关",
		},
	}
}
