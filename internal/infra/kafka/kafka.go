package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"

	kafkago "github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	Brokers               []string
	ClientID              string
	BidEventsTopic        string
	BidCommandsTopic      string
	BidCommandsPartitions int
	AuctionEventsTopic    string
	OrderEventsTopic      string
}

type Producer struct {
	brokers               []string
	clientID              string
	bidEventsTopic        string
	bidCommandsTopic      string
	bidCommandsPartitions int
	auctionEventsTopic    string
	orderEventsTopic      string

	mu      sync.Mutex
	writers map[string]*kafkago.Writer
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	brokers := normalizeBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka producer requires at least one broker")
	}
	clientID := strings.TrimSpace(cfg.ClientID)
	if clientID == "" {
		clientID = "aieas-backend"
	}
	partitions := cfg.BidCommandsPartitions
	if partitions <= 0 {
		partitions = defaultBidCommandsPartitions
	}
	return &Producer{
		brokers:               brokers,
		clientID:              clientID,
		bidEventsTopic:        defaultString(cfg.BidEventsTopic, "aieas.bid.events"),
		bidCommandsTopic:      defaultString(cfg.BidCommandsTopic, "aieas.bid.commands"),
		bidCommandsPartitions: partitions,
		auctionEventsTopic:    defaultString(cfg.AuctionEventsTopic, "aieas.auction.events"),
		orderEventsTopic:      defaultString(cfg.OrderEventsTopic, "aieas.order.events"),
		writers:               make(map[string]*kafkago.Writer),
	}, nil
}

// defaultBidCommandsPartitions 与 config.DefaultBidCommandsPartitions 保持一致；
// 这里复制一个常量是为了避免 infra/kafka 反向依赖 internal/config。
const defaultBidCommandsPartitions = 16

// EnsureBidCommandsTopic 在装配期幂等地创建 aieas.bid.commands topic 并使用配置
// 的分区数。CreateTopics 对已存在的 topic 是 no-op（不会改写已有分区），所以本
// 方法仅保证“首次启动 / 新环境”自动建出 16 partition 的 topic；存量 topic 的
// 扩容仍需 DBA/运维手工执行：
//
//	kafka-topics.sh --alter --topic aieas.bid.commands --partitions 16
//
// 实现使用 segmentio/kafka-go 的 (*kafka.Conn).CreateTopics——它内部会先
// 通过任意 broker 拿到 controller 信息再执行 CreateTopics RPC。失败时由调用方
// log warn 不阻塞启动，以避免 Kafka 临时不可达把整个服务 readiness 拖垮。
func (p *Producer) EnsureBidCommandsTopic(ctx context.Context) error {
	if p == nil {
		return nil
	}
	topic := strings.TrimSpace(p.bidCommandsTopic)
	if topic == "" {
		return nil
	}
	partitions := p.bidCommandsPartitions
	if partitions <= 0 {
		partitions = defaultBidCommandsPartitions
	}
	dialer := &kafkago.Dialer{ClientID: p.clientID, Timeout: 5 * time.Second}
	var lastErr error
	for _, broker := range p.brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		// CreateTopics 在 segmentio/kafka-go 中会自动定位 controller，所以
		// 任意一个可达 broker 都可以发起调用。
		err = conn.CreateTopics(kafkago.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: -1, // 让 broker 使用 default.replication.factor
		})
		_ = conn.Close()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return fmt.Errorf("ensure topic %s: no kafka brokers configured", topic)
	}
	return fmt.Errorf("ensure topic %s with %d partitions: %w", topic, partitions, lastErr)
}

// BidCommand 是异步竞价裁决的命令消息：由 WS handler 在 preCheck 通过后投递，
// 由 BidDecisionWorker 并发消费并复用 Lua 裁决。
//
// Route X：Kafka key 使用 bid/request 维度，不再用 auctionId 固定分区；Kafka
// 不承载同拍品顺序保证，业务正确性边界由 Redis EvalOnShard + bid.lua + idem key 提供。
type BidCommand struct {
	BidID                string          `json:"bidId"`
	AuctionID            uint64          `json:"auctionId"`
	LiveSessionID        uint64          `json:"liveSessionId"`
	UserID               string          `json:"userId"`
	SellerID             string          `json:"sellerId"`
	Price                int64           `json:"price"`
	ExpectedCurrentPrice *int64          `json:"expectedCurrentPrice"`
	Source               string          `json:"source"`
	MinIncrement         int64           `json:"minIncrement"`
	AntiSnipingMS        int64           `json:"antiSnipingMs"`
	AntiExtendMS         int64           `json:"antiExtendMs"`
	AntiExtendMode       string          `json:"antiExtendMode"`
	MaxExtendCount       int             `json:"maxExtendCount"`
	FreqLimitCount       int             `json:"freqLimitCount"`
	FreqWindowMS         int64           `json:"freqWindowMs"`
	StartPrice           int64           `json:"startPrice"`
	CapPrice             int64           `json:"capPrice"`
	IncrementRule        json.RawMessage `json:"incrementRule"`
	BidderNickname       string          `json:"bidderNickname"`
	BidderAvatarURL      string          `json:"bidderAvatarUrl"`
	PreCheckPassed       bool            `json:"preCheckPassed"`
	EnqueuedAtMS         int64           `json:"enqueuedAtMs"`
	OriginInstanceID     string          `json:"originInstanceId,omitempty"`

	// TraceParent / TraceState 仅用于跨进程链路透传，不参与 JSON 消息体编码。
	TraceParent string `json:"-"`
	TraceState  string `json:"-"`
}

// PublishBidCommand 把竞价命令投递到 aieas.bid.commands。
//
// Route X：message key 不再是 auctionId，而是包含 bid/request 维度的 command key；
// 继续配合 Hash balancer，让同一 auction 的不同 bid 可分散到不同 partition。
// 同拍品并发乱序由 Redis Lua / EvalOnShard / idem key 保证正确性。
// Kafka disabled 时 Producer 为 nil，直接返回 nil。
func (p *Producer) PublishBidCommand(ctx context.Context, cmd BidCommand) error {
	if p == nil {
		return nil
	}
	msg, err := bidCommandMessage(ctx, cmd)
	if err != nil {
		return err
	}
	return p.publishMessages(ctx, p.bidCommandsTopic, msg)
}

func bidCommandMessage(ctx context.Context, cmd BidCommand) (kafkago.Message, error) {
	if cmd.EnqueuedAtMS == 0 {
		cmd.EnqueuedAtMS = time.Now().UTC().UnixMilli()
	}
	value, err := json.Marshal(cmd)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("marshal bid command: %w", err)
	}
	headers := map[string]string{"command_type": "bid.place"}
	tracing.InjectMap(ctx, headers)
	return kafkago.Message{
		Key:     []byte(bidCommandKey(cmd)),
		Value:   value,
		Time:    time.Now().UTC(),
		Headers: kafkaHeaders(headers),
	}, nil
}

func bidCommandKey(cmd BidCommand) string {
	if bidID := strings.TrimSpace(cmd.BidID); bidID != "" {
		return fmt.Sprintf("bid:%d:%s", cmd.AuctionID, bidID)
	}
	return fmt.Sprintf("bid:%d:fallback:%s:%d:%d", cmd.AuctionID, strings.TrimSpace(cmd.UserID), cmd.Price, cmd.EnqueuedAtMS)
}

func (p *Producer) PublishBidEvent(ctx context.Context, event redisinfra.BidEvent) error {
	if p == nil {
		return nil
	}
	msg, err := p.bidEventMessage(ctx, event)
	if err != nil {
		return err
	}
	return p.publishMessages(ctx, p.bidEventsTopic, msg)
}

func (p *Producer) PublishBidEvents(ctx context.Context, events []redisinfra.BidEvent) error {
	if p == nil || len(events) == 0 {
		return nil
	}
	messages := make([]kafkago.Message, 0, len(events))
	for _, event := range events {
		msg, err := p.bidEventMessage(ctx, event)
		if err != nil {
			return err
		}
		messages = append(messages, msg)
	}
	return p.publishMessages(ctx, p.bidEventsTopic, messages...)
}

func (p *Producer) bidEventMessage(ctx context.Context, event redisinfra.BidEvent) (kafkago.Message, error) {
	payload := map[string]interface{}{}
	if err := json.Unmarshal(event.PayloadJSON(), &payload); err != nil {
		return kafkago.Message{}, fmt.Errorf("marshal bid kafka event: %w", err)
	}
	payload["eventType"] = event.EventType
	value, err := json.Marshal(payload)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("marshal bid kafka event: %w", err)
	}
	headers := map[string]string{"event_type": event.EventType}
	for k, v := range event.TraceCarrier() {
		headers[k] = v
	}
	if headers["traceparent"] == "" {
		tracing.InjectMap(ctx, headers)
	}
	return kafkago.Message{
		Key:     []byte(strconv.FormatUint(event.AuctionID, 10)),
		Value:   value,
		Time:    time.Now().UTC(),
		Headers: kafkaHeaders(headers),
	}, nil
}

func (p *Producer) PublishAuctionClosed(ctx context.Context, auction domain.AuctionLot, result domain.HammerResult, order *domain.OrderDeal) error {
	if p == nil {
		return nil
	}
	payload := map[string]interface{}{
		"eventType":     "auction.closed",
		"requestId":     result.RequestID,
		"auctionId":     result.AuctionID,
		"sellerId":      auction.SellerID,
		"title":         auction.Title,
		"category":      auction.Category,
		"status":        result.Status,
		"winnerId":      result.WinnerID,
		"price":         result.Price,
		"closedAt":      result.ClosedAt,
		"closedAtMs":    result.ClosedAt.UnixMilli(),
		"closedBy":      auction.ClosedBy,
		"version":       result.Version,
		"depositAmount": auction.DepositAmount,
	}
	if auction.LiveSessionID != nil {
		payload["liveSessionId"] = *auction.LiveSessionID
	}
	if order != nil {
		payload["orderId"] = order.ID
	}
	value, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal auction.closed kafka event: %w", err)
	}
	headers := map[string]string{"event_type": "auction.closed"}
	tracing.InjectMap(ctx, headers)
	return p.publish(ctx, p.auctionEventsTopic, strconv.FormatUint(result.AuctionID, 10), value, headers)
}

func (p *Producer) PublishOrderCreated(ctx context.Context, order domain.OrderDeal) error {
	if p == nil || order.ID == 0 {
		return nil
	}
	payload := map[string]interface{}{
		"eventType":     "order.created",
		"orderId":       order.ID,
		"auctionId":     order.AuctionID,
		"winnerId":      order.WinnerID,
		"sellerId":      order.SellerID,
		"dealPrice":     order.DealPrice,
		"depositAmount": order.DepositAmount,
		"status":        order.Status,
		"payStatus":     order.PayStatus,
		"payDeadline":   order.PayDeadline,
		"createdAt":     order.CreatedAt,
		"updatedAt":     order.UpdatedAt,
	}
	if order.LiveSessionID != nil {
		payload["liveSessionId"] = *order.LiveSessionID
	}
	value, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal order.created kafka event: %w", err)
	}
	headers := map[string]string{"event_type": "order.created"}
	tracing.InjectMap(ctx, headers)
	return p.publish(ctx, p.orderEventsTopic, strconv.FormatUint(order.AuctionID, 10), value, headers)
}

func (p *Producer) Ping(ctx context.Context) error {
	if p == nil {
		return nil
	}
	var lastErr error
	for _, broker := range p.brokers {
		dialer := &kafkago.Dialer{ClientID: p.clientID, Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no kafka brokers configured")
	}
	return lastErr
}

func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close kafka writer %s: %w", topic, err)
		}
	}
	return firstErr
}

func (p *Producer) publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	return p.publishMessages(ctx, topic, kafkago.Message{
		Key:     []byte(key),
		Value:   value,
		Time:    time.Now().UTC(),
		Headers: kafkaHeaders(headers),
	})
}

func (p *Producer) publishMessages(ctx context.Context, topic string, messages ...kafkago.Message) error {
	if len(messages) == 0 {
		return nil
	}
	writer := p.writer(topic)
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write kafka topic %s: %w", topic, err)
	}
	return nil
}

func (p *Producer) writer(topic string) *kafkago.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if writer := p.writers[topic]; writer != nil {
		return writer
	}
	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(p.brokers...),
		Topic: topic,
		// Hash 保持按 message key 路由；bid commands 的 key 已是 bid/request 维度，
		// 因此同 auction 的不同 bid 不再被固定到同一 partition。
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}
	p.writers[topic] = writer
	return writer
}

type BidEventReader struct {
	reader *kafkago.Reader
}

type BidEventReaderConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

func NewBidEventReader(cfg BidEventReaderConfig) (*BidEventReader, error) {
	brokers := normalizeBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka bid event reader requires at least one broker")
	}
	groupID := strings.TrimSpace(cfg.GroupID)
	if groupID == "" {
		groupID = "aieas-bid-record-writers"
	}
	topic := defaultString(cfg.Topic, "aieas.bid.events")
	return &BidEventReader{reader: kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10 << 20,
		MaxWait:  500 * time.Millisecond,
	})}, nil
}

func (r *BidEventReader) FetchBidEvent(ctx context.Context) (redisinfra.BidEvent, func(context.Context) error, error) {
	if r == nil || r.reader == nil {
		return redisinfra.BidEvent{}, nil, fmt.Errorf("kafka bid event reader is not configured")
	}
	msg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return redisinfra.BidEvent{}, nil, err
	}
	event, err := decodeBidEventMessage(msg)
	if err != nil {
		commit := func(commitCtx context.Context) error {
			return r.reader.CommitMessages(commitCtx, msg)
		}
		return redisinfra.BidEvent{}, commit, err
	}
	commit := func(commitCtx context.Context) error {
		return r.reader.CommitMessages(commitCtx, msg)
	}
	return event, commit, nil
}

func (r *BidEventReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

// BidCommandReader 从 aieas.bid.commands 拉取命令。Kafka 仍保证单 partition 内位点顺序，
// 但 Route X 不依赖同 auction 落同 partition，也不把 Kafka FIFO 作为业务正确性边界。
type BidCommandReader struct {
	reader *kafkago.Reader

	// 按 partition 缓存最近一次 fetch 出来的 lag 值（HighWaterMark - Offset - 1）。
	// segmentio/kafka-go 的 Reader.Stats() 在 group consumer 模式下只返回当前
	// reader 持有的单个 partition；且 Partition 字段在多 reader 实例下经常表
	// 现为 "" 或 -1（聚合占位），无法直接拿到真实 partition 编号。这里改为从
	// 每条 FetchMessage 返回的 Message.Partition / Message.HighWaterMark / Offset
	// 自行推算并按 partition 缓存，配合周期上报实现 bid_kafka_partition_lag
	// 的真实 partition 标签。
	lagMu      sync.Mutex
	partitions map[int]int64
}

type BidCommandReaderConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

func NewBidCommandReader(cfg BidCommandReaderConfig) (*BidCommandReader, error) {
	brokers := normalizeBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka bid command reader requires at least one broker")
	}
	groupID := strings.TrimSpace(cfg.GroupID)
	if groupID == "" {
		groupID = "aieas-bid-decision-workers"
	}
	topic := defaultString(cfg.Topic, "aieas.bid.commands")
	return &BidCommandReader{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10 << 20,
			MaxWait:  200 * time.Millisecond,
		}),
		partitions: make(map[int]int64),
	}, nil
}

// FetchBidCommand 拉取下一条命令，返回 commit 回调用于消费成功后提交位点。
// 解码失败时仍返回 commit（跳过坏消息，避免卡住 partition）。
func (r *BidCommandReader) FetchBidCommand(ctx context.Context) (BidCommand, func(context.Context) error, error) {
	if r == nil || r.reader == nil {
		return BidCommand{}, nil, fmt.Errorf("kafka bid command reader is not configured")
	}
	msg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return BidCommand{}, nil, err
	}
	r.recordPartitionLag(msg)
	commit := func(commitCtx context.Context) error {
		return r.reader.CommitMessages(commitCtx, msg)
	}
	var cmd BidCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		return BidCommand{}, commit, fmt.Errorf("decode kafka bid command: %w", err)
	}
	cmd.TraceParent = headerValue(msg.Headers, "traceparent")
	cmd.TraceState = headerValue(msg.Headers, "tracestate")
	return cmd, commit, nil
}

// recordPartitionLag 在每次成功 fetch 后更新 reader 持有 partition 的最近 lag。
// HighWaterMark 是 broker 端 partition 当前最高 offset+1；Offset 是已读取该条
// 消息的位点。lag 反映该 partition 后面还有多少消息未消费。
func (r *BidCommandReader) recordPartitionLag(msg kafkago.Message) {
	if r == nil {
		return
	}
	lag := msg.HighWaterMark - msg.Offset - 1
	if lag < 0 {
		lag = 0
	}
	r.lagMu.Lock()
	if r.partitions == nil {
		r.partitions = make(map[int]int64)
	}
	r.partitions[msg.Partition] = lag
	r.lagMu.Unlock()
}

func (r *BidCommandReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

// PartitionLag 暴露 reader 当前已观测到的所有 partition 的最近 lag。
// 多 reader 实例（同一 group 不同进程）下，每个实例只会看到自己被分配的
// partition，分别上报；同一 partition 在多个实例间不会出现重复 series（同
// 一 group 同一 partition 同一时刻只会被一个实例消费）。
//
// 返回值是 map[partition]lag 的副本，调用方可安全迭代。当 reader 还没拉到
// 任何消息时返回空 map（非 nil）。
func (r *BidCommandReader) PartitionLag() map[int]int64 {
	if r == nil {
		return map[int]int64{}
	}
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	out := make(map[int]int64, len(r.partitions))
	for p, lag := range r.partitions {
		out[p] = lag
	}
	return out
}

func decodeBidEventMessage(msg kafkago.Message) (redisinfra.BidEvent, error) {
	var payload struct {
		RequestID      string               `json:"requestId"`
		AuctionID      uint64               `json:"auctionId"`
		LiveSessionID  uint64               `json:"liveSessionId"`
		BidderID       string               `json:"bidderId"`
		BidderNickname string               `json:"bidderNickname"`
		Price          int64                `json:"price"`
		Accepted       bool                 `json:"accepted"`
		Reason         string               `json:"reason"`
		CurrentPrice   int64                `json:"currentPrice"`
		LeaderBidderID string               `json:"leaderBidderId"`
		EndTSMS        int64                `json:"endTsMs"`
		Extended       bool                 `json:"extended"`
		ExtendCount    int                  `json:"extendCount"`
		Seq            int64                `json:"seq"`
		StreamID       string               `json:"streamId"`
		CreatedAtMS    int64                `json:"createdAtMs"`
		BidTSMS        int64                `json:"bidTsMs"`
		Source         string               `json:"source"`
		Event          string               `json:"event"`
		EventType      string               `json:"eventType"`
		RiskResult     domain.BidRiskResult `json:"riskResult"`
		AuctionStatus  domain.AuctionStatus `json:"auctionStatus"`
		AutoClosed     bool                 `json:"autoClosed"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return redisinfra.BidEvent{}, fmt.Errorf("decode kafka bid event: %w", err)
	}
	eventType := firstNonEmpty(payload.EventType, payload.Event, headerValue(msg.Headers, "event_type"))
	if eventType == "" {
		return redisinfra.BidEvent{}, fmt.Errorf("decode kafka bid event: missing event type")
	}
	createdAtMS := payload.CreatedAtMS
	if createdAtMS == 0 {
		if !msg.Time.IsZero() {
			createdAtMS = msg.Time.UTC().UnixMilli()
		} else {
			createdAtMS = time.Now().UTC().UnixMilli()
		}
	}
	streamID := payload.StreamID
	if streamID == "" && payload.Seq > 0 {
		streamID = strconv.FormatInt(payload.Seq, 10) + "-0"
	}
	return redisinfra.BidEvent{
		AuctionID:      payload.AuctionID,
		LiveSessionID:  payload.LiveSessionID,
		StreamID:       streamID,
		Seq:            payload.Seq,
		RequestID:      payload.RequestID,
		BidderID:       payload.BidderID,
		BidderNickname: payload.BidderNickname,
		BidPrice:       payload.Price,
		BidTSMS:        payload.BidTSMS,
		Source:         payload.Source,
		RiskResult:     payload.RiskResult,
		RejectReason:   payload.Reason,
		Accepted:       payload.Accepted,
		CurrentPrice:   payload.CurrentPrice,
		LeaderBidderID: payload.LeaderBidderID,
		EndTSMS:        payload.EndTSMS,
		Extended:       payload.Extended,
		ExtendCount:    payload.ExtendCount,
		CreatedAtMS:    createdAtMS,
		EventType:      eventType,
		AuctionStatus:  payload.AuctionStatus,
		AutoClosed:     payload.AutoClosed,
		TraceParent:    headerValue(msg.Headers, "traceparent"),
		TraceState:     headerValue(msg.Headers, "tracestate"),
	}, nil
}

func kafkaHeaders(values map[string]string) []kafkago.Header {
	headers := make([]kafkago.Header, 0, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		headers = append(headers, kafkago.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func normalizeBrokers(in []string) []string {
	out := make([]string, 0, len(in))
	for _, broker := range in {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
