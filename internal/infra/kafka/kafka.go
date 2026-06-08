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
	Brokers            []string
	ClientID           string
	BidEventsTopic     string
	BidCommandsTopic   string
	AuctionEventsTopic string
	OrderEventsTopic   string
}

type Producer struct {
	brokers            []string
	clientID           string
	bidEventsTopic     string
	bidCommandsTopic   string
	auctionEventsTopic string
	orderEventsTopic   string

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
	return &Producer{
		brokers:            brokers,
		clientID:           clientID,
		bidEventsTopic:     defaultString(cfg.BidEventsTopic, "aieas.bid.events"),
		bidCommandsTopic:   defaultString(cfg.BidCommandsTopic, "aieas.bid.commands"),
		auctionEventsTopic: defaultString(cfg.AuctionEventsTopic, "aieas.auction.events"),
		orderEventsTopic:   defaultString(cfg.OrderEventsTopic, "aieas.order.events"),
		writers:            make(map[string]*kafkago.Writer),
	}, nil
}

// BidCommand 是异步竞价裁决的命令消息：由 WS handler 在 preCheck 通过后投递，
// 由 BidDecisionWorker 顺序消费并复用 Lua 裁决。key 必须是 auctionId，
// 保证同一拍品的命令落在同一 partition 内、严格有序。
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

// PublishBidCommand 把竞价命令投递到 aieas.bid.commands。key=auctionId 以保证
// 同 auction 同 partition 顺序。Kafka disabled 时 Producer 为 nil，直接返回 nil。
func (p *Producer) PublishBidCommand(ctx context.Context, cmd BidCommand) error {
	if p == nil {
		return nil
	}
	if cmd.EnqueuedAtMS == 0 {
		cmd.EnqueuedAtMS = time.Now().UTC().UnixMilli()
	}
	value, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal bid command: %w", err)
	}
	headers := map[string]string{"command_type": "bid.place"}
	tracing.InjectMap(ctx, headers)
	return p.publish(ctx, p.bidCommandsTopic, strconv.FormatUint(cmd.AuctionID, 10), value, headers)
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
		Addr:         kafkago.TCP(p.brokers...),
		Topic:        topic,
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

// BidCommandReader 顺序消费 aieas.bid.commands。GroupID 内按 partition 顺序消费，
// 单 partition 单 goroutine（由调用方保证不在 partition 内并发）。
type BidCommandReader struct {
	reader *kafkago.Reader
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
	return &BidCommandReader{reader: kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10 << 20,
		MaxWait:  200 * time.Millisecond,
	})}, nil
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

func (r *BidCommandReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
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
