package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	BaseURL            string
	AuctionID          uint64
	LiveRoomID         uint64
	Buyers             int
	Duration           time.Duration
	QPS                float64
	AccountFormat      string
	AccountStart       int
	Password           string
	TokenFile          string
	Enroll             bool
	EnrollConcurrency  int
	BidStep            int64
	Expect             string
	StatePollInterval  time.Duration
	ConnectTimeout     time.Duration
	HTTPTimeout        time.Duration
	PrintInterval      time.Duration
	InsecureSkipVerify bool
}

type apiResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

type loginResult struct {
	AccessToken string `json:"accessToken"`
	User        struct {
		ID string `json:"id"`
	} `json:"user"`
}

type buyer struct {
	Index   int
	Account string
	UserID  string
	Token   string
}

type auctionState struct {
	AuctionID      uint64 `json:"auctionId"`
	Status         string `json:"status"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId"`
	Version        int64  `json:"version"`
	Seq            int64  `json:"seq"`
	Source         string `json:"source"`
}

type envelope struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId,omitempty"`
	Seq       int64           `json:"seq,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// bidResult 兼容同步 bid.ack / 房间广播 bid.accepted | bid.rejected payload。
type bidResult struct {
	RequestID      string `json:"requestId"`
	AuctionID      uint64 `json:"auctionId"`
	BidderID       string `json:"bidderId"`
	Price          int64  `json:"price"`
	Accepted       bool   `json:"accepted"`
	Duplicate      bool   `json:"duplicate"`
	Reason         string `json:"reason"`
	Code           string `json:"code"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId"`
	Version        int64  `json:"version"`
	Seq            int64  `json:"seq"`
	Event          string `json:"event"`
	AuctionStatus  string `json:"auctionStatus"`
}

// bidAckPayload 解析 S→C bid.ack 的 payload（同步形态 + 异步 ASYNC 形态）。
//   - 同步形态：mode 缺省，accepted/code/reason/currentPrice/... 等同 BidResult 字段。
//   - 异步形态：mode="ASYNC"，status="QUEUED"|"REJECTED"，bidId 必填。
type bidAckPayload struct {
	Mode           string `json:"mode"`
	Status         string `json:"status"`
	BidID          string `json:"bidId"`
	AuctionID      uint64 `json:"auctionId"`
	RequestID      string `json:"requestId"`
	Reason         string `json:"reason"`
	Code           string `json:"code"`
	Accepted       *bool  `json:"accepted"`
	Duplicate      bool   `json:"duplicate"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId"`
	Version        int64  `json:"version"`
	Seq            int64  `json:"seq"`
}

// bidResultPayload 解析异步终态帧 S→C bid.result 的 payload。
type bidResultPayload struct {
	BidID          string `json:"bidId"`
	AuctionID      uint64 `json:"auctionId"`
	FinalStatus    string `json:"finalStatus"`
	Reason         string `json:"reason"`
	Code           string `json:"code"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId"`
	EndTimeMs      int64  `json:"endTimeMs"`
	ServerTimeMs   int64  `json:"serverTimeMs"`
	ResultSeq      int64  `json:"resultSeq"`
}

type sharedAuctionState struct {
	mu           sync.RWMutex
	currentPrice int64
	version      int64
	seq          int64
}

func (s *sharedAuctionState) snapshot() (int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentPrice, s.version
}

func (s *sharedAuctionState) update(currentPrice, version, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if currentPrice > s.currentPrice || version > s.version || seq > s.seq {
		if currentPrice > 0 {
			s.currentPrice = currentPrice
		}
		if version > s.version {
			s.version = version
		}
		if seq > s.seq {
			s.seq = seq
		}
	}
}

type counters struct {
	loginOK           atomic.Int64
	loginErr          atomic.Int64
	enrollOK          atomic.Int64
	enrollErr         atomic.Int64
	connectOK         atomic.Int64
	connectErr        atomic.Int64
	sent              atomic.Int64
	writeErr          atomic.Int64
	ack               atomic.Int64
	syncAcked         atomic.Int64 // 收到同步 bid.ack（无 mode）的次数
	asyncQueued       atomic.Int64 // 收到异步 bid.ack mode=ASYNC status=QUEUED 的次数
	asyncResulted     atomic.Int64 // 收到异步 bid.result 终态帧的次数（去重前）
	accepted          atomic.Int64 // 终态 ACCEPTED 总数（同步+异步）
	rejected          atomic.Int64 // 终态 REJECTED 总数（同步+异步，含队列保护）
	queueRejected     atomic.Int64 // 异步 bid.ack REJECTED（队列保护拒因）数量
	duplicate         atomic.Int64 // BidResult.duplicate=true 数量
	resultDuplicates  atomic.Int64 // 同 bidId 多次收到 bid.result 的重复次数
	resultAckSent     atomic.Int64 // 已回发 bid.result.ack 的次数
	resultAckErr      atomic.Int64 // 回发 bid.result.ack 失败次数
	readErr           atomic.Int64
	broadcastAccepted atomic.Int64
	broadcastRejected atomic.Int64
}

type reasonCounters struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newReasonCounters() *reasonCounters {
	return &reasonCounters{counts: make(map[string]int64)}
}

func (r *reasonCounters) inc(reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "UNKNOWN"
	}
	r.mu.Lock()
	r.counts[reason]++
	r.mu.Unlock()
}

func (r *reasonCounters) snapshot() []reasonCount {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]reasonCount, 0, len(r.counts))
	for reason, count := range r.counts {
		out = append(out, reasonCount{Reason: reason, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Reason < out[j].Reason
		}
		return out[i].Count > out[j].Count
	})
	return out
}

type reasonCount struct {
	Reason string
	Count  int64
}

type latencyRecorder struct {
	mu     sync.Mutex
	values []int64
}

func (l *latencyRecorder) add(d time.Duration) {
	l.mu.Lock()
	l.values = append(l.values, d.Microseconds())
	l.mu.Unlock()
}

func (l *latencyRecorder) summary() latencySummary {
	l.mu.Lock()
	values := append([]int64(nil), l.values...)
	l.mu.Unlock()
	if len(values) == 0 {
		return latencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return latencySummary{
		Count: len(values),
		P50:   percentile(values, 0.50),
		P95:   percentile(values, 0.95),
		P99:   percentile(values, 0.99),
		Max:   values[len(values)-1],
	}
}

type latencySummary struct {
	Count int
	P50   int64
	P95   int64
	P99   int64
	Max   int64
}

// inflightStage 标记某个 bidId 当前在压测端的处理阶段。
type inflightStage int

const (
	stagePending inflightStage = iota // 已发送，未收到任何 bid.ack
	stageQueued                       // 已收到异步 bid.ack mode=ASYNC status=QUEUED，等待 bid.result
)

type inflightEntry struct {
	sentAt time.Time
	stage  inflightStage
}

// sentTracker 记录在飞 bid 的发送时间与异步阶段，并对终态做幂等去重（resulted 集合）。
type sentTracker struct {
	mu       sync.Mutex
	inflight map[string]*inflightEntry
	resulted map[string]time.Time // 已经计入终态统计的 bidId（用于去重 + 过期清理）
}

func newSentTracker() *sentTracker {
	return &sentTracker{
		inflight: make(map[string]*inflightEntry),
		resulted: make(map[string]time.Time),
	}
}

func (s *sentTracker) put(requestID string, t time.Time) {
	s.mu.Lock()
	s.inflight[requestID] = &inflightEntry{sentAt: t, stage: stagePending}
	s.mu.Unlock()
}

// take 返回 bidId 的发送时间并将其从 inflight 表里移除（终态使用）。
func (s *sentTracker) take(requestID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.inflight[requestID]
	if !ok {
		return time.Time{}, false
	}
	delete(s.inflight, requestID)
	return entry.sentAt, true
}

// peek 仅查看发送时间，不移除（异步 QUEUED 中间态使用）。
func (s *sentTracker) peek(requestID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.inflight[requestID]
	if !ok {
		return time.Time{}, false
	}
	return entry.sentAt, true
}

// markQueued 将 bidId 标记为已入队，仍保留发送时间。
func (s *sentTracker) markQueued(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.inflight[requestID]; ok {
		entry.stage = stageQueued
	}
}

// finalize 尝试把 bidId 设为终态：
//   - 返回 sentAt：若该 bidId 仍在 inflight 中则给出，并从表里删除。
//   - 返回 firstFinal：true 表示这是首个终态（应计入统计）；false 表示重复终态（应忽略统计但仍要回 ack）。
func (s *sentTracker) finalize(requestID string, now time.Time) (sentAt time.Time, hasSent bool, firstFinal bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.resulted[requestID]; dup {
		// 已经终态过：刷新过期时间避免提前清理，但不计入统计。
		s.resulted[requestID] = now
		if entry, ok := s.inflight[requestID]; ok {
			delete(s.inflight, requestID)
			return entry.sentAt, true, false
		}
		return time.Time{}, false, false
	}
	s.resulted[requestID] = now
	if entry, ok := s.inflight[requestID]; ok {
		delete(s.inflight, requestID)
		return entry.sentAt, true, true
	}
	return time.Time{}, false, true
}

// gc 清理超过 ttl 的 inflight 与 resulted 条目，避免长时间运行内存泄漏。
func (s *sentTracker) gc(now time.Time, inflightTTL, resultedTTL time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, entry := range s.inflight {
		if now.Sub(entry.sentAt) > inflightTTL {
			delete(s.inflight, k)
		}
	}
	for k, ts := range s.resulted {
		if now.Sub(ts) > resultedTTL {
			delete(s.resulted, k)
		}
	}
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid args: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: cfg.HTTPTimeout}
	if cfg.InsecureSkipVerify {
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // Load-test flag for local/staging self-signed certs.
	}

	stats := &counters{}
	reasons := newReasonCounters()
	connectReasons := newReasonCounters()
	queueReasons := newReasonCounters()
	syncLatencies := &latencyRecorder{}
	asyncLatencies := &latencyRecorder{}
	tracker := newSentTracker()
	sharedState := &sharedAuctionState{}

	buyers, err := prepareBuyers(ctx, cfg, client, stats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "prepare buyers: %v\n", err)
		os.Exit(1)
	}
	if len(buyers) == 0 {
		fmt.Fprintln(os.Stderr, "prepare buyers: no buyers available")
		os.Exit(1)
	}

	firstState, err := fetchAuctionState(ctx, client, cfg.BaseURL, cfg.AuctionID, buyers[0].Token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch auction state: %v\n", err)
		os.Exit(1)
	}
	sharedState.update(firstState.CurrentPrice, firstState.Version, firstState.Seq)
	fmt.Printf("initial state: auction=%d status=%s currentPrice=%d version=%d source=%s\n", firstState.AuctionID, firstState.Status, firstState.CurrentPrice, firstState.Version, firstState.Source)

	if cfg.Enroll {
		if err := enrollBuyers(ctx, cfg, client, buyers, stats); err != nil {
			fmt.Fprintf(os.Stderr, "enroll buyers: %v\n", err)
			os.Exit(1)
		}
	}

	runCtx, cancelRun := context.WithTimeout(ctx, cfg.Duration)
	defer cancelRun()

	if cfg.StatePollInterval > 0 {
		go pollAuctionState(runCtx, client, cfg, buyers[0].Token, sharedState)
	}

	jobs := make([]chan struct{}, len(buyers))
	var wg sync.WaitGroup
	for i := range buyers {
		jobs[i] = make(chan struct{}, 1024)
		wg.Add(1)
		go func(b buyer, ch <-chan struct{}) {
			defer wg.Done()
			runBuyer(runCtx, cfg, b, ch, sharedState, stats, connectReasons, reasons, queueReasons, syncLatencies, asyncLatencies, tracker)
		}(buyers[i], jobs[i])
	}

	start := time.Now()
	go printProgress(runCtx, start, cfg.PrintInterval, stats, syncLatencies, asyncLatencies)
	go gcInflight(runCtx, tracker)

	scheduleBids(runCtx, cfg, jobs)
	for _, ch := range jobs {
		close(ch)
	}
	wg.Wait()

	printFinal(time.Since(start), cfg, stats, connectReasons, reasons, queueReasons, syncLatencies.summary(), asyncLatencies.summary())
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.BaseURL, "base-url", "http://127.0.0.1:8080", "server base URL, for example http://127.0.0.1:8080")
	flag.Uint64Var(&cfg.AuctionID, "auction-id", 0, "auction ID to bid on")
	flag.Uint64Var(&cfg.LiveRoomID, "live-room-id", 0, "optional live room ID; when set, WebSocket connects through /ws/live-rooms/:id")
	flag.IntVar(&cfg.Buyers, "buyers", 1, "number of buyer clients")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "load duration")
	flag.Float64Var(&cfg.QPS, "qps", 10, "total bid.place send rate")
	flag.StringVar(&cfg.AccountFormat, "account-format", "buyer%03d", "fmt.Sprintf-compatible account format used when token-file is empty")
	flag.IntVar(&cfg.AccountStart, "account-start", 1, "first account number for account-format")
	flag.StringVar(&cfg.Password, "password", "Passw0rd!", "buyer password for login mode")
	flag.StringVar(&cfg.TokenFile, "token-file", "", "optional token file; supports one access token per line or a JSON array")
	flag.BoolVar(&cfg.Enroll, "enroll", true, "enroll buyers before the timed WebSocket bid load")
	flag.IntVar(&cfg.EnrollConcurrency, "enroll-concurrency", 16, "max concurrent enroll/login HTTP requests")
	flag.Int64Var(&cfg.BidStep, "bid-step", 100, "price increment in cents for generated bids")
	flag.StringVar(&cfg.Expect, "expect", "price", "stale-state guard: price or version")
	flag.DurationVar(&cfg.StatePollInterval, "state-poll-interval", 2*time.Second, "HTTP state polling interval; set 0 to disable")
	flag.DurationVar(&cfg.ConnectTimeout, "connect-timeout", 5*time.Second, "WebSocket connect timeout")
	flag.DurationVar(&cfg.HTTPTimeout, "http-timeout", 5*time.Second, "HTTP client timeout")
	flag.DurationVar(&cfg.PrintInterval, "print-interval", 5*time.Second, "progress print interval")
	flag.BoolVar(&cfg.InsecureSkipVerify, "insecure-skip-verify", false, "skip TLS certificate verification for local/staging HTTPS/WSS")
	flag.Parse()
	return cfg
}

func (c config) validate() error {
	if c.AuctionID == 0 {
		return errors.New("-auction-id is required")
	}
	if c.Buyers <= 0 {
		return errors.New("-buyers must be positive")
	}
	if c.Duration <= 0 {
		return errors.New("-duration must be positive")
	}
	if c.QPS <= 0 {
		return errors.New("-qps must be positive")
	}
	if c.BidStep <= 0 {
		return errors.New("-bid-step must be positive")
	}
	if c.EnrollConcurrency <= 0 {
		return errors.New("-enroll-concurrency must be positive")
	}
	if c.ConnectTimeout <= 0 || c.HTTPTimeout <= 0 {
		return errors.New("timeouts must be positive")
	}
	if c.PrintInterval <= 0 {
		return errors.New("-print-interval must be positive")
	}
	switch c.Expect {
	case "version", "price":
	default:
		return errors.New("-expect must be version or price")
	}
	parsed, err := url.Parse(c.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("-base-url must include scheme and host")
	}
	return nil
}

func prepareBuyers(ctx context.Context, cfg config, client *http.Client, stats *counters) ([]buyer, error) {
	if cfg.TokenFile != "" {
		tokens, err := loadTokens(cfg.TokenFile)
		if err != nil {
			return nil, err
		}
		if len(tokens) < cfg.Buyers {
			return nil, fmt.Errorf("token file has %d tokens, need %d buyers", len(tokens), cfg.Buyers)
		}
		buyers := make([]buyer, cfg.Buyers)
		for i := range buyers {
			buyers[i] = buyer{Index: i, Token: tokens[i]}
			stats.loginOK.Add(1)
		}
		return buyers, nil
	}
	return loginBuyers(ctx, cfg, client, stats)
}

func loginBuyers(ctx context.Context, cfg config, client *http.Client, stats *counters) ([]buyer, error) {
	buyers := make([]buyer, cfg.Buyers)
	errCh := make(chan error, cfg.Buyers)
	sem := make(chan struct{}, cfg.EnrollConcurrency)
	var wg sync.WaitGroup
	for i := range buyers {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			account := fmt.Sprintf(cfg.AccountFormat, cfg.AccountStart+i)
			result, err := login(ctx, client, cfg.BaseURL, account, cfg.Password)
			if err != nil {
				stats.loginErr.Add(1)
				errCh <- fmt.Errorf("login %s: %w", account, err)
				return
			}
			stats.loginOK.Add(1)
			buyers[i] = buyer{Index: i, Account: account, UserID: result.User.ID, Token: result.AccessToken}
		}()
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, joinFirstErrors(errs, 5)
	}
	return buyers, nil
}

func login(ctx context.Context, client *http.Client, baseURL, account, password string) (loginResult, error) {
	body := map[string]string{
		"account":  account,
		"password": password,
		"role":     "buyer",
	}
	var out loginResult
	err := doJSON(ctx, client, http.MethodPost, apiURL(baseURL, "/api/v1/auth/login"), "", body, &out, "")
	return out, err
}

func enrollBuyers(ctx context.Context, cfg config, client *http.Client, buyers []buyer, stats *counters) error {
	errCh := make(chan error, len(buyers))
	sem := make(chan struct{}, cfg.EnrollConcurrency)
	var wg sync.WaitGroup
	for _, b := range buyers {
		b := b
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			idempotencyKey := fmt.Sprintf("bidload-enroll-%d-%d-%s", cfg.AuctionID, b.Index, shortTokenHash(b.Token))
			body := map[string]string{"depositPayChannel": "MOCK_PAY"}
			err := doJSON(ctx, client, http.MethodPost, apiURL(cfg.BaseURL, fmt.Sprintf("/api/v1/auctions/%d/enroll", cfg.AuctionID)), b.Token, body, nil, idempotencyKey)
			if err != nil {
				stats.enrollErr.Add(1)
				errCh <- fmt.Errorf("enroll buyer[%d] %s: %w", b.Index, b.Account, err)
				return
			}
			stats.enrollOK.Add(1)
		}()
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return joinFirstErrors(errs, 5)
	}
	return nil
}

func fetchAuctionState(ctx context.Context, client *http.Client, baseURL string, auctionID uint64, token string) (auctionState, error) {
	var state auctionState
	err := doJSON(ctx, client, http.MethodGet, apiURL(baseURL, fmt.Sprintf("/api/v1/auctions/%d/state", auctionID)), token, nil, &state, "")
	return state, err
}

func pollAuctionState(ctx context.Context, client *http.Client, cfg config, token string, sharedState *sharedAuctionState) {
	ticker := time.NewTicker(cfg.StatePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state, err := fetchAuctionState(ctx, client, cfg.BaseURL, cfg.AuctionID, token)
			if err == nil {
				sharedState.update(state.CurrentPrice, state.Version, state.Seq)
			}
		}
	}
}

func runBuyer(ctx context.Context, cfg config, b buyer, jobs <-chan struct{}, sharedState *sharedAuctionState, stats *counters, connectReasons *reasonCounters, bidReasons *reasonCounters, queueReasons *reasonCounters, syncLatencies *latencyRecorder, asyncLatencies *latencyRecorder, tracker *sentTracker) {
	conn, err := dialWebSocket(ctx, cfg, b.Token)
	if err != nil {
		stats.connectErr.Add(1)
		connectReasons.inc(classifyConnectError(err))
		return
	}
	stats.connectOK.Add(1)
	defer conn.Close()

	done := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(done)
		readLoop(ctx, conn, &writeMu, sharedState, stats, bidReasons, queueReasons, syncLatencies, asyncLatencies, tracker)
	}()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case _, ok := <-jobs:
			if !ok {
				return
			}
			seq++
			if err := sendBid(conn, &writeMu, cfg, b, seq, sharedState, tracker); err != nil {
				stats.writeErr.Add(1)
				return
			}
			stats.sent.Add(1)
		}
	}
}

func dialWebSocket(ctx context.Context, cfg config, token string) (*websocket.Conn, error) {
	wsURL, err := websocketURL(cfg, token)
	if err != nil {
		return nil, err
	}
	netDialer := &net.Dialer{Timeout: cfg.ConnectTimeout}
	dialer := websocket.Dialer{
		HandshakeTimeout: cfg.ConnectTimeout,
		NetDialContext:   netDialer.DialContext,
	}
	if cfg.InsecureSkipVerify {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Load-test flag for local/staging self-signed certs.
	}
	header := http.Header{}
	header.Set("Origin", websocketOrigin(cfg.BaseURL))
	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, websocketDialError(resp, err)
	}
	return conn, nil
}

func websocketDialError(resp *http.Response, err error) error {
	if resp == nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	body := strings.TrimSpace(string(raw))
	if body == "" {
		return fmt.Errorf("websocket handshake http %d: %w", resp.StatusCode, err)
	}
	return fmt.Errorf("websocket handshake http %d: %s: %w", resp.StatusCode, body, err)
}

func classifyConnectError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if strings.Contains(text, "http 401") {
		return "http_401"
	}
	if strings.Contains(text, "http 403") {
		return "http_403"
	}
	if strings.Contains(text, "http 404") {
		return "http_404"
	}
	if strings.Contains(text, "http 409") {
		return "http_409"
	}
	if strings.Contains(text, "http 429") {
		return "http_429"
	}
	if strings.Contains(text, "http 503") {
		return "http_503"
	}
	if strings.Contains(text, "timeout") || strings.Contains(text, "deadline") {
		return "timeout"
	}
	if strings.Contains(text, "connection refused") {
		return "connection_refused"
	}
	return "connect_error"
}

func readLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, sharedState *sharedAuctionState, stats *counters, reasons *reasonCounters, queueReasons *reasonCounters, syncLatencies *latencyRecorder, asyncLatencies *latencyRecorder, tracker *sentTracker) {
	for {
		var env envelope
		if err := conn.ReadJSON(&env); err != nil {
			if ctx.Err() == nil {
				stats.readErr.Add(1)
			}
			return
		}
		switch env.Type {
		case "room.snapshot":
			var state auctionState
			if json.Unmarshal(env.Payload, &state) == nil {
				sharedState.update(state.CurrentPrice, state.Version, env.Seq)
			}
		case "bid.ack":
			handleBidAck(env, stats, reasons, queueReasons, syncLatencies, tracker, sharedState)
		case "bid.result":
			handleBidResult(conn, writeMu, env, stats, reasons, asyncLatencies, tracker, sharedState)
		case "bid.accepted":
			stats.broadcastAccepted.Add(1)
			var result bidResult
			if json.Unmarshal(env.Payload, &result) == nil {
				sharedState.update(result.CurrentPrice, result.Version, env.Seq)
			}
		case "bid.rejected":
			stats.broadcastRejected.Add(1)
			var result bidResult
			if json.Unmarshal(env.Payload, &result) == nil {
				sharedState.update(result.CurrentPrice, result.Version, env.Seq)
			}
		default:
			// 未知/无关帧（ranking.updated / auction.state / presence.snapshot /
			// live.voice_broadcast / ai.assistant.switch / heartbeat 等）一律忽略，
			// 不能让它们打断压测或被错误归类。
		}
	}
}

// handleBidAck 同时处理同步和异步形态的 bid.ack。
func handleBidAck(env envelope, stats *counters, reasons *reasonCounters, queueReasons *reasonCounters, syncLatencies *latencyRecorder, tracker *sentTracker, sharedState *sharedAuctionState) {
	stats.ack.Add(1)
	if len(env.Payload) == 0 {
		return
	}
	var ack bidAckPayload
	if err := json.Unmarshal(env.Payload, &ack); err != nil {
		return
	}
	bidID := ack.BidID
	if bidID == "" {
		bidID = ack.RequestID
	}
	if bidID == "" {
		bidID = env.RequestID
	}

	if strings.EqualFold(ack.Mode, "ASYNC") {
		// 异步形态：QUEUED 表示已入队待裁决；REJECTED 是终态失败（含队列保护）。
		switch strings.ToUpper(ack.Status) {
		case "QUEUED":
			stats.asyncQueued.Add(1)
			if bidID != "" {
				tracker.markQueued(bidID)
			}
		case "REJECTED":
			stats.queueRejected.Add(1)
			stats.rejected.Add(1)
			reasonStr := ack.Code
			if reasonStr == "" {
				reasonStr = ack.Reason
			}
			reasons.inc(reasonStr)
			queueReasons.inc(reasonStr)
			if bidID != "" {
				if sentAt, hasSent, firstFinal := tracker.finalize(bidID, time.Now()); hasSent && firstFinal {
					syncLatencies.add(time.Since(sentAt))
				}
			}
		default:
			// 未知 status 不当作终态。
		}
		return
	}

	// 同步形态：直接按 accepted 定终态。
	stats.syncAcked.Add(1)
	var result bidResult
	if err := json.Unmarshal(env.Payload, &result); err != nil {
		return
	}
	target := bidID
	if target == "" {
		target = result.RequestID
	}
	if target == "" {
		target = env.RequestID
	}
	now := time.Now()
	if target != "" {
		if sentAt, hasSent, firstFinal := tracker.finalize(target, now); hasSent && firstFinal {
			syncLatencies.add(now.Sub(sentAt))
		} else if !firstFinal {
			// 重复同步终态，不计入。
			return
		}
	}
	if result.Accepted {
		stats.accepted.Add(1)
		sharedState.update(result.CurrentPrice, result.Version, result.Seq)
	} else {
		stats.rejected.Add(1)
		reasonStr := result.Code
		if reasonStr == "" {
			reasonStr = result.Reason
		}
		reasons.inc(reasonStr)
		if result.CurrentPrice > 0 || result.Version > 0 {
			sharedState.update(result.CurrentPrice, result.Version, result.Seq)
		}
	}
	if result.Duplicate {
		stats.duplicate.Add(1)
	}
}

// handleBidResult 处理异步终态 bid.result 帧：去重统计 + 必须回 bid.result.ack。
func handleBidResult(conn *websocket.Conn, writeMu *sync.Mutex, env envelope, stats *counters, reasons *reasonCounters, asyncLatencies *latencyRecorder, tracker *sentTracker, sharedState *sharedAuctionState) {
	stats.asyncResulted.Add(1)
	if len(env.Payload) == 0 {
		return
	}
	var result bidResultPayload
	if err := json.Unmarshal(env.Payload, &result); err != nil {
		return
	}
	bidID := result.BidID
	if bidID == "" {
		bidID = env.RequestID
	}

	now := time.Now()
	firstFinal := true
	if bidID != "" {
		sentAt, hasSent, first := tracker.finalize(bidID, now)
		firstFinal = first
		if first && hasSent {
			asyncLatencies.add(now.Sub(sentAt))
		}
	}

	if firstFinal {
		switch strings.ToUpper(result.FinalStatus) {
		case "ACCEPTED":
			stats.accepted.Add(1)
			if result.CurrentPrice > 0 {
				sharedState.update(result.CurrentPrice, 0, result.ResultSeq)
			}
		case "REJECTED":
			stats.rejected.Add(1)
			reasonStr := result.Code
			if reasonStr == "" {
				reasonStr = result.Reason
			}
			reasons.inc(reasonStr)
		default:
			// 未知 finalStatus：保守不计 accepted/rejected，但仍回 ack。
		}
	} else {
		stats.resultDuplicates.Add(1)
	}

	// 不论是否首个终态都必须回 bid.result.ack，否则后端会重发。
	if bidID != "" {
		if err := sendResultAck(conn, writeMu, bidID); err != nil {
			stats.resultAckErr.Add(1)
		} else {
			stats.resultAckSent.Add(1)
		}
	}
}

// sendResultAck 发送 C→S bid.result.ack {"type":"bid.result.ack","payload":{"bidId":...}}。
func sendResultAck(conn *websocket.Conn, writeMu *sync.Mutex, bidID string) error {
	payload, err := json.Marshal(map[string]string{"bidId": bidID})
	if err != nil {
		return err
	}
	env := envelope{
		Type:    "bid.result.ack",
		Payload: payload,
	}
	writeMu.Lock()
	err = conn.WriteJSON(env)
	writeMu.Unlock()
	return err
}

func sendBid(conn *websocket.Conn, writeMu *sync.Mutex, cfg config, b buyer, seq int64, sharedState *sharedAuctionState, tracker *sentTracker) error {
	currentPrice, version := sharedState.snapshot()
	price := currentPrice + cfg.BidStep
	requestID := fmt.Sprintf("bidload-%d-%d-%d", time.Now().UnixNano(), b.Index, seq)
	payload := map[string]interface{}{
		"price": price,
	}
	if cfg.LiveRoomID == 0 {
		payload["auctionId"] = cfg.AuctionID
	}
	if cfg.Expect == "version" {
		payload["expectedVersion"] = version
	} else {
		payload["expectedCurrentPrice"] = currentPrice
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	env := envelope{
		Type:      "bid.place",
		RequestID: requestID,
		Payload:   raw,
	}
	tracker.put(requestID, time.Now())
	writeMu.Lock()
	err = conn.WriteJSON(env)
	writeMu.Unlock()
	if err != nil {
		_, _ = tracker.take(requestID)
	}
	return err
}

func scheduleBids(ctx context.Context, cfg config, jobs []chan struct{}) {
	interval := time.Duration(float64(time.Second) / cfg.QPS)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx := rnd.Intn(len(jobs))
			select {
			case jobs[idx] <- struct{}{}:
			default:
			}
		}
	}
}

func printProgress(ctx context.Context, started time.Time, interval time.Duration, stats *counters, syncLatencies *latencyRecorder, asyncLatencies *latencyRecorder) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(started).Seconds()
			syncLat := syncLatencies.summary()
			asyncLat := asyncLatencies.summary()
			fmt.Printf(
				"elapsed=%.0fs sent=%d ack=%d syncAcked=%d asyncQueued=%d asyncResulted=%d accepted=%d rejected=%d queueRejected=%d qps=%.1f accepted/s=%.1f syncP95=%s asyncP95=%s writeErr=%d readErr=%d\n",
				elapsed,
				stats.sent.Load(),
				stats.ack.Load(),
				stats.syncAcked.Load(),
				stats.asyncQueued.Load(),
				stats.asyncResulted.Load(),
				stats.accepted.Load(),
				stats.rejected.Load(),
				stats.queueRejected.Load(),
				float64(stats.sent.Load())/maxFloat(elapsed, 1),
				float64(stats.accepted.Load())/maxFloat(elapsed, 1),
				formatMicros(syncLat.P95),
				formatMicros(asyncLat.P95),
				stats.writeErr.Load(),
				stats.readErr.Load(),
			)
		}
	}
}

// gcInflight 周期性清理过期的 inflight / resulted 条目，避免长时间运行内存膨胀。
func gcInflight(ctx context.Context, tracker *sentTracker) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// 在飞 5 分钟仍未拿到任何终态的视为遗失；
			// 已终态条目保留 5 分钟用于幂等去重，避免后端晚到的重发把统计搞错。
			tracker.gc(now, 5*time.Minute, 5*time.Minute)
		}
	}
}

func printFinal(elapsed time.Duration, cfg config, stats *counters, connectReasons *reasonCounters, bidReasons *reasonCounters, queueReasons *reasonCounters, syncLat latencySummary, asyncLat latencySummary) {
	seconds := elapsed.Seconds()
	fmt.Println()
	fmt.Println("bidload summary")
	if cfg.LiveRoomID > 0 {
		fmt.Printf("liveRoom=%d auction=%d buyers=%d duration=%s targetQPS=%.1f expect=%s\n", cfg.LiveRoomID, cfg.AuctionID, cfg.Buyers, cfg.Duration, cfg.QPS, cfg.Expect)
	} else {
		fmt.Printf("auction=%d buyers=%d duration=%s targetQPS=%.1f expect=%s\n", cfg.AuctionID, cfg.Buyers, cfg.Duration, cfg.QPS, cfg.Expect)
	}
	fmt.Printf("loginOK=%d loginErr=%d enrollOK=%d enrollErr=%d connectOK=%d connectErr=%d\n", stats.loginOK.Load(), stats.loginErr.Load(), stats.enrollOK.Load(), stats.enrollErr.Load(), stats.connectOK.Load(), stats.connectErr.Load())
	fmt.Printf("sent=%d ack=%d syncAcked=%d asyncQueued=%d asyncResulted=%d accepted=%d rejected=%d queueRejected=%d duplicate=%d resultDup=%d writeErr=%d readErr=%d\n",
		stats.sent.Load(), stats.ack.Load(),
		stats.syncAcked.Load(), stats.asyncQueued.Load(), stats.asyncResulted.Load(),
		stats.accepted.Load(), stats.rejected.Load(), stats.queueRejected.Load(),
		stats.duplicate.Load(), stats.resultDuplicates.Load(),
		stats.writeErr.Load(), stats.readErr.Load())
	fmt.Printf("resultAck sent=%d err=%d\n", stats.resultAckSent.Load(), stats.resultAckErr.Load())
	fmt.Printf("sendQPS=%.2f acceptedQPS=%.2f ackRate=%.2f%%\n", float64(stats.sent.Load())/maxFloat(seconds, 1), float64(stats.accepted.Load())/maxFloat(seconds, 1), ratioPercent(stats.ack.Load(), stats.sent.Load()))
	fmt.Printf("syncAckLatency  count=%d p50=%s p95=%s p99=%s max=%s\n", syncLat.Count, formatMicros(syncLat.P50), formatMicros(syncLat.P95), formatMicros(syncLat.P99), formatMicros(syncLat.Max))
	fmt.Printf("asyncResultLatency count=%d p50=%s p95=%s p99=%s max=%s\n", asyncLat.Count, formatMicros(asyncLat.P50), formatMicros(asyncLat.P95), formatMicros(asyncLat.P99), formatMicros(asyncLat.Max))
	fmt.Printf("broadcastFrames accepted=%d rejected=%d\n", stats.broadcastAccepted.Load(), stats.broadcastRejected.Load())
	if counts := connectReasons.snapshot(); len(counts) > 0 {
		fmt.Println("connect error reasons:")
		for _, item := range counts {
			fmt.Printf("  %s=%d\n", item.Reason, item.Count)
		}
	}
	if counts := bidReasons.snapshot(); len(counts) > 0 {
		fmt.Println("bid reject reasons:")
		for _, item := range counts {
			fmt.Printf("  %s=%d\n", item.Reason, item.Count)
		}
	}
	if counts := queueReasons.snapshot(); len(counts) > 0 {
		fmt.Println("async queue reject reasons:")
		for _, item := range counts {
			fmt.Printf("  %s=%d\n", item.Reason, item.Count)
		}
	}
}

func doJSON(ctx context.Context, client *http.Client, method, targetURL, token string, body interface{}, out interface{}, idempotencyKey string) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var apiResp apiResponse
	if err := json.Unmarshal(raw, &apiResp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if apiResp.Code != 0 {
		return fmt.Errorf("api code %d: %s", apiResp.Code, apiResp.Message)
	}
	if out != nil && len(apiResp.Data) > 0 && string(apiResp.Data) != "null" {
		if err := json.Unmarshal(apiResp.Data, out); err != nil {
			return fmt.Errorf("decode data: %w", err)
		}
	}
	return nil
}

func apiURL(baseURL, path string) string {
	base := strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}

func websocketURL(cfg config, token string) (string, error) {
	baseURL := cfg.BaseURL
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported URL scheme %q", parsed.Scheme)
	}
	if cfg.LiveRoomID > 0 {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + fmt.Sprintf("/ws/live-rooms/%d", cfg.LiveRoomID)
	} else {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + fmt.Sprintf("/ws/auctions/%d", cfg.AuctionID)
	}
	query := parsed.Query()
	query.Set("token", token)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func websocketOrigin(baseURL string) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "http://localhost"
	}
	switch parsed.Scheme {
	case "https", "wss":
		parsed.Scheme = "https"
	default:
		parsed.Scheme = "http"
	}
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func loadTokens(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("empty token file")
	}
	if trimmed[0] == '[' {
		tokens, err := parseJSONTokens(trimmed)
		if err != nil {
			return nil, err
		}
		return tokens, nil
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	var tokens []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tokens = append(tokens, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, errors.New("token file contains no tokens")
	}
	return tokens, nil
}

func parseJSONTokens(raw []byte) ([]string, error) {
	var asStrings []string
	if err := json.Unmarshal(raw, &asStrings); err == nil {
		return compactTokens(asStrings), nil
	}
	var asObjects []struct {
		AccessToken string `json:"accessToken"`
		Token       string `json:"token"`
	}
	if err := json.Unmarshal(raw, &asObjects); err != nil {
		return nil, err
	}
	tokens := make([]string, 0, len(asObjects))
	for _, item := range asObjects {
		token := strings.TrimSpace(item.AccessToken)
		if token == "" {
			token = strings.TrimSpace(item.Token)
		}
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	if len(tokens) == 0 {
		return nil, errors.New("json token file contains no tokens")
	}
	return tokens, nil
}

func compactTokens(tokens []string) []string {
	out := tokens[:0]
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token != "" {
			out = append(out, token)
		}
	}
	return out
}

func shortTokenHash(token string) string {
	sum := sha1.Sum([]byte(token))
	return hex.EncodeToString(sum[:])[:10]
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1)*p + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatMicros(us int64) string {
	if us <= 0 {
		return "0"
	}
	return (time.Duration(us) * time.Microsecond).String()
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func ratioPercent(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func joinFirstErrors(errs []error, limit int) error {
	if len(errs) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(errs) {
		limit = len(errs)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, errs[i].Error())
	}
	if len(errs) > limit {
		parts = append(parts, fmt.Sprintf("... and %d more", len(errs)-limit))
	}
	return errors.New(strings.Join(parts, "; "))
}
