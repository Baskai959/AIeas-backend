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

	"golang.org/x/net/websocket"
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

type bidResult struct {
	RequestID      string `json:"requestId"`
	AuctionID      uint64 `json:"auctionId"`
	BidderID       string `json:"bidderId"`
	Price          int64  `json:"price"`
	Accepted       bool   `json:"accepted"`
	Duplicate      bool   `json:"duplicate"`
	Reason         string `json:"reason"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId"`
	Version        int64  `json:"version"`
	Seq            int64  `json:"seq"`
	Event          string `json:"event"`
	AuctionStatus  string `json:"auctionStatus"`
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
	accepted          atomic.Int64
	rejected          atomic.Int64
	duplicate         atomic.Int64
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

type sentTracker struct {
	mu sync.Mutex
	ts map[string]time.Time
}

func newSentTracker() *sentTracker {
	return &sentTracker{ts: make(map[string]time.Time)}
}

func (s *sentTracker) put(requestID string, t time.Time) {
	s.mu.Lock()
	s.ts[requestID] = t
	s.mu.Unlock()
}

func (s *sentTracker) take(requestID string) (time.Time, bool) {
	s.mu.Lock()
	t, ok := s.ts[requestID]
	if ok {
		delete(s.ts, requestID)
	}
	s.mu.Unlock()
	return t, ok
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
	latencies := &latencyRecorder{}
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
			runBuyer(runCtx, cfg, b, ch, sharedState, stats, reasons, latencies, tracker)
		}(buyers[i], jobs[i])
	}

	start := time.Now()
	go printProgress(runCtx, start, cfg.PrintInterval, stats, latencies)

	scheduleBids(runCtx, cfg, jobs)
	for _, ch := range jobs {
		close(ch)
	}
	wg.Wait()

	printFinal(time.Since(start), cfg, stats, reasons, latencies.summary())
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

func runBuyer(ctx context.Context, cfg config, b buyer, jobs <-chan struct{}, sharedState *sharedAuctionState, stats *counters, reasons *reasonCounters, latencies *latencyRecorder, tracker *sentTracker) {
	conn, err := dialWebSocket(ctx, cfg, b.Token)
	if err != nil {
		stats.connectErr.Add(1)
		return
	}
	stats.connectOK.Add(1)
	defer conn.Close()

	done := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(done)
		readLoop(ctx, conn, sharedState, stats, reasons, latencies, tracker)
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
	origin := websocketOrigin(cfg.BaseURL)
	wsConfig, err := websocket.NewConfig(wsURL, origin)
	if err != nil {
		return nil, err
	}
	wsConfig.Dialer = &net.Dialer{Timeout: cfg.ConnectTimeout}
	if cfg.InsecureSkipVerify {
		wsConfig.TlsConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Load-test flag for local/staging self-signed certs.
	}
	return wsConfig.DialContext(ctx)
}

func readLoop(ctx context.Context, conn *websocket.Conn, sharedState *sharedAuctionState, stats *counters, reasons *reasonCounters, latencies *latencyRecorder, tracker *sentTracker) {
	for {
		var env envelope
		if err := websocket.JSON.Receive(conn, &env); err != nil {
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
			stats.ack.Add(1)
			var result bidResult
			if json.Unmarshal(env.Payload, &result) == nil {
				if sentAt, ok := tracker.take(env.RequestID); ok {
					latencies.add(time.Since(sentAt))
				}
				if result.Accepted {
					stats.accepted.Add(1)
					sharedState.update(result.CurrentPrice, result.Version, result.Seq)
				} else {
					stats.rejected.Add(1)
					reasons.inc(result.Reason)
					if result.CurrentPrice > 0 || result.Version > 0 {
						sharedState.update(result.CurrentPrice, result.Version, result.Seq)
					}
				}
				if result.Duplicate {
					stats.duplicate.Add(1)
				}
			}
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
		}
	}
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
	err = websocket.JSON.Send(conn, env)
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

func printProgress(ctx context.Context, started time.Time, interval time.Duration, stats *counters, latencies *latencyRecorder) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(started).Seconds()
			lat := latencies.summary()
			fmt.Printf(
				"elapsed=%.0fs sent=%d ack=%d accepted=%d rejected=%d qps=%.1f accepted/s=%.1f p95=%s writeErr=%d readErr=%d\n",
				elapsed,
				stats.sent.Load(),
				stats.ack.Load(),
				stats.accepted.Load(),
				stats.rejected.Load(),
				float64(stats.sent.Load())/maxFloat(elapsed, 1),
				float64(stats.accepted.Load())/maxFloat(elapsed, 1),
				formatMicros(lat.P95),
				stats.writeErr.Load(),
				stats.readErr.Load(),
			)
		}
	}
}

func printFinal(elapsed time.Duration, cfg config, stats *counters, reasons *reasonCounters, lat latencySummary) {
	seconds := elapsed.Seconds()
	fmt.Println()
	fmt.Println("bidload summary")
	if cfg.LiveRoomID > 0 {
		fmt.Printf("liveRoom=%d auction=%d buyers=%d duration=%s targetQPS=%.1f expect=%s\n", cfg.LiveRoomID, cfg.AuctionID, cfg.Buyers, cfg.Duration, cfg.QPS, cfg.Expect)
	} else {
		fmt.Printf("auction=%d buyers=%d duration=%s targetQPS=%.1f expect=%s\n", cfg.AuctionID, cfg.Buyers, cfg.Duration, cfg.QPS, cfg.Expect)
	}
	fmt.Printf("loginOK=%d loginErr=%d enrollOK=%d enrollErr=%d connectOK=%d connectErr=%d\n", stats.loginOK.Load(), stats.loginErr.Load(), stats.enrollOK.Load(), stats.enrollErr.Load(), stats.connectOK.Load(), stats.connectErr.Load())
	fmt.Printf("sent=%d ack=%d accepted=%d rejected=%d duplicate=%d writeErr=%d readErr=%d\n", stats.sent.Load(), stats.ack.Load(), stats.accepted.Load(), stats.rejected.Load(), stats.duplicate.Load(), stats.writeErr.Load(), stats.readErr.Load())
	fmt.Printf("sendQPS=%.2f acceptedQPS=%.2f ackRate=%.2f%%\n", float64(stats.sent.Load())/maxFloat(seconds, 1), float64(stats.accepted.Load())/maxFloat(seconds, 1), ratioPercent(stats.ack.Load(), stats.sent.Load()))
	fmt.Printf("ackLatency count=%d p50=%s p95=%s p99=%s max=%s\n", lat.Count, formatMicros(lat.P50), formatMicros(lat.P95), formatMicros(lat.P99), formatMicros(lat.Max))
	fmt.Printf("broadcastFrames accepted=%d rejected=%d\n", stats.broadcastAccepted.Load(), stats.broadcastRejected.Load())
	if counts := reasons.snapshot(); len(counts) > 0 {
		fmt.Println("reject reasons:")
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
