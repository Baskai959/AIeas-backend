package app

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/repository"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
)

type apiResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

func TestPingStillWorks(t *testing.T) {
	h := newTestServer()
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ping", nil)
	if resp.Code != 200 {
		t.Fatalf("expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "pong") {
		t.Fatalf("expected pong response, got %s", resp.Body.String())
	}
}

func TestAuthLoginAndMeSuccess(t *testing.T) {
	h := newTestServer()
	login := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/auth/login", `{"account":"buyer001","password":"Passw0rd!","role":"buyer"}`)
	if login.status != 200 || login.body.Code != 0 {
		t.Fatalf("expected successful login, got status=%d body=%+v raw=%s", login.status, login.body, login.raw)
	}
	var loginData struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"`
		User         struct {
			ID       string `json:"id"`
			Nickname string `json:"nickname"`
			Role     string `json:"role"`
			Status   string `json:"status"`
		} `json:"user"`
	}
	mustDecodeData(t, login.body.Data, &loginData)
	if loginData.AccessToken == "" || loginData.RefreshToken == "" || loginData.ExpiresIn != 43200 {
		t.Fatalf("unexpected token payload: %+v", loginData)
	}
	if loginData.User.ID != "u_1001" || loginData.User.Role != "buyer" || loginData.User.Status != "" {
		t.Fatalf("unexpected login user payload: %+v", loginData.User)
	}

	me := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auth/me", "", ut.Header{Key: "Authorization", Value: "Bearer " + loginData.AccessToken})
	if me.status != 200 || me.body.Code != 0 {
		t.Fatalf("expected successful me, got status=%d body=%+v raw=%s", me.status, me.body, me.raw)
	}
	var meData struct {
		ID       string `json:"id"`
		Nickname string `json:"nickname"`
		Role     string `json:"role"`
		Status   string `json:"status"`
	}
	mustDecodeData(t, me.body.Data, &meData)
	if meData.ID != "u_1001" || meData.Role != "buyer" || meData.Status != "ACTIVE" {
		t.Fatalf("unexpected me payload: %+v", meData)
	}
}

func TestNewServerWithConfigUsesJWTTTL(t *testing.T) {
	cfg := appconfig.Default()
	cfg.JWT.AccessTokenTTL = appconfig.Duration(45 * time.Minute)
	h := NewServerWithUserRepository(cfg, repository.NewSeedUserRepository())

	login := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/auth/login", `{"account":"buyer001","password":"Passw0rd!","role":"buyer"}`)
	if login.status != 200 || login.body.Code != 0 {
		t.Fatalf("expected successful login, got status=%d body=%+v raw=%s", login.status, login.body, login.raw)
	}

	var loginData struct {
		ExpiresIn int64 `json:"expiresIn"`
	}
	mustDecodeData(t, login.body.Data, &loginData)
	if loginData.ExpiresIn != 2700 {
		t.Fatalf("expected configured expiresIn 2700, got %d", loginData.ExpiresIn)
	}
}

func TestAuthLoginFailures(t *testing.T) {
	h := newTestServer()
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   int
	}{
		{name: "wrong password", body: `{"account":"buyer001","password":"wrong","role":"buyer"}`, wantStatus: 401, wantCode: 10004},
		{name: "disabled user", body: `{"account":"disabled001","password":"Passw0rd!","role":"buyer"}`, wantStatus: 403, wantCode: 10005},
		{name: "invalid role", body: `{"account":"buyer001","password":"Passw0rd!","role":"unknown"}`, wantStatus: 400, wantCode: 20001},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/auth/login", tt.body)
			if got.status != tt.wantStatus || got.body.Code != tt.wantCode {
				t.Fatalf("expected status/code %d/%d, got %d/%d raw=%s", tt.wantStatus, tt.wantCode, got.status, got.body.Code, got.raw)
			}
		})
	}
}

func TestAuthMeRejectsMissingAndInvalidToken(t *testing.T) {
	h := newTestServer()
	missing := doJSON(t, h.Engine, consts.MethodGet, "/api/v1/auth/me", "")
	if missing.status != 401 || missing.body.Code != 10001 {
		t.Fatalf("expected missing token error, got status=%d code=%d raw=%s", missing.status, missing.body.Code, missing.raw)
	}
	invalid := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auth/me", "", ut.Header{Key: "Authorization", Value: "Bearer invalid.token.value"})
	if invalid.status != 401 || invalid.body.Code != 10002 {
		t.Fatalf("expected invalid token error, got status=%d code=%d raw=%s", invalid.status, invalid.body.Code, invalid.raw)
	}
}

func TestMCPInitializeAndToolsList(t *testing.T) {
	h := newTestServer()
	readAPIKey := appconfig.Default().MCP.Read.APIKey
	controlAPIKey := appconfig.Default().MCP.Control.APIKey

	readInitResp := doMCPPath(t, h.Engine, "/mcp/read", readAPIKey, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`)
	if readInitResp.status != 200 || readInitResp.body.Error != nil {
		t.Fatalf("expected read initialize success, status=%d raw=%s", readInitResp.status, readInitResp.raw)
	}
	var initResult struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name string `json:"name"`
		} `json:"serverInfo"`
	}
	mustDecodeData(t, readInitResp.body.Result, &initResult)
	if initResult.ProtocolVersion != "2025-06-18" || initResult.ServerInfo.Name != "aieas-read-mcp" {
		t.Fatalf("unexpected read initialize result: %+v", initResult)
	}

	readToolsResp := doMCPPath(t, h.Engine, "/mcp/read", readAPIKey, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	if readToolsResp.status != 200 || readToolsResp.body.Error != nil {
		t.Fatalf("expected read tools/list success, status=%d raw=%s", readToolsResp.status, readToolsResp.raw)
	}
	var toolsResult struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	mustDecodeData(t, readToolsResp.body.Result, &toolsResult)
	if !containsTool(toolsResult.Tools, "read_live_session_bids") ||
		!containsTool(toolsResult.Tools, "read_live_session_settlement") ||
		containsTool(toolsResult.Tools, "get_merchant_live_control_context") ||
		containsTool(toolsResult.Tools, "operate_live_session_lot") {
		t.Fatalf("expected only read tools from read MCP, got %+v", toolsResult.Tools)
	}

	controlInitResp := doMCPPath(t, h.Engine, "/mcp/control", controlAPIKey, `{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`)
	if controlInitResp.status != 200 || controlInitResp.body.Error != nil {
		t.Fatalf("expected control initialize success, status=%d raw=%s", controlInitResp.status, controlInitResp.raw)
	}
	mustDecodeData(t, controlInitResp.body.Result, &initResult)
	if initResult.ProtocolVersion != "2025-06-18" || initResult.ServerInfo.Name != "aieas-control-mcp" {
		t.Fatalf("unexpected control initialize result: %+v", initResult)
	}

	controlToolsResp := doMCPPath(t, h.Engine, "/mcp/control", controlAPIKey, `{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}`)
	if controlToolsResp.status != 200 || controlToolsResp.body.Error != nil {
		t.Fatalf("expected control tools/list success, status=%d raw=%s", controlToolsResp.status, controlToolsResp.raw)
	}
	mustDecodeData(t, controlToolsResp.body.Result, &toolsResult)
	if containsTool(toolsResult.Tools, "read_live_session_bids") ||
		!containsTool(toolsResult.Tools, "get_merchant_live_control_context") ||
		!containsTool(toolsResult.Tools, "operate_live_session_lot") ||
		len(toolsResult.Tools) != 2 {
		t.Fatalf("expected only control tools from control MCP, got %+v", toolsResult.Tools)
	}
}

func TestMCPReadLiveSessionBidsAuthorization(t *testing.T) {
	ctx := context.Background()
	userRepo := repository.NewSeedUserRepository()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	orderRepo := repository.NewMemoryOrderRepository()

	item := domain.Item{SellerID: "u_2001", Title: "Vintage Camera", Category: "camera", ConditionGrade: domain.ConditionGood, Images: json.RawMessage(`[]`), Status: domain.ItemStatusReady}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}
	room := domain.LiveRoom{MerchantID: "u_2001", Title: "春拍专场", Status: domain.LiveRoomStatusLive}
	if err := roomRepo.Create(ctx, &room); err != nil {
		t.Fatalf("create room: %v", err)
	}
	closedAt := time.Now().UTC()
	session := domain.LiveSession{LiveRoomID: room.ID, MerchantID: "u_2001", Title: "春拍专场", Status: domain.LiveSessionStatusEnded, OpenedAt: closedAt.Add(-time.Hour), ClosedAt: &closedAt, LotsTotal: 1, LotsSold: 1, BidCount: 1, GMVCent: 120000}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	lot := domain.AuctionLot{
		AuctionID:     10001,
		ItemID:        item.ID,
		SellerID:      "u_2001",
		LiveRoomID:    room.ID,
		LiveSessionID: &session.ID,
		AuctionType:   domain.AuctionTypeEnglish,
		StartPrice:    100000,
		ReservePrice:  100000,
		CapPrice:      200000,
		IncrementRule: domain.DefaultIncrementRule(),
		RuleSnapshot:  json.RawMessage(`{}`),
		Status:        domain.AuctionStatusClosedWon,
		StartTime:     closedAt.Add(-time.Hour),
		EndTime:       closedAt,
		DealPrice:     ptrInt64(120000),
		WinnerID:      ptrString("u_1001"),
		ClosedAt:      &closedAt,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if err := bidRepo.Create(ctx, &domain.BidRecord{RequestID: "bid-1", AuctionID: lot.AuctionID, LiveSessionID: &session.ID, BidderID: "u_1001", BidPrice: 120000, BidTSMS: closedAt.UnixMilli(), Source: "ws", RiskResult: domain.BidRiskAllow}); err != nil {
		t.Fatalf("create bid: %v", err)
	}
	_, _, err := orderRepo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{AuctionID: lot.AuctionID, LiveSessionID: &session.ID, WinnerID: "u_1001", SellerID: "u_2001", DealPrice: 120000, Status: domain.OrderStatusPaid, PayStatus: domain.PayStatusPaid})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}

	cfg := appconfig.Default()
	cfg.MCP.Read.APIKey = "merchant-mcp-key"
	cfg.MCP.Read.ActorID = "u_2001"
	cfg.MCP.Read.ActorRole = "merchant"
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:        userRepo,
		ItemRepo:        itemRepo,
		AuctionRepo:     auctionRepo,
		LiveRoomRepo:    roomRepo,
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  service.DisabledProductAuditor{},
	})

	body := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"read_live_session_bids","arguments":{"sessionId":` + strconv.FormatUint(session.ID, 10) + `,"limit":10}}}`
	merchantResp := doMCPPath(t, h.Engine, "/mcp/read", "merchant-mcp-key", body)
	if merchantResp.status != 200 || merchantResp.body.Error != nil {
		t.Fatalf("expected merchant mcp success, status=%d raw=%s", merchantResp.status, merchantResp.raw)
	}
	var toolResult mcpToolResult
	mustDecodeData(t, merchantResp.body.Result, &toolResult)
	if len(toolResult.Content) != 1 {
		t.Fatalf("expected one tool content item, got %+v", toolResult)
	}
	var payload struct {
		Data struct {
			Items []domain.BidRecord `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(toolResult.Content[0].Text), &payload); err != nil {
		t.Fatalf("decode tool payload: %v text=%s", err, toolResult.Content[0].Text)
	}
	if len(payload.Data.Items) != 1 || payload.Data.Items[0].BidPrice != 120000 {
		t.Fatalf("unexpected bid payload: %+v", payload.Data.Items)
	}

	buyerCfg := appconfig.Default()
	buyerCfg.MCP.Read.APIKey = "buyer-mcp-key"
	buyerCfg.MCP.Read.ActorID = "u_1001"
	buyerCfg.MCP.Read.ActorRole = "buyer"
	buyerServer := NewServerWithDependencies(buyerCfg, ServerDependencies{
		UserRepo:        userRepo,
		ItemRepo:        itemRepo,
		AuctionRepo:     auctionRepo,
		LiveRoomRepo:    roomRepo,
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  service.DisabledProductAuditor{},
	})
	buyerResp := doMCPPath(t, buyerServer.Engine, "/mcp/read", "buyer-mcp-key", body)
	if buyerResp.status != 200 || buyerResp.body.Error == nil || buyerResp.body.Error.Code != -32003 {
		t.Fatalf("expected buyer forbidden json-rpc error, status=%d raw=%s", buyerResp.status, buyerResp.raw)
	}

	invalidKeyResp := doMCPPath(t, h.Engine, "/mcp/read", "wrong-key", body)
	if invalidKeyResp.status != 401 || invalidKeyResp.body.Error == nil || invalidKeyResp.body.Error.Code != -32001 {
		t.Fatalf("expected invalid api key unauthorized, status=%d raw=%s", invalidKeyResp.status, invalidKeyResp.raw)
	}
}

func TestMCPLiveControlContextAndOperations(t *testing.T) {
	ctx := context.Background()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	now := time.Now().UTC()

	room := domain.LiveRoom{MerchantID: "u_2001", Title: "直播控制台", Status: domain.LiveRoomStatusLive}
	if err := roomRepo.Create(ctx, &room); err != nil {
		t.Fatalf("create room: %v", err)
	}
	session := domain.LiveSession{LiveRoomID: room.ID, MerchantID: "u_2001", Title: "直播控制台", Status: domain.LiveSessionStatusLive, OpenedAt: now.Add(-time.Minute)}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	lot := domain.AuctionLot{
		AuctionID:      91001,
		ItemID:         1001,
		SellerID:       "u_2001",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   0,
		CapPrice:       0,
		IncrementRule:  domain.DefaultIncrementRule(),
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		DepositAmount:  100,
		Status:         domain.AuctionStatusReady,
		RuleSnapshot:   json.RawMessage(`{}`),
		DurationSec:    600,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create auction: %v", err)
	}

	cfg := appconfig.Default()
	cfg.MCP.Control.APIKey = "merchant-live-control-key"
	cfg.MCP.Control.ActorID = "u_2001"
	cfg.MCP.Control.ActorRole = "merchant"
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:        userRepo,
		AuctionRepo:     auctionRepo,
		LiveRoomRepo:    roomRepo,
		LiveSessionRepo: sessionRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  service.DisabledProductAuditor{},
	})

	contextBody := `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"get_merchant_live_control_context","arguments":{"merchantId":"u_2001"}}}`
	contextResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", contextBody)
	if contextResp.status != 200 || contextResp.body.Error != nil {
		t.Fatalf("expected live context success, status=%d raw=%s", contextResp.status, contextResp.raw)
	}
	contextPayload := decodeMCPToolEnvelope[service.MCPLiveControlContext](t, contextResp)
	if contextPayload.Data.Room.ID != room.ID || contextPayload.Data.Session == nil || contextPayload.Data.Session.ID != session.ID {
		t.Fatalf("unexpected live context: %+v", contextPayload.Data)
	}
	if len(contextPayload.Data.Lots.CandidateLots) != 1 || contextPayload.Data.Lots.CandidateLots[0].AuctionID != lot.AuctionID {
		t.Fatalf("expected candidate lot before on shelf, got %+v", contextPayload.Data.Lots.CandidateLots)
	}

	onShelfBody := `{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"onShelf"}}}`
	onShelfResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", onShelfBody)
	if onShelfResp.status != 200 || onShelfResp.body.Error != nil {
		t.Fatalf("expected onShelf success, status=%d raw=%s", onShelfResp.status, onShelfResp.raw)
	}
	onShelf := decodeMCPToolEnvelope[service.MCPLiveLotOperationResult](t, onShelfResp)
	if onShelf.Data.Lot == nil || onShelf.Data.Lot.LiveRoomID != room.ID || onShelf.Data.Context == nil || len(onShelf.Data.Context.Lots.UpcomingLots) != 1 {
		t.Fatalf("unexpected onShelf result: %+v", onShelf.Data)
	}

	startBody := `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"startExplain","durationSec":600}}}`
	startResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", startBody)
	if startResp.status != 200 || startResp.body.Error != nil {
		t.Fatalf("expected startExplain success, status=%d raw=%s", startResp.status, startResp.raw)
	}
	started := decodeMCPToolEnvelope[service.MCPLiveLotOperationResult](t, startResp)
	if started.Data.Context == nil || started.Data.Context.Lots.ExplainingLot == nil || started.Data.Context.Lots.ExplainingLot.AuctionID != lot.AuctionID {
		t.Fatalf("expected explaining lot after start, got %+v", started.Data)
	}
	if started.Data.Context.CurrentAuctionState == nil ||
		started.Data.Context.CurrentAuctionState.AuctionID != lot.AuctionID ||
		started.Data.Context.CurrentAuctionState.CurrentPrice != lot.StartPrice {
		t.Fatalf("expected current auction state after start, got %+v", started.Data.Context.CurrentAuctionState)
	}

	hammerBody := `{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"hammer","force":true,"requestId":"mcp-live-control-hammer-1"}}}`
	hammerResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", hammerBody)
	if hammerResp.status != 200 || hammerResp.body.Error != nil {
		t.Fatalf("expected hammer success, status=%d raw=%s", hammerResp.status, hammerResp.raw)
	}
	hammered := decodeMCPToolEnvelope[service.MCPLiveLotOperationResult](t, hammerResp)
	if hammered.Data.HammerResult == nil || hammered.Data.HammerResult.Status != domain.AuctionStatusClosedFailed {
		t.Fatalf("expected closed failed hammer result without bids, got %+v", hammered.Data.HammerResult)
	}
	if hammered.Data.Context == nil || hammered.Data.Context.Lots.ExplainingLot != nil || len(hammered.Data.Context.Lots.UnsoldLots) != 1 {
		t.Fatalf("unexpected context after hammer: %+v", hammered.Data.Context)
	}
}

func TestAuthRefreshLogoutAndAdminLogin(t *testing.T) {
	h := newTestServer()
	login := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/auth/login", `{"account":"buyer001","password":"Passw0rd!","role":"buyer"}`)
	var loginData struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
	}
	mustDecodeData(t, login.body.Data, &loginData)

	refresh := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/auth/refresh", `{"refreshToken":"`+loginData.RefreshToken+`"}`)
	if refresh.status != 200 || refresh.body.Code != 0 {
		t.Fatalf("expected refresh success, got status=%d code=%d raw=%s", refresh.status, refresh.body.Code, refresh.raw)
	}

	logout := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auth/logout", `{"refreshToken":"`+loginData.RefreshToken+`"}`, ut.Header{Key: "Authorization", Value: "Bearer " + loginData.AccessToken})
	if logout.status != 200 || logout.body.Code != 0 {
		t.Fatalf("expected logout success, got status=%d code=%d raw=%s", logout.status, logout.body.Code, logout.raw)
	}
	meAfterLogout := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auth/me", "", ut.Header{Key: "Authorization", Value: "Bearer " + loginData.AccessToken})
	if meAfterLogout.status != 401 || meAfterLogout.body.Code != 10002 {
		t.Fatalf("expected revoked token error, got status=%d code=%d raw=%s", meAfterLogout.status, meAfterLogout.body.Code, meAfterLogout.raw)
	}

	admin := doJSON(t, h.Engine, consts.MethodPost, "/api/v1/admin/auth/login", `{"account":"admin001","password":"AdminPassw0rd!"}`)
	if admin.status != 200 || admin.body.Code != 0 {
		t.Fatalf("expected admin login success, got status=%d code=%d raw=%s", admin.status, admin.body.Code, admin.raw)
	}
}

func TestItemCRUDRoutes(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	adminToken := loginForToken(t, h.Engine, "admin001", "AdminPassw0rd!", "admin")

	create := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Vintage Camera",
		"category":       "camera",
		"brand":          "Minolta",
		"conditionGrade": "GOOD",
		"description":    "tested",
	}, []multipartTestFile{
		{FieldName: "images", Filename: "camera.jpg", Body: "fake image bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 200 || create.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
	var created struct {
		ID       uint64   `json:"id"`
		SellerID string   `json:"sellerId"`
		Status   string   `json:"status"`
		Images   []string `json:"images"`
	}
	mustDecodeData(t, create.body.Data, &created)
	if created.ID == 0 || created.SellerID != "u_2001" || created.Status != "PENDING_AUDIT" {
		t.Fatalf("unexpected created item: %+v", created)
	}
	if len(created.Images) != 1 || !strings.HasPrefix(created.Images[0], "/api/v1/images/") || strings.Contains(created.Images[0], "/items/") {
		t.Fatalf("expected proxied image URL, got %+v", created.Images)
	}
	imageResp := ut.PerformRequest(h.Engine, consts.MethodGet, created.Images[0], nil)
	if imageResp.Code != 200 || imageResp.Body.String() != "fake image bytes" {
		t.Fatalf("expected image proxy success, got status=%d body=%q", imageResp.Code, imageResp.Body.String())
	}

	patch := doMultipartWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/items/"+strconv.FormatUint(created.ID, 10), map[string]string{
		"status": "READY",
		"title":  "Vintage Camera Kit",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected item patch success, got status=%d raw=%s", patch.status, patch.raw)
	}
	var patched struct {
		ID     uint64 `json:"id"`
		Status string `json:"status"`
	}
	mustDecodeData(t, patch.body.Data, &patched)
	if patched.ID != created.ID || patched.Status != "PENDING_AUDIT" {
		t.Fatalf("expected merchant patch to submit item for audit, got %+v", patched)
	}

	adminPatch := doMultipartWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/items/"+strconv.FormatUint(created.ID, 10), map[string]string{
		"status": "READY",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if adminPatch.status != 200 || adminPatch.body.Code != 0 {
		t.Fatalf("expected admin item patch success, got status=%d raw=%s", adminPatch.status, adminPatch.raw)
	}

	list := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/items?status=READY", "", ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if list.status != 200 || list.body.Code != 0 {
		t.Fatalf("expected item list success, got status=%d raw=%s", list.status, list.raw)
	}
	var listed struct {
		Items []struct {
			ID     uint64 `json:"id"`
			Status string `json:"status"`
		} `json:"items"`
	}
	mustDecodeData(t, list.body.Data, &listed)
	if len(listed.Items) != 1 || listed.Items[0].ID != created.ID || listed.Items[0].Status != "READY" {
		t.Fatalf("unexpected item list: %+v", listed)
	}

	deleteResp := doJSONWithHeaders(t, h.Engine, consts.MethodDelete, "/api/v1/items/"+strconv.FormatUint(created.ID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if deleteResp.status != 200 || deleteResp.body.Code != 0 {
		t.Fatalf("expected item delete success, got status=%d raw=%s", deleteResp.status, deleteResp.raw)
	}
}

func TestLiveRoomCoverUploadRoute(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-rooms", `{"title":"商家直播间"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 200 || create.body.Code != 0 {
		t.Fatalf("expected live room create success, got status=%d raw=%s", create.status, create.raw)
	}
	var room struct {
		ID       uint64 `json:"id"`
		CoverURL string `json:"coverUrl"`
	}
	mustDecodeData(t, create.body.Data, &room)
	if room.ID == 0 {
		t.Fatalf("expected live room id, got %+v", room)
	}

	upload := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-rooms/"+strconv.FormatUint(room.ID, 10)+"/cover", nil, []multipartTestFile{
		{FieldName: "image", Filename: "live-cover.jpg", Body: "live room cover bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token}, ut.Header{Key: "Idempotency-Key", Value: "live-room-cover-1"})
	if upload.status != 200 || upload.body.Code != 0 {
		t.Fatalf("expected live room cover upload success, got status=%d raw=%s", upload.status, upload.raw)
	}
	var updated struct {
		ID       uint64 `json:"id"`
		CoverURL string `json:"coverUrl"`
	}
	mustDecodeData(t, upload.body.Data, &updated)
	if updated.ID != room.ID || !strings.HasPrefix(updated.CoverURL, "/api/v1/images/") {
		t.Fatalf("expected updated cover URL, got %+v", updated)
	}
	imageResp := ut.PerformRequest(h.Engine, consts.MethodGet, updated.CoverURL, nil)
	if imageResp.Code != 200 || imageResp.Body.String() != "live room cover bytes" {
		t.Fatalf("expected uploaded cover proxy success, got status=%d body=%q", imageResp.Code, imageResp.Body.String())
	}
}

func TestItemCreateTriggersProductAuditCallback(t *testing.T) {
	cases := []struct {
		name            string
		callbackSuccess bool
		approved        bool
		wantStatus      string
	}{
		{
			name:            "approved",
			callbackSuccess: true,
			approved:        true,
			wantStatus:      "READY",
		},
		{
			name:            "rejected",
			callbackSuccess: true,
			approved:        false,
			wantStatus:      "REJECTED",
		},
		{
			name:            "audit failed",
			callbackSuccess: false,
			approved:        false,
			wantStatus:      "PENDING_AUDIT",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			auditor := &captureProductAuditor{result: service.ProductAuditResult{Success: true, RequestID: "agent-product-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
			h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
				UserRepo:       repository.NewSeedUserRepository(),
				ObjectUploader: objectstorage.NewMemoryUploader(""),
				ProductAuditor: auditor,
			})
			token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

			resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
				"title":          "机械键盘 87键",
				"category":       "电脑外设/键盘",
				"conditionGrade": "GOOD",
				"description":    "RGB背光，适合办公和游戏",
			}, []multipartTestFile{
				{FieldName: "images", Filename: "keyboard.jpg", Body: "audit image bytes"},
			}, ut.Header{Key: "Authorization", Value: "Bearer " + token})
			if resp.status != 200 || resp.body.Code != 0 {
				t.Fatalf("expected item create success, got status=%d raw=%s", resp.status, resp.raw)
			}
			var item struct {
				ID     uint64 `json:"id"`
				Status string `json:"status"`
			}
			mustDecodeData(t, resp.body.Data, &item)
			if item.Status != "PENDING_AUDIT" {
				t.Fatalf("expected create response to stay PENDING_AUDIT, got %+v", item)
			}
			input := auditor.waitInput(t)
			if input.ProductText == "" || !strings.Contains(input.ProductText, "机械键盘 87键") || !strings.Contains(input.ProductText, "电脑外设/键盘") {
				t.Fatalf("unexpected audit product text: %q", input.ProductText)
			}
			if string(input.Image) != "audit image bytes" || input.ImageName != "keyboard.jpg" {
				t.Fatalf("unexpected audit image: name=%q body=%q", input.ImageName, string(input.Image))
			}
			if input.CallbackURL != appconfig.Default().Agent.ProductAuditCallbackURL ||
				input.CallbackHeaders["X-Callback-Key"] != appconfig.Default().Agent.LiveAnalysisCallbackAPIKey ||
				input.CallbackHeaders["Authorization"] != "Bearer "+appconfig.Default().Agent.LiveAnalysisCallbackAPIKey {
				t.Fatalf("unexpected audit callback config: %+v", input)
			}
			if contextItemID, ok := input.CallbackContext["itemId"].(uint64); !ok || contextItemID != item.ID {
				t.Fatalf("unexpected callback context itemId: %+v", input.CallbackContext)
			}
			if tt.name == "approved" {
				badCallback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items/audit/callback", productAuditCallbackJSON(t, tt.callbackSuccess, tt.approved, input.CallbackContext))
				if badCallback.status != 401 {
					t.Fatalf("expected callback auth failure, got status=%d raw=%s", badCallback.status, badCallback.raw)
				}
			}
			callback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items/audit/callback", productAuditCallbackJSON(t, tt.callbackSuccess, tt.approved, input.CallbackContext),
				ut.Header{Key: "X-Callback-Key", Value: appconfig.Default().Agent.LiveAnalysisCallbackAPIKey},
			)
			if callback.status != 200 || callback.body.Code != 0 {
				t.Fatalf("expected callback success, got status=%d raw=%s", callback.status, callback.raw)
			}
			gotStatus := pollItemStatus(t, h.Engine, item.ID, token, tt.wantStatus)
			if gotStatus != tt.wantStatus {
				t.Fatalf("expected callback status %s, got %s", tt.wantStatus, gotStatus)
			}
		})
	}
}

func TestItemAuditCallbackIgnoresStaleSnapshot(t *testing.T) {
	auditor := &captureProductAuditor{result: service.ProductAuditResult{Success: true, RequestID: "agent-product-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 2)}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "机械键盘 87键",
		"category":       "电脑外设/键盘",
		"conditionGrade": "GOOD",
		"description":    "RGB背光，适合办公和游戏",
	}, []multipartTestFile{
		{FieldName: "images", Filename: "keyboard.jpg", Body: "audit image bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var item struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, resp.body.Data, &item)
	firstAudit := auditor.waitInput(t)

	patch := doMultipartWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/items/"+strconv.FormatUint(item.ID, 10), map[string]string{
		"title": "机械键盘 87键 改版",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected item patch success, got status=%d raw=%s", patch.status, patch.raw)
	}
	_ = auditor.waitInput(t)

	callback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items/audit/callback", productAuditCallbackJSON(t, true, true, firstAudit.CallbackContext),
		ut.Header{Key: "Authorization", Value: "Bearer " + appconfig.Default().Agent.LiveAnalysisCallbackAPIKey},
	)
	if callback.status != 200 || callback.body.Code != 0 {
		t.Fatalf("expected stale callback success response, got status=%d raw=%s", callback.status, callback.raw)
	}
	gotStatus := pollItemStatus(t, h.Engine, item.ID, token, "PENDING_AUDIT")
	if gotStatus != "PENDING_AUDIT" {
		t.Fatalf("expected stale callback to keep PENDING_AUDIT, got %s", gotStatus)
	}
}

func TestItemCreateRejectsImageLargerThan2MB(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":    "Large Image Item",
		"category": "camera",
	}, []multipartTestFile{
		{FieldName: "images", Filename: "large.jpg", Body: strings.Repeat("x", 2*1024*1024+1)},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if create.status != 400 || create.body.Code != 20001 {
		t.Fatalf("expected image size validation error, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestItemDescriptionOptimizeRoute(t *testing.T) {
	generator := &captureProductDescriptionGenerator{
		result: service.ProductDescriptionResult{
			Title:       "机械键盘 87键",
			Category:    "电脑外设/键盘",
			Description: "这是一款适合日常办公和游戏的 87 键机械键盘，成色良好，键帽干净，敲击手感清脆。",
		},
	}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		DescriptionGen: generator,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items/description/optimize", map[string]string{
		"title":     "机械键盘 87键",
		"category":  "电脑外设/键盘",
		"condition": "九成新",
	}, []multipartTestFile{
		{FieldName: "image", Filename: "keyboard.jpg", Body: "fake image bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected description optimize success, got status=%d body=%+v raw=%s", resp.status, resp.body, resp.raw)
	}
	var result service.ProductDescriptionResult
	mustDecodeData(t, resp.body.Data, &result)
	if result.Description == "" || result.Title != "机械键盘 87键" || result.Category != "电脑外设/键盘" {
		t.Fatalf("unexpected optimize result: %+v", result)
	}
	if generator.input.Title != "机械键盘 87键" || generator.input.Category != "电脑外设/键盘" || generator.input.Condition != "九成新" || generator.imageBody != "fake image bytes" {
		t.Fatalf("unexpected generator input: %+v image=%q", generator.input, generator.imageBody)
	}
}

func TestItemDescriptionOptimizeRouteUsesImageURL(t *testing.T) {
	uploader := objectstorage.NewMemoryUploader("")
	imageURL, err := uploader.Upload(context.Background(), objectstorage.UploadInput{
		Filename:    "saved-keyboard.jpg",
		ContentType: "image/jpeg",
		Size:        int64(len("saved image bytes")),
		Body:        strings.NewReader("saved image bytes"),
	})
	if err != nil {
		t.Fatalf("upload memory image: %v", err)
	}
	generator := &captureProductDescriptionGenerator{
		result: service.ProductDescriptionResult{
			Title:       "机械键盘 87键",
			Category:    "电脑外设/键盘",
			Description: "已保存图片也可以用于生成商品描述。",
		},
	}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: uploader,
		DescriptionGen: generator,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items/description/optimize", map[string]string{
		"title":     "机械键盘 87键",
		"category":  "电脑外设/键盘",
		"condition": "九成新",
		"imageUrl":  imageURL,
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected description optimize with imageUrl success, got status=%d body=%+v raw=%s", resp.status, resp.body, resp.raw)
	}
	if generator.imageBody != "saved image bytes" || generator.input.ImageName == "" {
		t.Fatalf("expected generator to receive saved image bytes, input=%+v image=%q", generator.input, generator.imageBody)
	}
}

func TestItemRoutesProtectItemsBoundToActiveAuctions(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	missing := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/items/999999", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if missing.status != 404 || missing.body.Code != 30001 {
		t.Fatalf("expected item not found code 30001, got status=%d code=%d raw=%s", missing.status, missing.body.Code, missing.raw)
	}

	createItem := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Running Lot Item",
		"category":       "collectible",
		"conditionGrade": "GOOD",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createItem.status != 200 || createItem.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d raw=%s", createItem.status, createItem.raw)
	}
	var itemData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, createItem.body.Data, &itemData)

	auctionBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":10000,"capPrice":20000,"depositAmount":0,"status":"READY","incrementRule":{"type":"fixed","amount":100,"maxBidSteps":10}}`
	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)

	start := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "item-active-auction-start"})
	if start.status != 200 || start.body.Code != 0 {
		t.Fatalf("expected auction start success, got status=%d raw=%s", start.status, start.raw)
	}

	descriptionPatch := doMultipartWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/items/"+strconv.FormatUint(itemData.ID, 10), map[string]string{
		"description": "updated display copy",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if descriptionPatch.status != 200 || descriptionPatch.body.Code != 0 {
		t.Fatalf("expected non-critical description patch success, got status=%d raw=%s", descriptionPatch.status, descriptionPatch.raw)
	}

	titlePatch := doMultipartWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/items/"+strconv.FormatUint(itemData.ID, 10), map[string]string{
		"title": "Blocked Critical Change",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if titlePatch.status != 409 || titlePatch.body.Code != 30003 {
		t.Fatalf("expected active-auction critical patch to fail with 30003, got status=%d code=%d raw=%s", titlePatch.status, titlePatch.body.Code, titlePatch.raw)
	}

	deleteResp := doJSONWithHeaders(t, h.Engine, consts.MethodDelete, "/api/v1/items/"+strconv.FormatUint(itemData.ID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if deleteResp.status != 409 || deleteResp.body.Code != 30003 {
		t.Fatalf("expected active-auction delete to fail with 30003, got status=%d code=%d raw=%s", deleteResp.status, deleteResp.body.Code, deleteResp.raw)
	}
}

func TestAuctionCreateAndUpdateReadyWithoutAudit(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	item := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Audit Required Lot",
		"category":       "collectible",
		"conditionGrade": "GOOD",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if item.status != 200 || item.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d raw=%s", item.status, item.raw)
	}
	var itemData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, item.body.Data, &itemData)

	readyBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"capPrice":20000,"depositAmount":1000,"status":"READY","incrementRule":{"type":"fixed","amount":500,"maxBidSteps":10}}`
	createReady := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", readyBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createReady.status != 200 || createReady.body.Code != 0 {
		t.Fatalf("expected create READY success, got status=%d raw=%s", createReady.status, createReady.raw)
	}

	pendingBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"capPrice":20000,"depositAmount":1000,"status":"PENDING_AUDIT","incrementRule":{"type":"fixed","amount":500,"maxBidSteps":10}}`
	createPending := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", pendingBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createPending.status != 400 || createPending.body.Code != 20001 {
		t.Fatalf("expected create PENDING_AUDIT to fail validation, got status=%d code=%d raw=%s", createPending.status, createPending.body.Code, createPending.raw)
	}

	systemBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":100,"reservePrice":5000,"capPrice":10000,"depositAmount":0,"status":"RUNNING","incrementRule":{"type":"fixed","amount":100,"maxBidSteps":5}}`
	createSystem := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", systemBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createSystem.status != 400 || createSystem.body.Code != 20001 {
		t.Fatalf("expected create RUNNING to fail validation, got status=%d code=%d raw=%s", createSystem.status, createSystem.body.Code, createSystem.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createReady.body.Data, &auctionData)

	patchRunning := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), `{"status":"RUNNING"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if patchRunning.status != 400 || patchRunning.body.Code != 20001 {
		t.Fatalf("expected patch RUNNING to fail validation, got status=%d code=%d raw=%s", patchRunning.status, patchRunning.body.Code, patchRunning.raw)
	}
}

func TestLiveRoomAutoMountLotRoute(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	createRoom := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-rooms", `{"title":"自动上架直播间"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createRoom.status != 200 || createRoom.body.Code != 0 {
		t.Fatalf("expected live room create success, got status=%d raw=%s", createRoom.status, createRoom.raw)
	}
	var roomData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, createRoom.body.Data, &roomData)

	item := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Auto Mount Lot",
		"category":       "collectible",
		"conditionGrade": "GOOD",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if item.status != 200 || item.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d raw=%s", item.status, item.raw)
	}
	var itemData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, item.body.Data, &itemData)

	auctionBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"depositAmount":1000,"status":"READY","incrementRule":{"type":"fixed","amount":500,"maxBidSteps":10}}`
	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)

	mount := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-rooms/lots", `{"auctionId":`+strconv.FormatUint(auctionData.AuctionID, 10)+`}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "auto-mount-lot-1"})
	if mount.status != 200 || mount.body.Code != 0 {
		t.Fatalf("expected auto mount success, got status=%d raw=%s", mount.status, mount.raw)
	}
	var mountData struct {
		Lot struct {
			AuctionID  uint64 `json:"auctionId"`
			LiveRoomID uint64 `json:"liveRoomId"`
		} `json:"lot"`
	}
	mustDecodeData(t, mount.body.Data, &mountData)
	if mountData.Lot.AuctionID != auctionData.AuctionID || mountData.Lot.LiveRoomID != roomData.ID {
		t.Fatalf("unexpected mount response: %+v want auction=%d room=%d", mountData, auctionData.AuctionID, roomData.ID)
	}
}

func TestAuctionRoutesStateAndIdempotencyMiddleware(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	item := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Watch",
		"category":       "luxury",
		"conditionGrade": "LIKE_NEW",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if item.status != 200 || item.body.Code != 0 {
		t.Fatalf("expected item create success, got status=%d raw=%s", item.status, item.raw)
	}
	var itemData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, item.body.Data, &itemData)

	auctionBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"capPrice":20000,"depositAmount":1000,"incrementRule":{"type":"fixed","amount":500,"maxBidSteps":10}}`
	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
		Status    string `json:"status"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)
	if auctionData.AuctionID == 0 || auctionData.Status != "READY" {
		t.Fatalf("unexpected auction create payload: %+v", auctionData)
	}

	noKey := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if noKey.status != 400 || noKey.body.Code != 20011 {
		t.Fatalf("expected missing idempotency key, got status=%d code=%d raw=%s", noKey.status, noKey.body.Code, noKey.raw)
	}

	start := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "idem-start-1"})
	if start.status != 200 || start.body.Code != 0 {
		t.Fatalf("expected auction start success, got status=%d raw=%s", start.status, start.raw)
	}

	state := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/state", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if state.status != 200 || state.body.Code != 0 {
		t.Fatalf("expected auction state success for buyer, got status=%d raw=%s", state.status, state.raw)
	}
	var stateData struct {
		AuctionID    uint64 `json:"auctionId"`
		Status       string `json:"status"`
		CurrentPrice int64  `json:"currentPrice"`
		Source       string `json:"source"`
	}
	mustDecodeData(t, state.body.Data, &stateData)
	if stateData.AuctionID != auctionData.AuctionID || stateData.Status != "RUNNING" || stateData.CurrentPrice != 10000 || stateData.Source != "redis" {
		t.Fatalf("unexpected state payload: %+v", stateData)
	}

	enroll := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/enroll", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if enroll.status != 200 || enroll.body.Code != 0 {
		t.Fatalf("expected enroll success, got status=%d raw=%s", enroll.status, enroll.raw)
	}
	var enrollData struct {
		AuctionID uint64 `json:"auctionId"`
		UserID    string `json:"userId"`
		Status    string `json:"status"`
	}
	mustDecodeData(t, enroll.body.Data, &enrollData)
	if enrollData.AuctionID != auctionData.AuctionID || enrollData.UserID != "u_1001" || enrollData.Status != "READY" {
		t.Fatalf("unexpected enroll payload: %+v", enrollData)
	}

	hammer := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/hammer", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "hammer-empty-1"})
	if hammer.status != 409 || hammer.body.Code != 20010 {
		t.Fatalf("expected not-ended hammer rejection, got status=%d code=%d raw=%s", hammer.status, hammer.body.Code, hammer.raw)
	}
}

func TestOrderRoutesPayIdempotency(t *testing.T) {
	cfg := appconfig.Default()
	orderRepo := repository.NewMemoryOrderRepository()
	deadline := time.Now().UTC().Add(time.Hour)
	order, _, err := orderRepo.CreateIfAbsentByAuction(t.Context(), &domain.OrderDeal{
		AuctionID:     12345,
		WinnerID:      "u_1001",
		SellerID:      "u_2001",
		DealPrice:     12000,
		DepositAmount: 1000,
		Status:        domain.OrderStatusCreated,
		PayStatus:     domain.PayStatusUnpaid,
		PayDeadline:   &deadline,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	h := NewServerWithDependencies(cfg, ServerDependencies{UserRepo: repository.NewSeedUserRepository(), OrderRepo: orderRepo})
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	mine := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/orders/mine", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if mine.status != 200 || mine.body.Code != 0 {
		t.Fatalf("expected mine success, got status=%d raw=%s", mine.status, mine.raw)
	}
	var mineData struct {
		Orders []domain.OrderDeal `json:"orders"`
	}
	mustDecodeData(t, mine.body.Data, &mineData)
	if len(mineData.Orders) != 1 || mineData.Orders[0].ID != order.ID {
		t.Fatalf("unexpected mine orders: %+v", mineData.Orders)
	}

	noKey := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/orders/"+strconv.FormatUint(order.ID, 10)+"/pay", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if noKey.status != 400 || noKey.body.Code != 20011 {
		t.Fatalf("expected pay idempotency key error, got status=%d raw=%s", noKey.status, noKey.raw)
	}
	paid := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/orders/"+strconv.FormatUint(order.ID, 10)+"/pay", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken}, ut.Header{Key: "Idempotency-Key", Value: "pay-1"})
	if paid.status != 200 || paid.body.Code != 0 {
		t.Fatalf("expected pay success, got status=%d raw=%s", paid.status, paid.raw)
	}
	paidAgain := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/orders/"+strconv.FormatUint(order.ID, 10)+"/pay", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken}, ut.Header{Key: "Idempotency-Key", Value: "pay-1"})
	if paidAgain.status != 200 || paidAgain.body.Code != 0 {
		t.Fatalf("expected duplicate pay success, got status=%d raw=%s", paidAgain.status, paidAgain.raw)
	}
}

func TestLiveAnalysisReportRoutes(t *testing.T) {
	requester := &captureLiveAnalysisRequester{
		result: service.LiveAnalysisAsyncResult{RequestID: "agent-live-analysis-1", Status: "ACCEPTED"},
	}
	reportRepo := repository.NewMemoryLiveAnalysisReportRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	closedAt := time.Now().UTC()
	session := domain.LiveSession{
		LiveRoomID:  9001,
		MerchantID:  "u_2001",
		Status:      domain.LiveSessionStatusEnded,
		OpenedAt:    closedAt.Add(-time.Hour),
		ClosedAt:    &closedAt,
		LotsTotal:   3,
		LotsSold:    2,
		BidCount:    8,
		GMVCent:     120000,
		ViewerPeak:  20,
		ViewerTotal: 80,
	}
	if err := sessionRepo.Create(t.Context(), &session); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:               repository.NewSeedUserRepository(),
		LiveSessionRepo:        sessionRepo,
		ObjectUploader:         objectstorage.NewMemoryUploader(""),
		ProductAuditor:         service.DisabledProductAuditor{},
		LiveAnalysisReportRepo: reportRepo,
		LiveAnalysisRequester:  requester,
	})
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	running := getLiveAnalysisTask(t, h.Engine, session.ID, merchantToken)
	if running.TaskID == "" || running.LiveSessionID != session.ID || running.MerchantID != "u_2001" {
		t.Fatalf("unexpected running task: %+v", running)
	}
	if running.Status != service.LiveAnalysisTaskRunning || running.Report != "" || running.AttemptCount != 1 {
		t.Fatalf("expected accepted running task with empty report, got %+v", running)
	}
	if requester.prompt() != "帮我总结商家id为u_2001直播场次id为"+strconv.FormatUint(session.ID, 10)+"的直播情况，重点看成交、出价、订单和风险问题。" ||
		requester.input.CallbackContext["taskId"] != running.TaskID ||
		requester.input.CallbackContext["liveSessionId"] != session.ID ||
		requester.input.CallbackContext["attempt"] != 1 ||
		requester.input.CallbackHeaders["X-Callback-Key"] == "" {
		t.Fatalf("unexpected requester input: %+v", requester.input)
	}

	badCallback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-analysis/callback", `{"request_id":"agent-live-analysis-1","success":true,"status":"COMPLETED","summary":"bad","callback_context":{"taskId":"`+running.TaskID+`","liveSessionId":`+strconv.FormatUint(session.ID, 10)+`,"attempt":1}}`)
	if badCallback.status != 401 {
		t.Fatalf("expected callback auth failure, got status=%d raw=%s", badCallback.status, badCallback.raw)
	}
	callback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-analysis/callback", `{"request_id":"agent-live-analysis-1","success":true,"status":"COMPLETED","summary":"本场直播共成交 3 件拍品，转化良好。","error_message":null,"callback_context":{"taskId":"`+running.TaskID+`","liveSessionId":`+strconv.FormatUint(session.ID, 10)+`,"merchantId":"u_2001","attempt":1},"completed_at":"2026-05-26T10:30:00Z"}`,
		ut.Header{Key: "X-Callback-Key", Value: appconfig.Default().Agent.LiveAnalysisCallbackAPIKey},
	)
	if callback.status != 200 || callback.body.Code != 0 {
		t.Fatalf("expected callback success, got status=%d raw=%s", callback.status, callback.raw)
	}

	got := pollLiveAnalysisTask(t, h.Engine, session.ID, merchantToken)
	if got.Status != service.LiveAnalysisTaskSucceeded || got.Report != "本场直播共成交 3 件拍品，转化良好。" {
		t.Fatalf("unexpected finished task: %+v", got)
	}
	persisted, err := reportRepo.FindByLiveSessionID(t.Context(), session.ID)
	if err != nil || persisted.Status != service.LiveAnalysisTaskSucceeded || persisted.Report != got.Report {
		t.Fatalf("expected persisted succeeded report, report=%+v err=%v", persisted, err)
	}

	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")
	forbidden := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/live-analysis/reports/"+strconv.FormatUint(session.ID, 10), "",
		ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken},
	)
	if forbidden.status != 403 || forbidden.body.Code != 10003 {
		t.Fatalf("expected buyer forbidden, got status=%d raw=%s", forbidden.status, forbidden.raw)
	}
}

func TestLiveSessionEndedHookStartsLiveAnalysis(t *testing.T) {
	requester := &captureLiveAnalysisRequester{
		result: service.LiveAnalysisAsyncResult{RequestID: "agent-hook-1", Status: "ACCEPTED"},
	}
	reportRepo := repository.NewMemoryLiveAnalysisReportRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	closedAt := time.Now().UTC()
	session := domain.LiveSession{
		LiveRoomID: 9002,
		MerchantID: "u_2001",
		Status:     domain.LiveSessionStatusEnded,
		OpenedAt:   closedAt.Add(-30 * time.Minute),
		ClosedAt:   &closedAt,
	}
	if err := sessionRepo.Create(t.Context(), &session); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	svc := service.NewLiveAnalysisService(reportRepo, sessionRepo, requester, service.LiveAnalysisOptions{
		CallbackURL:    appconfig.Default().Agent.LiveAnalysisCallbackURL,
		CallbackAPIKey: appconfig.Default().Agent.LiveAnalysisCallbackAPIKey,
	})

	hook := buildLiveSessionEndedHook(nil, svc)
	if hook == nil {
		t.Fatal("expected hook")
	}
	hook(t.Context(), session)

	task, err := reportRepo.FindByLiveSessionID(t.Context(), session.ID)
	if err != nil {
		t.Fatalf("find report: %v", err)
	}
	if task.Status != service.LiveAnalysisTaskRunning || task.LiveSessionID != session.ID || requester.prompt() == "" {
		t.Fatalf("expected hook to start live analysis, task=%+v requester=%+v", task, requester.input)
	}
}

func TestAdminRoutesMinimalClosedLoop(t *testing.T) {
	cfg := appconfig.Default()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	auditRepo := repository.NewMemoryAuditRepository()
	now := time.Now().UTC()
	auction := &domain.AuctionLot{
		AuctionID:      88001,
		ItemID:         1001,
		SellerID:       "u_2001",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   0,
		CapPrice:       2000,
		IncrementRule:  json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
		RuleSnapshot:   json.RawMessage(`{"auctionType":"ENGLISH"}`),
		StartTime:      now.Add(time.Minute),
		EndTime:        now.Add(time.Hour),
	}
	if err := auctionRepo.Create(t.Context(), auction); err != nil {
		t.Fatalf("seed auction: %v", err)
	}
	if _, _, err := orderRepo.CreateIfAbsentByAuction(t.Context(), &domain.OrderDeal{AuctionID: 88001, WinnerID: "u_1001", SellerID: "u_2001", DealPrice: 1200, Status: domain.OrderStatusCreated, PayStatus: domain.PayStatusUnpaid}); err != nil {
		t.Fatalf("seed order: %v", err)
	}
	if err := riskRepo.CreateEvent(t.Context(), &domain.RiskEvent{EventType: "BID_FREQ", UserID: "u_1001", AuctionID: 88001, Severity: domain.RiskSeverityMid, Status: domain.RiskEventPending}); err != nil {
		t.Fatalf("seed risk event: %v", err)
	}
	h := NewServerWithDependencies(cfg, ServerDependencies{UserRepo: userRepo, AuctionRepo: auctionRepo, OrderRepo: orderRepo, RiskRepo: riskRepo, AuditRepo: auditRepo})
	adminToken := loginForToken(t, h.Engine, "admin001", "AdminPassw0rd!", "admin")
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	noAuth := doJSON(t, h.Engine, consts.MethodGet, "/api/v1/admin/users", "")
	if noAuth.status != 401 || noAuth.body.Code != 10001 {
		t.Fatalf("expected admin users require auth, got status=%d raw=%s", noAuth.status, noAuth.raw)
	}
	forbidden := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/users", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if forbidden.status != 403 || forbidden.body.Code != 10003 {
		t.Fatalf("expected buyer forbidden, got status=%d raw=%s", forbidden.status, forbidden.raw)
	}

	users := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/users?page=1&page_size=20", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if users.status != 200 || users.body.Code != 0 {
		t.Fatalf("expected admin users success, got status=%d raw=%s", users.status, users.raw)
	}

	patchUser := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/admin/users/u_1001", `{"status":"DISABLED","reason":"test"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "admin-user-disable-1"})
	if patchUser.status != 200 || patchUser.body.Code != 0 {
		t.Fatalf("expected admin user patch success, got status=%d raw=%s", patchUser.status, patchUser.raw)
	}
	patchUserAgain := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/admin/users/u_1001", `{"status":"DISABLED","reason":"test"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "admin-user-disable-1"})
	if patchUserAgain.status != 200 || patchUserAgain.body.Code != 0 || patchUserAgain.raw != patchUser.raw {
		t.Fatalf("expected identical idempotency replay, first=%s second=%s", patchUser.raw, patchUserAgain.raw)
	}
	patchUserConflict := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/admin/users/u_1001", `{"status":"ACTIVE","reason":"different"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "admin-user-disable-1"})
	if patchUserConflict.status != 409 || patchUserConflict.body.Code != 20012 {
		t.Fatalf("expected idempotency body conflict, got status=%d code=%d raw=%s", patchUserConflict.status, patchUserConflict.body.Code, patchUserConflict.raw)
	}
	updated, err := userRepo.FindByID("u_1001")
	if err != nil || updated.Status != domain.UserStatusDisabled {
		t.Fatalf("expected user disabled, user=%+v err=%v", updated, err)
	}
	if len(auditRepo.Logs()) == 0 {
		t.Fatal("expected write operation to create audit log")
	}

	auctions := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/auctions?page=1&page_size=20", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if auctions.status != 200 || auctions.body.Code != 0 {
		t.Fatalf("expected admin auctions success, got status=%d raw=%s", auctions.status, auctions.raw)
	}
	auditAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/admin/auctions/88001/audit", `{"auditResult":"APPROVED","reason":"ok"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "admin-audit-1"})
	if auditAuction.status != 200 || auditAuction.body.Code != 0 {
		t.Fatalf("expected admin auction audit success, got status=%d raw=%s", auditAuction.status, auditAuction.raw)
	}
	approved, err := auctionRepo.FindByID(t.Context(), 88001)
	if err != nil || approved.Status != domain.AuctionStatusReady {
		t.Fatalf("expected auction ready, auction=%+v err=%v", approved, err)
	}

	blacklist := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/admin/blacklist", `{"userId":"u_1002","reason":"abuse"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "blacklist-add-1"})
	if blacklist.status != 200 || blacklist.body.Code != 0 {
		t.Fatalf("expected blacklist add success, got status=%d raw=%s", blacklist.status, blacklist.raw)
	}
	blacklistList := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/blacklist", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if blacklistList.status != 200 || blacklistList.body.Code != 0 {
		t.Fatalf("expected blacklist list success, got status=%d raw=%s", blacklistList.status, blacklistList.raw)
	}
	blacklistDelete := doJSONWithHeaders(t, h.Engine, consts.MethodDelete, "/api/v1/admin/blacklist/u_1002", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "blacklist-del-1"})
	if blacklistDelete.status != 200 || blacklistDelete.body.Code != 0 {
		t.Fatalf("expected blacklist delete success, got status=%d raw=%s", blacklistDelete.status, blacklistDelete.raw)
	}

	orders := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/orders", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if orders.status != 200 || orders.body.Code != 0 {
		t.Fatalf("expected admin orders success, got status=%d raw=%s", orders.status, orders.raw)
	}
	auditLogs := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/audit-logs", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if auditLogs.status != 200 || auditLogs.body.Code != 0 {
		t.Fatalf("expected admin audit logs success, got status=%d raw=%s", auditLogs.status, auditLogs.raw)
	}
	merchantCreateItem := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/items", map[string]string{
		"title":          "Audit Log Item",
		"category":       "collectible",
		"conditionGrade": "GOOD",
	}, nil, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if merchantCreateItem.status != 200 || merchantCreateItem.body.Code != 0 {
		t.Fatalf("expected merchant item create success, got status=%d raw=%s", merchantCreateItem.status, merchantCreateItem.raw)
	}
	merchantAuditLogs := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/audit-logs?page=1&page_size=5", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if merchantAuditLogs.status != 200 || merchantAuditLogs.body.Code != 0 {
		t.Fatalf("expected merchant audit logs success, got status=%d raw=%s", merchantAuditLogs.status, merchantAuditLogs.raw)
	}
	var merchantAuditData struct {
		Items []domain.AuditLog `json:"items"`
	}
	mustDecodeData(t, merchantAuditLogs.body.Data, &merchantAuditData)
	if len(merchantAuditData.Items) == 0 {
		t.Fatalf("expected merchant audit logs, got %+v", merchantAuditData)
	}
	for _, log := range merchantAuditData.Items {
		if log.OperatorID != "u_2001" {
			t.Fatalf("expected only merchant logs, got %+v", merchantAuditData.Items)
		}
	}
	riskEvents := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/risk/events", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if riskEvents.status != 200 || riskEvents.body.Code != 0 {
		t.Fatalf("expected risk events success, got status=%d raw=%s", riskEvents.status, riskEvents.raw)
	}
	handleRisk := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/admin/risk/events/1", `{"status":"REVIEWED","remark":"ok"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "risk-event-1"})
	if handleRisk.status != 200 || handleRisk.body.Code != 0 {
		t.Fatalf("expected risk handle success, got status=%d raw=%s", handleRisk.status, handleRisk.raw)
	}
	handled, err := riskRepo.FindEventByID(t.Context(), 1)
	if err != nil || handled.Status != domain.RiskEventReviewed || handled.ReviewedBy != "u_9001" {
		t.Fatalf("expected risk reviewed by admin, event=%+v err=%v", handled, err)
	}
}

func TestAdminDashboardMetrics(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 5, 27, 10, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Hour)
	closedAt := start.Add(45 * time.Minute)
	paidAt := start.Add(70 * time.Minute)

	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()

	room := domain.LiveRoom{MerchantID: "u_2001", Title: "监控测试直播间", Status: domain.LiveRoomStatusLive}
	if err := roomRepo.Create(ctx, &room); err != nil {
		t.Fatalf("create room: %v", err)
	}
	session := domain.LiveSession{
		LiveRoomID:  room.ID,
		MerchantID:  "u_2001",
		Title:       "监控测试场次",
		Status:      domain.LiveSessionStatusLive,
		OpenedAt:    start.Add(5 * time.Minute),
		LotsTotal:   3,
		LotsSold:    1,
		LotsUnsold:  1,
		BidCount:    2,
		GMVCent:     1500,
		ViewerPeak:  12,
		ViewerTotal: 20,
	}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	won := domain.AuctionLot{
		AuctionID:     99001,
		ItemID:        1,
		SellerID:      "u_2001",
		LiveRoomID:    room.ID,
		LiveSessionID: &session.ID,
		AuctionType:   domain.AuctionTypeEnglish,
		StartPrice:    1000,
		ReservePrice:  1000,
		IncrementRule: domain.DefaultIncrementRule(),
		RuleSnapshot:  json.RawMessage(`{}`),
		Status:        domain.AuctionStatusClosedWon,
		StartTime:     start,
		EndTime:       closedAt,
		WinnerID:      ptrString("u_1001"),
		DealPrice:     ptrInt64(1500),
		ClosedAt:      &closedAt,
		CreatedAt:     start.Add(10 * time.Minute),
	}
	if err := auctionRepo.Create(ctx, &won); err != nil {
		t.Fatalf("create won auction: %v", err)
	}
	failed := won
	failed.AuctionID = 99002
	failed.Status = domain.AuctionStatusClosedFailed
	failed.WinnerID = nil
	failed.DealPrice = nil
	if err := auctionRepo.Create(ctx, &failed); err != nil {
		t.Fatalf("create failed auction: %v", err)
	}
	running := won
	running.AuctionID = 99003
	running.Status = domain.AuctionStatusRunning
	running.WinnerID = nil
	running.DealPrice = nil
	running.ClosedAt = nil
	if err := auctionRepo.Create(ctx, &running); err != nil {
		t.Fatalf("create running auction: %v", err)
	}
	_, _, err := orderRepo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID: won.AuctionID,
		WinnerID:  "u_1001",
		SellerID:  "u_2001",
		DealPrice: 1500,
		Status:    domain.OrderStatusPaid,
		PayStatus: domain.PayStatusPaid,
		PaidAt:    &paidAt,
		CreatedAt: start.Add(20 * time.Minute),
		UpdatedAt: paidAt,
	})
	if err != nil {
		t.Fatalf("create paid order: %v", err)
	}
	_, _, err = orderRepo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID: failed.AuctionID,
		WinnerID:  "u_1002",
		SellerID:  "u_2001",
		DealPrice: 900,
		Status:    domain.OrderStatusCreated,
		PayStatus: domain.PayStatusUnpaid,
		CreatedAt: start.Add(30 * time.Minute),
		UpdatedAt: start.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("create unpaid order: %v", err)
	}
	for _, bid := range []domain.BidRecord{
		{RequestID: "dashboard-bid-1", AuctionID: won.AuctionID, LiveSessionID: &session.ID, BidderID: "u_1001", BidPrice: 1200, BidTSMS: start.Add(15 * time.Minute).UnixMilli(), Source: "ws", RiskResult: domain.BidRiskAllow, CreatedAt: start.Add(15 * time.Minute)},
		{RequestID: "dashboard-bid-2", AuctionID: won.AuctionID, LiveSessionID: &session.ID, BidderID: "u_1002", BidPrice: 1500, BidTSMS: start.Add(25 * time.Minute).UnixMilli(), Source: "ws", RiskResult: domain.BidRiskAllow, CreatedAt: start.Add(25 * time.Minute)},
	} {
		bid := bid
		if err := bidRepo.Create(ctx, &bid); err != nil {
			t.Fatalf("create bid: %v", err)
		}
	}
	if err := riskRepo.CreateEvent(ctx, &domain.RiskEvent{EventType: "BID_FREQ", UserID: "u_1001", AuctionID: won.AuctionID, Severity: domain.RiskSeverityMid, Status: domain.RiskEventPending, CreatedAt: start.Add(40 * time.Minute)}); err != nil {
		t.Fatalf("create pending risk event: %v", err)
	}
	if err := riskRepo.CreateEvent(ctx, &domain.RiskEvent{EventType: "ABUSE_RETRY", UserID: "u_1002", AuctionID: failed.AuctionID, Severity: domain.RiskSeverityLow, Status: domain.RiskEventReviewed, CreatedAt: start.Add(50 * time.Minute)}); err != nil {
		t.Fatalf("create reviewed risk event: %v", err)
	}

	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:        userRepo,
		AuctionRepo:     auctionRepo,
		LiveRoomRepo:    roomRepo,
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		RiskRepo:        riskRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  service.DisabledProductAuditor{},
	})
	adminToken := loginForToken(t, h.Engine, "admin001", "AdminPassw0rd!", "admin")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")
	values := url.Values{}
	values.Set("startTime", start.Format(time.RFC3339))
	values.Set("endTime", end.Format(time.RFC3339))
	values.Set("bucket", "hour")
	path := "/api/v1/admin/dashboard/metrics?" + values.Encode()

	forbidden := doJSONWithHeaders(t, h.Engine, consts.MethodGet, path, "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if forbidden.status != 403 || forbidden.body.Code != 10003 {
		t.Fatalf("expected buyer forbidden, got status=%d raw=%s", forbidden.status, forbidden.raw)
	}
	resp := doJSONWithHeaders(t, h.Engine, consts.MethodGet, path, "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected dashboard metrics success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var data domain.AdminDashboardMetrics
	mustDecodeData(t, resp.body.Data, &data)
	if data.Summary.DealGMVCent != 1500 || data.Summary.PaidGMVCent != 1500 || data.Summary.PaidOrderCount != 1 {
		t.Fatalf("unexpected money/order summary: %+v", data.Summary)
	}
	if data.Summary.OrderCreatedCount != 2 || data.Summary.UnpaidOrderCount != 1 || data.Summary.AuctionCreatedCount != 3 {
		t.Fatalf("unexpected created summary: %+v", data.Summary)
	}
	if data.Summary.ClosedWonAuctionCount != 1 || data.Summary.ClosedFailedAuctionCount != 1 {
		t.Fatalf("unexpected closed auction summary: %+v", data.Summary)
	}
	if data.Summary.BidCount != 2 || data.Summary.ActiveBidderCount != 2 || data.Summary.RiskEventCount != 2 {
		t.Fatalf("unexpected bid/risk summary: %+v", data.Summary)
	}
	if data.Current.LiveRoomLiveCount != 1 || data.Current.ActiveLiveSessionCount != 1 || data.Current.RunningAuctionCount != 1 || data.Current.PendingRiskEventCount != 1 {
		t.Fatalf("unexpected current stats: %+v", data.Current)
	}
	if !hasStatusCount(data.Breakdowns.OrderStatus, string(domain.OrderStatusPaid), 1) || !hasStatusCount(data.Breakdowns.PayStatus, string(domain.PayStatusUnpaid), 1) {
		t.Fatalf("unexpected breakdowns: %+v", data.Breakdowns)
	}
	if len(data.Trend) != 3 || data.Trend[0].DealGMVCent != 1500 || data.Trend[1].PaidGMVCent != 1500 {
		t.Fatalf("unexpected trend: %+v", data.Trend)
	}
}

func newTestServer() *server.Hertz {
	return NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: service.DisabledProductAuditor{},
	})
}

type captureProductDescriptionGenerator struct {
	input     service.ProductDescriptionInput
	imageBody string
	result    service.ProductDescriptionResult
	err       error
}

func (g *captureProductDescriptionGenerator) GenerateProductDescription(ctx context.Context, in service.ProductDescriptionInput) (service.ProductDescriptionResult, error) {
	_ = ctx
	g.input = in
	g.imageBody = string(in.Image)
	if g.err != nil {
		return service.ProductDescriptionResult{}, g.err
	}
	return g.result, nil
}

type captureProductAuditor struct {
	mu     sync.Mutex
	input  service.ProductAuditInput
	result service.ProductAuditResult
	err    error
	called chan struct{}
}

func (a *captureProductAuditor) AuditProduct(ctx context.Context, in service.ProductAuditInput) (service.ProductAuditResult, error) {
	_ = ctx
	a.mu.Lock()
	a.input = in
	if a.called != nil {
		select {
		case a.called <- struct{}{}:
		default:
		}
	}
	a.mu.Unlock()
	if a.err != nil {
		return service.ProductAuditResult{}, a.err
	}
	return a.result, nil
}

func (a *captureProductAuditor) waitInput(t *testing.T) service.ProductAuditInput {
	t.Helper()
	if a.called != nil {
		select {
		case <-a.called:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for product audit hook")
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.input
}

type captureLiveAnalysisRequester struct {
	mu     sync.Mutex
	input  service.LiveAnalysisAsyncInput
	result service.LiveAnalysisAsyncResult
	err    error
}

func (r *captureLiveAnalysisRequester) RequestLiveAnalysis(ctx context.Context, in service.LiveAnalysisAsyncInput) (service.LiveAnalysisAsyncResult, error) {
	_ = ctx
	r.mu.Lock()
	r.input = in
	result := r.result
	err := r.err
	r.mu.Unlock()
	if err != nil {
		return service.LiveAnalysisAsyncResult{}, err
	}
	return result, nil
}

func (r *captureLiveAnalysisRequester) prompt() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.input.Prompt
}

func pollLiveAnalysisTask(t *testing.T, engine *route.Engine, liveSessionID uint64, token string) service.LiveAnalysisTask {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last service.LiveAnalysisTask
	for time.Now().Before(deadline) {
		last = getLiveAnalysisTask(t, engine, liveSessionID, token)
		if last.Status == service.LiveAnalysisTaskSucceeded || last.Status == service.LiveAnalysisTaskFailed {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task did not finish: %+v", last)
	return service.LiveAnalysisTask{}
}

func pollItemStatus(t *testing.T, engine *route.Engine, itemID uint64, token string, want string) string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp := doJSONWithHeaders(t, engine, consts.MethodGet, "/api/v1/items/"+strconv.FormatUint(itemID, 10), "",
			ut.Header{Key: "Authorization", Value: "Bearer " + token},
		)
		if resp.status != 200 || resp.body.Code != 0 {
			t.Fatalf("expected item get success, got status=%d raw=%s", resp.status, resp.raw)
		}
		var item struct {
			Status string `json:"status"`
		}
		mustDecodeData(t, resp.body.Data, &item)
		last = item.Status
		if last == want {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last
}

func productAuditCallbackJSON(t *testing.T, success, approved bool, callbackContext map[string]interface{}) string {
	t.Helper()
	payload, err := json.Marshal(map[string]interface{}{
		"success":          success,
		"is_approved":      approved,
		"callback_context": callbackContext,
	})
	if err != nil {
		t.Fatalf("marshal product audit callback: %v", err)
	}
	return string(payload)
}

func getLiveAnalysisTask(t *testing.T, engine *route.Engine, liveSessionID uint64, token string) service.LiveAnalysisTask {
	t.Helper()
	resp := doJSONWithHeaders(t, engine, consts.MethodGet, "/api/v1/live-analysis/reports/"+strconv.FormatUint(liveSessionID, 10), "",
		ut.Header{Key: "Authorization", Value: "Bearer " + token},
	)
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected poll success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var task service.LiveAnalysisTask
	mustDecodeData(t, resp.body.Data, &task)
	return task
}

func loginForToken(t *testing.T, engine *route.Engine, account, password, role string) string {
	t.Helper()
	login := doJSON(t, engine, consts.MethodPost, "/api/v1/auth/login", `{"account":"`+account+`","password":"`+password+`","role":"`+role+`"}`)
	if login.status != 200 || login.body.Code != 0 {
		t.Fatalf("login failed status=%d raw=%s", login.status, login.raw)
	}
	var data struct {
		AccessToken string `json:"accessToken"`
	}
	mustDecodeData(t, login.body.Data, &data)
	if data.AccessToken == "" {
		t.Fatal("expected access token")
	}
	return data.AccessToken
}

type testHTTPResult struct {
	status int
	body   apiResponse
	raw    string
}

func doJSON(t *testing.T, engine *route.Engine, method, path, body string) testHTTPResult {
	t.Helper()
	return doJSONWithHeaders(t, engine, method, path, body)
}

func doJSONWithHeaders(t *testing.T, engine *route.Engine, method, path, body string, headers ...ut.Header) testHTTPResult {
	t.Helper()
	if body != "" {
		headers = append(headers, ut.Header{Key: "Content-Type", Value: "application/json; charset=utf-8"})
	}
	var requestBody *ut.Body
	if body != "" {
		requestBody = &ut.Body{Body: strings.NewReader(body), Len: len(body)}
	}
	resp := ut.PerformRequest(engine, method, path, requestBody, headers...)
	var decoded apiResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v raw=%s", err, resp.Body.String())
	}
	return testHTTPResult{status: resp.Code, body: decoded, raw: resp.Body.String()}
}

type mcpRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	} `json:"error"`
}

type testMCPResult struct {
	status int
	body   mcpRPCResponse
	raw    string
}

type mcpToolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"isError"`
}

func decodeMCPToolEnvelope[T any](t *testing.T, resp testMCPResult) struct {
	SchemaVersion string `json:"schemaVersion"`
	TraceID       string `json:"traceId"`
	Data          T      `json:"data"`
} {
	t.Helper()
	var toolResult mcpToolResult
	mustDecodeData(t, resp.body.Result, &toolResult)
	if len(toolResult.Content) != 1 {
		t.Fatalf("expected one tool content item, got %+v", toolResult)
	}
	var payload struct {
		SchemaVersion string `json:"schemaVersion"`
		TraceID       string `json:"traceId"`
		Data          T      `json:"data"`
	}
	if err := json.Unmarshal([]byte(toolResult.Content[0].Text), &payload); err != nil {
		t.Fatalf("decode MCP tool payload: %v text=%s", err, toolResult.Content[0].Text)
	}
	return payload
}

func doMCP(t *testing.T, engine *route.Engine, apiKey, body string) testMCPResult {
	t.Helper()
	return doMCPPath(t, engine, "/mcp/read", apiKey, body)
}

func doMCPPath(t *testing.T, engine *route.Engine, path, apiKey, body string) testMCPResult {
	t.Helper()
	headers := []ut.Header{{Key: "Content-Type", Value: "application/json; charset=utf-8"}}
	if apiKey != "" {
		headers = append(headers, ut.Header{Key: "X-API-Key", Value: apiKey})
	}
	resp := ut.PerformRequest(engine, consts.MethodPost, path, &ut.Body{Body: strings.NewReader(body), Len: len(body)}, headers...)
	var decoded mcpRPCResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode mcp response: %v raw=%s", err, resp.Body.String())
	}
	return testMCPResult{status: resp.Code, body: decoded, raw: resp.Body.String()}
}

func containsTool(tools []struct {
	Name string `json:"name"`
}, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func ptrInt64(v int64) *int64 {
	return &v
}

func ptrString(v string) *string {
	return &v
}

func hasStatusCount(items []domain.AdminStatusCount, status string, count int64) bool {
	for _, item := range items {
		if item.Status == status && item.Count == count {
			return true
		}
	}
	return false
}

type multipartTestFile struct {
	FieldName string
	Filename  string
	Body      string
}

func doMultipartWithHeaders(t *testing.T, engine *route.Engine, method, path string, fields map[string]string, files []multipartTestFile, headers ...ut.Header) testHTTPResult {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatalf("write multipart field: %v", err)
		}
	}
	for _, file := range files {
		part, err := writer.CreateFormFile(file.FieldName, file.Filename)
		if err != nil {
			t.Fatalf("create multipart file: %v", err)
		}
		if _, err := part.Write([]byte(file.Body)); err != nil {
			t.Fatalf("write multipart file: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	headers = append(headers, ut.Header{Key: "Content-Type", Value: writer.FormDataContentType()})
	requestBody := &ut.Body{Body: bytes.NewReader(body.Bytes()), Len: body.Len()}
	resp := ut.PerformRequest(engine, method, path, requestBody, headers...)
	var decoded apiResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v raw=%s", err, resp.Body.String())
	}
	return testHTTPResult{status: resp.Code, body: decoded, raw: resp.Body.String()}
}

func mustDecodeData(t *testing.T, data json.RawMessage, out interface{}) {
	t.Helper()
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("decode data: %v raw=%s", err, string(data))
	}
}
