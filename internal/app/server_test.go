package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	aiapp "aieas_backend/internal/modules/ai/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	liveanalysisapp "aieas_backend/internal/modules/live_analysis/app"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	"aieas_backend/internal/tests/repository"
	httptransport "aieas_backend/internal/transport/http"
	wstransport "aieas_backend/internal/transport/ws"

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

type appDisabledProductAuditor struct{}

func (appDisabledProductAuditor) AuditProduct(ctx context.Context, in auctionports.ProductAuditInput) (auctionports.ProductAuditResult, error) {
	_ = ctx
	_ = in
	return auctionports.ProductAuditResult{}, nil
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

func TestAppRoleRouteRegistration(t *testing.T) {
	tests := []struct {
		role       string
		wantAPI    int
		wantAPIGET int
		wantWS     int
		wantMCP    int
		wantPing   int
		wantReady  int
	}{
		{role: "all", wantAPI: 200, wantAPIGET: 401, wantWS: 401, wantMCP: 405, wantPing: 200, wantReady: 200},
		{role: "api", wantAPI: 200, wantAPIGET: 401, wantWS: 404, wantMCP: 405, wantPing: 200, wantReady: 200},
		{role: "ws-gateway", wantAPI: 404, wantAPIGET: 404, wantWS: 401, wantMCP: 404, wantPing: 200, wantReady: 200},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			cfg := appconfig.Default()
			cfg.App.Role = tt.role
			h := NewServerWithDependencies(cfg, ServerDependencies{
				UserRepo:       repository.NewSeedUserRepository(),
				ObjectUploader: objectstorage.NewMemoryUploader(""),
				ProductAuditor: appDisabledProductAuditor{},
			})

			loginBody := `{"account":"buyer001","password":"Passw0rd!","role":"buyer"}`
			apiResp := ut.PerformRequest(h.Engine, consts.MethodPost, "/api/v1/auth/login", &ut.Body{Body: strings.NewReader(loginBody), Len: len(loginBody)}, ut.Header{Key: "Content-Type", Value: "application/json"})
			if apiResp.Code != tt.wantAPI {
				t.Fatalf("api status=%d want %d raw=%s", apiResp.Code, tt.wantAPI, apiResp.Body.String())
			}
			apiGetResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/api/v1/auth/me", nil)
			if apiGetResp.Code != tt.wantAPIGET {
				t.Fatalf("api get status=%d want %d raw=%s", apiGetResp.Code, tt.wantAPIGET, apiGetResp.Body.String())
			}
			wsResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/auctions/1", nil)
			if wsResp.Code != tt.wantWS {
				t.Fatalf("ws status=%d want %d body=%s", wsResp.Code, tt.wantWS, wsResp.Body.String())
			}
			wsLiveRoomResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/live-rooms/1", nil)
			if wsLiveRoomResp.Code != tt.wantWS {
				t.Fatalf("ws live-room status=%d want %d body=%s", wsLiveRoomResp.Code, tt.wantWS, wsLiveRoomResp.Body.String())
			}
			mcpResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/mcp/read", nil)
			if mcpResp.Code != tt.wantMCP {
				t.Fatalf("mcp status=%d want %d body=%s", mcpResp.Code, tt.wantMCP, mcpResp.Body.String())
			}
			pingResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ping", nil)
			if pingResp.Code != tt.wantPing {
				t.Fatalf("ping status=%d want %d", pingResp.Code, tt.wantPing)
			}
			readyResp := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
			if readyResp.Code != tt.wantReady {
				t.Fatalf("readyz status=%d want %d body=%s", readyResp.Code, tt.wantReady, readyResp.Body.String())
			}
		})
	}
}

func TestAppRoleWorkerDecisions(t *testing.T) {
	tests := []struct {
		role                string
		wantAPI             bool
		wantWS              bool
		wantBusinessWorkers bool
		wantWSConsumers     bool
	}{
		{role: "all", wantAPI: true, wantWS: true, wantBusinessWorkers: true, wantWSConsumers: true},
		{role: "api", wantAPI: true, wantWS: false, wantBusinessWorkers: true, wantWSConsumers: false},
		{role: "ws-gateway", wantAPI: false, wantWS: true, wantBusinessWorkers: false, wantWSConsumers: true},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			cfg := appconfig.Default()
			cfg.App.Role = tt.role
			if got := shouldRegisterAPIRoutes(cfg); got != tt.wantAPI {
				t.Fatalf("shouldRegisterAPIRoutes=%v want %v", got, tt.wantAPI)
			}
			if got := shouldRegisterWSRoutes(cfg); got != tt.wantWS {
				t.Fatalf("shouldRegisterWSRoutes=%v want %v", got, tt.wantWS)
			}
			if got := shouldStartBusinessWorkers(cfg); got != tt.wantBusinessWorkers {
				t.Fatalf("shouldStartBusinessWorkers=%v want %v", got, tt.wantBusinessWorkers)
			}
			if got := shouldStartWSConsumers(cfg); got != tt.wantWSConsumers {
				t.Fatalf("shouldStartWSConsumers=%v want %v", got, tt.wantWSConsumers)
			}
		})
	}
}

func TestReadyzFailsAfterHubBeginsDraining(t *testing.T) {
	cfg := appconfig.Default()
	cfg.App.Role = "ws-gateway"
	hub := wstransport.NewHub()
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		Hub:            hub,
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: appDisabledProductAuditor{},
	})

	ready := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if ready.Code != 200 {
		t.Fatalf("readyz before drain status=%d body=%s", ready.Code, ready.Body.String())
	}
	hub.BeginDrain(5000)
	ready = ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if ready.Code != consts.StatusServiceUnavailable {
		t.Fatalf("readyz after drain status=%d want 503 body=%s", ready.Code, ready.Body.String())
	}
}

func TestReadinessProbeRoleFiltering(t *testing.T) {
	tests := []struct {
		role       string
		wantStatus int
		wantMySQL  bool
	}{
		{role: "all", wantStatus: consts.StatusServiceUnavailable, wantMySQL: true},
		{role: "api", wantStatus: consts.StatusServiceUnavailable, wantMySQL: true},
		{role: "ws-gateway", wantStatus: consts.StatusOK, wantMySQL: false},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			calls := make(map[string]int)
			probe := func(name string, err error) httptransport.ReadinessProbe {
				return func(ctx context.Context) error {
					_ = ctx
					calls[name]++
					return err
				}
			}

			cfg := appconfig.Default()
			cfg.App.Role = tt.role
			h := NewServerWithDependencies(cfg, ServerDependencies{
				UserRepo:       repository.NewSeedUserRepository(),
				ObjectUploader: objectstorage.NewMemoryUploader(""),
				ProductAuditor: appDisabledProductAuditor{},
				ReadinessProbes: map[string]httptransport.ReadinessProbe{
					"mysql":         probe("mysql", errors.New("mysql down")),
					"business_x":    probe("business_x", errors.New("business down")),
					"kafka":         probe("kafka", errors.New("kafka down")),
					"redis_rt":      probe("redis_rt", nil),
					"redis_cache":   probe("redis_cache", nil),
					"scripts":       probe("scripts", nil),
					"redis_pubsub":  probe("redis_pubsub", nil),
					"redis_pub_sub": probe("redis_pub_sub", nil),
					"bid_stream":    probe("bid_stream", nil),
				},
			})
			ready := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
			if ready.Code != tt.wantStatus {
				t.Fatalf("readyz status=%d want %d body=%s", ready.Code, tt.wantStatus, ready.Body.String())
			}
			var body struct {
				Components map[string]string `json:"components"`
			}
			if err := json.Unmarshal(ready.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode readyz body: %v raw=%s", err, ready.Body.String())
			}
			_, hasMySQL := body.Components["mysql"]
			if hasMySQL != tt.wantMySQL {
				t.Fatalf("mysql component present=%v want %v components=%v", hasMySQL, tt.wantMySQL, body.Components)
			}
			if tt.wantMySQL && calls["mysql"] != 1 {
				t.Fatalf("mysql probe calls=%d want 1", calls["mysql"])
			}
			if tt.role == "ws-gateway" {
				for _, name := range []string{"mysql", "business_x", "kafka"} {
					if _, ok := body.Components[name]; ok {
						t.Fatalf("%s probe should be filtered in ws-gateway: %v", name, body.Components)
					}
					if calls[name] != 0 {
						t.Fatalf("%s probe calls=%d want 0", name, calls[name])
					}
				}
				for _, name := range []string{"redis_rt", "redis_cache", "scripts", "redis_pubsub", "redis_pub_sub", "bid_stream"} {
					if body.Components[name] != "ok" {
						t.Fatalf("ws-gateway should keep %s probe, components=%v", name, body.Components)
					}
					if calls[name] != 1 {
						t.Fatalf("%s probe calls=%d want 1", name, calls[name])
					}
				}
				if body.Components["ws_draining"] != "ok" {
					t.Fatalf("ws-gateway should keep ws_draining probe, components=%v", body.Components)
				}
			}
		})
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

func TestAuthProfileAndAvatarUpdate(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	patch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auth/me", `{"nickname":"  新昵称001  "}`, ut.Header{Key: "Authorization", Value: "Bearer " + token}, ut.Header{Key: "Idempotency-Key", Value: "profile-update-1"})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected profile update success, got status=%d raw=%s", patch.status, patch.raw)
	}
	var profile struct {
		ID        string `json:"id"`
		Nickname  string `json:"nickname"`
		AvatarURL string `json:"avatarUrl"`
	}
	mustDecodeData(t, patch.body.Data, &profile)
	if profile.ID != "u_1001" || profile.Nickname != "新昵称001" || profile.AvatarURL != "" {
		t.Fatalf("unexpected profile update payload: %+v", profile)
	}

	upload := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auth/me/avatar", nil, []multipartTestFile{
		{FieldName: "avatar", Filename: "avatar.jpg", Body: "buyer avatar bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token}, ut.Header{Key: "Idempotency-Key", Value: "avatar-upload-1"})
	if upload.status != 200 || upload.body.Code != 0 {
		t.Fatalf("expected avatar upload success, got status=%d raw=%s", upload.status, upload.raw)
	}
	mustDecodeData(t, upload.body.Data, &profile)
	if profile.Nickname != "新昵称001" || !strings.HasPrefix(profile.AvatarURL, "/api/v1/images/") {
		t.Fatalf("unexpected avatar update payload: %+v", profile)
	}
	imageResp := ut.PerformRequest(h.Engine, consts.MethodGet, profile.AvatarURL, nil)
	if imageResp.Code != 200 || imageResp.Body.String() != "buyer avatar bytes" {
		t.Fatalf("expected avatar image proxy success, got status=%d body=%q", imageResp.Code, imageResp.Body.String())
	}

	me := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auth/me", "", ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if me.status != 200 || me.body.Code != 0 {
		t.Fatalf("expected me after avatar success, got status=%d raw=%s", me.status, me.raw)
	}
	mustDecodeData(t, me.body.Data, &profile)
	if profile.Nickname != "新昵称001" || !strings.HasPrefix(profile.AvatarURL, "/api/v1/images/") {
		t.Fatalf("expected me to include updated profile, got %+v", profile)
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
		Tools []mcpListedTool `json:"tools"`
	}
	mustDecodeData(t, readToolsResp.body.Result, &toolsResult)
	if !containsTool(toolsResult.Tools, "get_current_time") ||
		!containsTool(toolsResult.Tools, "read_live_session_bids") ||
		!containsTool(toolsResult.Tools, "read_live_session_settlement") ||
		containsTool(toolsResult.Tools, "read_item") ||
		containsTool(toolsResult.Tools, "read_items") ||
		containsTool(toolsResult.Tools, "read_live_room") ||
		containsTool(toolsResult.Tools, "read_live_rooms") ||
		containsTool(toolsResult.Tools, "get_merchant_live_control_context") ||
		containsTool(toolsResult.Tools, "operate_live_session_lot") ||
		containsTool(toolsResult.Tools, "live_voice_broadcast") {
		t.Fatalf("expected only read tools from read MCP, got %+v", toolsResult.Tools)
	}
	readTimeResp := doMCPPath(t, h.Engine, "/mcp/read", readAPIKey, `{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"get_current_time","arguments":{}}}`)
	if readTimeResp.status != 200 || readTimeResp.body.Error != nil {
		t.Fatalf("expected read current time success, status=%d raw=%s", readTimeResp.status, readTimeResp.raw)
	}
	readTime := decodeMCPToolEnvelope[mcpapp.CurrentTimeResult](t, readTimeResp)
	if readTime.Data.CurrentTime == "" || readTime.Data.Timezone != "UTC" || readTime.Data.UnixSeconds <= 0 || readTime.Data.UnixMilliseconds <= 0 {
		t.Fatalf("unexpected read current time result: %+v", readTime.Data)
	}
	if _, err := time.Parse(time.RFC3339Nano, readTime.Data.CurrentTime); err != nil {
		t.Fatalf("current time is not RFC3339Nano: %v", err)
	}
	readResourcesResp := doMCPPath(t, h.Engine, "/mcp/read", readAPIKey, `{"jsonrpc":"2.0","id":21,"method":"resources/templates/list","params":{}}`)
	if readResourcesResp.status != 200 || readResourcesResp.body.Error != nil {
		t.Fatalf("expected read resources/templates/list success, status=%d raw=%s", readResourcesResp.status, readResourcesResp.raw)
	}
	var resourcesResult struct {
		ResourceTemplates []struct {
			URITemplate string `json:"uriTemplate"`
			Name        string `json:"name"`
		} `json:"resourceTemplates"`
	}
	mustDecodeData(t, readResourcesResp.body.Result, &resourcesResult)
	for _, resource := range resourcesResult.ResourceTemplates {
		if strings.Contains(resource.URITemplate, "items") ||
			strings.Contains(resource.URITemplate, "live-rooms") ||
			strings.Contains(resource.Name, "item") ||
			strings.Contains(resource.Name, "live-room") {
			t.Fatalf("MCP resource template should use auction-lot/live-session dimensions only, got %+v", resource)
		}
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
		!containsTool(toolsResult.Tools, "get_current_time") ||
		!containsTool(toolsResult.Tools, "get_merchant_live_control_context") ||
		!containsTool(toolsResult.Tools, "operate_live_session_lot") ||
		!containsTool(toolsResult.Tools, "live_voice_broadcast") ||
		len(toolsResult.Tools) != 4 {
		t.Fatalf("expected only control tools from control MCP, got %+v", toolsResult.Tools)
	}
	operateTool, ok := findMCPTool(toolsResult.Tools, "operate_live_session_lot")
	if !ok {
		t.Fatalf("operate_live_session_lot tool not found: %+v", toolsResult.Tools)
	}
	if strings.Contains(operateTool.Description, "上架") || strings.Contains(operateTool.Description, "讲解") {
		t.Fatalf("operate_live_session_lot description should not expose onShelf or explain wording: %q", operateTool.Description)
	}
	actionSchema, ok := operateTool.InputSchema["properties"].(map[string]interface{})["action"].(map[string]interface{})
	if !ok {
		t.Fatalf("operate_live_session_lot action schema missing: %+v", operateTool.InputSchema)
	}
	for _, value := range actionSchema["enum"].([]interface{}) {
		if value == "onShelf" {
			t.Fatalf("operate_live_session_lot action enum should not include onShelf: %+v", actionSchema["enum"])
		}
	}
	controlTimeResp := doMCPPath(t, h.Engine, "/mcp/control", controlAPIKey, `{"jsonrpc":"2.0","id":23,"method":"tools/call","params":{"name":"get_current_time","arguments":{}}}`)
	if controlTimeResp.status != 200 || controlTimeResp.body.Error != nil {
		t.Fatalf("expected control current time success, status=%d raw=%s", controlTimeResp.status, controlTimeResp.raw)
	}
	controlTime := decodeMCPToolEnvelope[mcpapp.CurrentTimeResult](t, controlTimeResp)
	if controlTime.Data.CurrentTime == "" || controlTime.Data.Timezone != "UTC" || controlTime.Data.UnixSeconds <= 0 || controlTime.Data.UnixMilliseconds <= 0 {
		t.Fatalf("unexpected control current time result: %+v", controlTime.Data)
	}
}

func TestAIAssistantHubNotifierBroadcastsSwitchEvent(t *testing.T) {
	hub := wstransport.NewHub()
	const sessionID uint64 = 90001
	client := wstransport.NewClientWithSession("buyer-1", "u_1001", 0, sessionID, 4)
	if err := hub.SubscribeLiveSessionOnly(sessionID, client); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	select {
	case <-client.Outbound():
	default:
	}
	enabled := true
	delivered, err := (aiAssistantHubNotifier{hub: hub}).NotifyAIAssistantEvent(context.Background(), sessionID, aiapp.Event{
		Kind:          "switch",
		Status:        "enabled",
		MerchantID:    "u_2001",
		LiveSessionID: sessionID,
		Enabled:       &enabled,
		VideoSource:   "digitalHuman",
		LiveRoom: map[string]interface{}{
			"id":                 sessionID,
			"liveSessionId":      sessionID,
			"merchantId":         "u_2001",
			"videoSource":        "digitalHuman",
			"aiAssistantEnabled": true,
		},
		Message: "直播场次90001AI直播助手已开启",
	})
	if err != nil {
		t.Fatalf("notify switch: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("expected one delivered switch event, got %d", delivered)
	}
	select {
	case env := <-client.Outbound():
		if env.Type != wstransport.TypeAIAssistantSwitch || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected switch envelope: %+v", env)
		}
		var payload aiapp.Event
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.Enabled == nil || !*payload.Enabled || payload.Kind != "switch" {
			t.Fatalf("unexpected switch payload: %+v", payload)
		}
		if payload.VideoSource != "digitalHuman" || payload.LiveRoom["videoSource"] != "digitalHuman" || payload.LiveRoom["aiAssistantEnabled"] != true {
			t.Fatalf("expected switch payload to include live room playback mode, got %+v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for switch event")
	}
}

func TestLiveSessionLotHubNotifierBroadcastsLotListEvents(t *testing.T) {
	hub := wstransport.NewHub()
	const sessionID uint64 = 90003
	const auctionID uint64 = 91003
	buyer := wstransport.NewClientWithSession("buyer-lot-event", "u_1001", 0, sessionID, 8)
	if err := hub.SubscribeLiveSessionOnly(sessionID, buyer); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainAppOutbound(buyer)

	notifier := liveSessionLotHubNotifier{hub: hub}
	if delivered := notifier.NotifyLotMounted(context.Background(), "u_2001", sessionID, auctionID); delivered != 1 {
		t.Fatalf("expected one mounted event delivered, got %d", delivered)
	}
	assertLiveSessionLotEvent(t, buyer, wstransport.TypeLiveSessionLotMounted, sessionID, auctionID, "mounted")

	if delivered := notifier.NotifyLotUnmounted(context.Background(), "u_2001", sessionID, auctionID); delivered != 1 {
		t.Fatalf("expected one unmounted event delivered, got %d", delivered)
	}
	assertLiveSessionLotEvent(t, buyer, wstransport.TypeLiveSessionLotUnmounted, sessionID, auctionID, "unmounted")

	if delivered := notifier.NotifyLotChanged(context.Background(), "u_2001", sessionID, auctionID, "scheduled"); delivered != 1 {
		t.Fatalf("expected one changed event delivered, got %d", delivered)
	}
	assertLiveSessionLotEvent(t, buyer, wstransport.TypeLiveSessionLotChanged, sessionID, auctionID, "scheduled")
}

func TestAuctionEventPublisherAdapterBroadcastsLifecycleEventsToLiveSession(t *testing.T) {
	hub := wstransport.NewHub()
	const sessionID uint64 = 90004
	const auctionID uint64 = 91004
	sessionClient := wstransport.NewClientWithSession("buyer-lifecycle-session", "u_1001", 0, sessionID, 8)
	if err := hub.SubscribeLiveSessionOnly(sessionID, sessionClient); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainAppOutbound(sessionClient)

	payload, _ := json.Marshal(map[string]interface{}{
		"auctionId":     auctionID,
		"liveSessionId": sessionID,
		"state": map[string]interface{}{
			"auctionId":    auctionID,
			"status":       "RUNNING",
			"currentPrice": 1000,
			"endTime":      time.Now().UTC().Add(time.Minute),
		},
	})
	adapter := auctionEventPublisherAdapter{publisher: hub}
	if delivered := adapter.Broadcast(auctionID, auctionports.EventEnvelope{Type: "auction.started", Payload: payload}); delivered != 1 {
		t.Fatalf("expected one lifecycle event delivered to live session, got %d", delivered)
	}

	select {
	case env := <-sessionClient.Outbound():
		if env.Type != "auction.started" || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected lifecycle envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle event")
	}

	changedPayload, _ := json.Marshal(map[string]interface{}{
		"auctionId":     auctionID,
		"liveSessionId": sessionID,
		"merchantId":    "u_2001",
		"action":        "updated",
	})
	if delivered := adapter.Broadcast(auctionID, auctionports.EventEnvelope{Type: wstransport.TypeLiveSessionLotChanged, Payload: changedPayload}); delivered != 1 {
		t.Fatalf("expected one lot changed event delivered to live session, got %d", delivered)
	}
	select {
	case env := <-sessionClient.Outbound():
		if env.Type != wstransport.TypeLiveSessionLotChanged || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected lot changed envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lot changed event")
	}
}

func TestLiveVoiceHubBroadcasterDeliversOnlyOnlineClients(t *testing.T) {
	hub := wstransport.NewHub()
	const sessionID uint64 = 90002
	buyer := wstransport.NewClientWithSession("buyer-voice", "u_1001", 0, sessionID, 4)
	merchant := wstransport.NewClientWithSession("merchant-voice", "u_2001", 0, sessionID, 4)
	admin := wstransport.NewClientWithSession("admin-voice", "u_admin", 0, sessionID, 4)
	merchant.CountOnline = false
	admin.CountOnline = false
	for _, client := range []*wstransport.Client{buyer, merchant, admin} {
		if err := hub.SubscribeLiveSessionOnly(sessionID, client); err != nil {
			t.Fatalf("subscribe %s: %v", client.ID, err)
		}
	}
	drainAppOutbound(buyer, merchant, admin)

	delivered, err := (liveVoiceHubBroadcaster{hub: hub}).BroadcastLiveVoice(context.Background(), sessionID, mcpapp.LiveVoiceBroadcastPayload{
		RequestID:   "voice-online-only",
		AudioBase64: "AQI=",
		AudioFormat: "pcm_s16le",
		SampleRate:  24000,
		Channels:    1,
	})
	if err != nil {
		t.Fatalf("broadcast live voice: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("expected one online client delivered, got %d", delivered)
	}
	select {
	case env := <-buyer.Outbound():
		if env.Type != wstransport.TypeLiveVoiceBroadcast || env.LiveSessionID != sessionID || env.RequestID != "voice-online-only" {
			t.Fatalf("unexpected buyer voice envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for buyer voice envelope")
	}
	for _, client := range []*wstransport.Client{merchant, admin} {
		select {
		case env := <-client.Outbound():
			t.Fatalf("%s should not receive user voice broadcast: %+v", client.ID, env)
		default:
		}
	}
}

func assertLiveSessionLotEvent(t *testing.T, client *wstransport.Client, eventType string, sessionID, auctionID uint64, action string) {
	t.Helper()
	select {
	case env := <-client.Outbound():
		if env.Type != eventType || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected live session lot envelope: %+v", env)
		}
		var payload struct {
			LiveSessionID uint64 `json:"liveSessionId"`
			AuctionID     uint64 `json:"auctionId"`
			MerchantID    string `json:"merchantId"`
			Action        string `json:"action"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.LiveSessionID != sessionID || payload.AuctionID != auctionID || payload.MerchantID != "u_2001" || payload.Action != action {
			t.Fatalf("unexpected live session lot payload: %+v", payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s event", eventType)
	}
}

func TestMCPReadLiveSessionBidsAuthorization(t *testing.T) {
	ctx := context.Background()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	orderRepo := repository.NewMemoryOrderRepository()

	closedAt := time.Now().UTC()
	openedAt := closedAt.Add(-time.Hour)
	session := domain.LiveSession{MerchantID: "u_2001", Title: "春拍专场", Status: domain.LiveSessionStatusEnded, OpenedAt: &openedAt, ClosedAt: &closedAt, LotsTotal: 1, LotsSold: 1, BidCount: 1, GMVCent: 120000}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	lot := domain.AuctionLot{
		AuctionID:      10001,
		SellerID:       "u_2001",
		Title:          "Vintage Camera",
		Category:       "camera",
		ConditionGrade: domain.ConditionGood,
		LiveSessionID:  &session.ID,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     100000,
		ReservePrice:   100000,
		CapPrice:       200000,
		IncrementRule:  domain.DefaultIncrementRule(),
		RuleSnapshot:   json.RawMessage(`{}`),
		Status:         domain.AuctionStatusClosedWon,
		StartTime:      closedAt.Add(-time.Hour),
		EndTime:        closedAt,
		DealPrice:      ptrInt64(120000),
		WinnerID:       ptrString("u_1001"),
		ClosedAt:       &closedAt,
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
		AuctionRepo:     auctionRepo,
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  appDisabledProductAuditor{},
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
			Items []struct {
				BidPrice mcpDisplayMoney `json:"bidPrice"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(toolResult.Content[0].Text), &payload); err != nil {
		t.Fatalf("decode tool payload: %v text=%s", err, toolResult.Content[0].Text)
	}
	if len(payload.Data.Items) != 1 {
		t.Fatalf("unexpected bid payload: %+v", payload.Data.Items)
	}
	assertMCPDisplayMoney(t, payload.Data.Items[0].BidPrice, "1200.00")

	buyerCfg := appconfig.Default()
	buyerCfg.MCP.Read.APIKey = "buyer-mcp-key"
	buyerCfg.MCP.Read.ActorID = "u_1001"
	buyerCfg.MCP.Read.ActorRole = "buyer"
	buyerServer := NewServerWithDependencies(buyerCfg, ServerDependencies{
		UserRepo:        userRepo,
		AuctionRepo:     auctionRepo,
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  appDisabledProductAuditor{},
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

func drainAppOutbound(clients ...*wstransport.Client) {
	for _, client := range clients {
		for {
			select {
			case _, ok := <-client.Outbound():
				if !ok {
					goto nextClient
				}
			default:
				goto nextClient
			}
		}
	nextClient:
	}
}

func TestMCPLiveControlContextAndOperations(t *testing.T) {
	ctx := context.Background()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	now := time.Now().UTC()

	openedAt := now.Add(-time.Minute)
	session := domain.LiveSession{MerchantID: "u_2001", Title: "直播控制台", Status: domain.LiveSessionStatusLive, OpenedAt: &openedAt}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	lot := domain.AuctionLot{
		AuctionID:      91001,
		SellerID:       "u_2001",
		LiveSessionID:  &session.ID,
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
	merchant, err := userRepo.FindByID("u_2001")
	if err != nil {
		t.Fatalf("find merchant: %v", err)
	}
	merchant.AIPermission = domain.MerchantAIPermissionAllow
	if err := userRepo.Update(&merchant); err != nil {
		t.Fatalf("update merchant ai permission: %v", err)
	}
	configRepo := repository.NewMemoryConfigRepository()
	aiHostingConfig, err := json.Marshal(map[string]bool{"enabled": true})
	if err != nil {
		t.Fatalf("marshal ai hosting config: %v", err)
	}
	if err := configRepo.Upsert(ctx, &domain.ConfigItem{Key: "merchant.u_2001.live_agent_hook", Value: aiHostingConfig, Description: "test ai hosting", UpdatedBy: "u_2001", UpdatedAt: now}); err != nil {
		t.Fatalf("enable ai hosting config: %v", err)
	}
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:             userRepo,
		AuctionRepo:          auctionRepo,
		LiveSessionRepo:      sessionRepo,
		ConfigRepo:           configRepo,
		ObjectUploader:       objectstorage.NewMemoryUploader(""),
		ProductAuditor:       appDisabledProductAuditor{},
		LiveVoiceSynthesizer: appFakeLiveVoiceSynthesizer{},
		LiveVoiceBroadcaster: &appFakeLiveVoiceBroadcaster{delivered: 0},
	})

	contextBody := `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"get_merchant_live_control_context","arguments":{"merchantId":"u_2001"}}}`
	contextResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", contextBody)
	if contextResp.status != 200 || contextResp.body.Error != nil {
		t.Fatalf("expected live context success, status=%d raw=%s", contextResp.status, contextResp.raw)
	}
	contextPayload := decodeMCPToolEnvelope[mcpDisplayLiveControlContext](t, contextResp)
	if contextPayload.Data.Session == nil || contextPayload.Data.Session.ID != session.ID {
		t.Fatalf("unexpected live context: %+v", contextPayload.Data)
	}
	if len(contextPayload.Data.Lots.UpcomingLots) != 1 || contextPayload.Data.Lots.UpcomingLots[0].AuctionID != lot.AuctionID {
		t.Fatalf("expected upcoming lot before start auction, got %+v", contextPayload.Data.Lots.UpcomingLots)
	}

	onShelfBody := `{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"onShelf"}}}`
	onShelfResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", onShelfBody)
	if onShelfResp.status != 200 || onShelfResp.body.Error == nil || onShelfResp.body.Error.Code != -32602 {
		t.Fatalf("expected onShelf to be removed from MCP actions, status=%d raw=%s", onShelfResp.status, onShelfResp.raw)
	}

	missingDurationBody := `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"startExplain"}}}`
	missingDurationResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", missingDurationBody)
	if missingDurationResp.status != 200 || missingDurationResp.body.Error == nil || missingDurationResp.body.Error.Code != -32602 {
		t.Fatalf("expected missing durationSec to be invalid params, status=%d raw=%s", missingDurationResp.status, missingDurationResp.raw)
	}
	oldDurationBody := `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"startExplain","auctionDurationSec":600}}}`
	oldDurationResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", oldDurationBody)
	if oldDurationResp.status != 200 || oldDurationResp.body.Error == nil || oldDurationResp.body.Error.Code != -32602 {
		t.Fatalf("expected old auctionDurationSec to be invalid params, status=%d raw=%s", oldDurationResp.status, oldDurationResp.raw)
	}

	merchant.AIPermission = domain.MerchantAIPermissionDeny
	if err := userRepo.Update(&merchant); err != nil {
		t.Fatalf("update merchant ai permission deny before startExplain: %v", err)
	}
	startBody := `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"startExplain","durationSec":600}}}`
	deniedStartResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", startBody)
	if deniedStartResp.status != 200 || deniedStartResp.body.Error != nil {
		t.Fatalf("expected startExplain denial to return MCP tool result, status=%d raw=%s", deniedStartResp.status, deniedStartResp.raw)
	}
	var deniedStartTool mcpToolResult
	mustDecodeData(t, deniedStartResp.body.Result, &deniedStartTool)
	if !deniedStartTool.IsError {
		t.Fatalf("expected startExplain to require AI approval and be rejected when permission is deny, raw=%s", deniedStartResp.raw)
	}
	merchant.AIPermission = domain.MerchantAIPermissionAllow
	if err := userRepo.Update(&merchant); err != nil {
		t.Fatalf("restore merchant ai permission before hammer: %v", err)
	}
	startResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", startBody)
	if startResp.status != 200 || startResp.body.Error != nil {
		t.Fatalf("expected startExplain success after AI approval, status=%d raw=%s", startResp.status, startResp.raw)
	}
	started := decodeMCPToolEnvelope[mcpDisplayLiveLotOperationResult](t, startResp)
	if started.Data.Context == nil || started.Data.Context.Lots.ExplainingLot == nil || started.Data.Context.Lots.ExplainingLot.AuctionID != lot.AuctionID {
		t.Fatalf("expected explaining lot after start, got %+v", started.Data)
	}
	if started.Data.Context.CurrentAuctionState == nil ||
		started.Data.Context.CurrentAuctionState.AuctionID != lot.AuctionID ||
		started.Data.Context.CurrentAuctionState.CurrentPrice.Value != "10.00" {
		t.Fatalf("expected current auction state after start, got %+v", started.Data.Context.CurrentAuctionState)
	}
	assertMCPDisplayMoney(t, started.Data.Context.CurrentAuctionState.CurrentPrice, "10.00")

	voiceBody := `{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"live_voice_broadcast","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"text":"请大家关注当前拍品细节。","requestId":"mcp-live-voice-1"}}}`
	voiceResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", voiceBody)
	if voiceResp.status != 200 || voiceResp.body.Error != nil {
		t.Fatalf("expected live voice broadcast success, status=%d raw=%s", voiceResp.status, voiceResp.raw)
	}
	voice := decodeMCPToolEnvelope[mcpapp.LiveVoiceBroadcastResult](t, voiceResp)
	if voice.Data.Status != "GENERATED" || voice.Data.LiveSessionID != session.ID || voice.Data.Text == "" || voice.Data.RequestID != "mcp-live-voice-1" || voice.Data.AudioBytes == 0 {
		t.Fatalf("unexpected live voice broadcast result: %+v", voice.Data)
	}

	hammerBody := `{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"operate_live_session_lot","arguments":{"liveSessionId":` + strconv.FormatUint(session.ID, 10) + `,"auctionId":91001,"action":"hammer","force":true,"requestId":"mcp-live-control-hammer-1"}}}`
	hammerResp := doMCPPath(t, h.Engine, "/mcp/control", "merchant-live-control-key", hammerBody)
	if hammerResp.status != 200 || hammerResp.body.Error != nil {
		t.Fatalf("expected hammer success, status=%d raw=%s", hammerResp.status, hammerResp.raw)
	}
	hammered := decodeMCPToolEnvelope[mcpDisplayLiveLotOperationResult](t, hammerResp)
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

func TestItemRoutesAreNotRegistered(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	paths := []struct {
		method string
		path   string
		body   string
	}{
		{method: consts.MethodGet, path: "/api/v1/items"},
		{method: consts.MethodPost, path: "/api/v1/items", body: `{"title":"legacy item"}`},
		{method: consts.MethodGet, path: "/api/v1/items/1"},
		{method: consts.MethodPatch, path: "/api/v1/items/1", body: `{"title":"legacy item"}`},
		{method: consts.MethodDelete, path: "/api/v1/items/1"},
		{method: consts.MethodPost, path: "/api/v1/items/audit/callback", body: `{}`},
		{method: consts.MethodPost, path: "/api/v1/items/description/optimize", body: `{}`},
	}
	for _, tt := range paths {
		var reqBody *ut.Body
		if tt.body != "" {
			reqBody = &ut.Body{Body: strings.NewReader(tt.body), Len: len(tt.body)}
		}
		resp := ut.PerformRequest(h.Engine, tt.method, tt.path, reqBody, ut.Header{Key: "Authorization", Value: "Bearer " + token})
		if resp.Code != 404 {
			t.Fatalf("expected legacy item route %s %s to be unregistered, got status=%d body=%s", tt.method, tt.path, resp.Code, resp.Body.String())
		}
	}
}

func TestAuctionAuditCallbackRouteAcceptsAuctionContext(t *testing.T) {
	h := newTestServer()
	payload := `{"requestId":"audit-callback-1","status":"REJECTED","success":false,"isApproved":false,"rejectReasons":["标题含违禁词"],"riskLabels":["content_policy"],"context":{"auctionId":91001,"scope":"auction_lot_content"}}`

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/audit/callback", payload)
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected auction audit callback success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var data struct {
		Accepted      bool     `json:"accepted"`
		RequestID     string   `json:"requestId"`
		AuctionID     uint64   `json:"auctionId"`
		Status        string   `json:"status"`
		Success       bool     `json:"success"`
		IsApproved    bool     `json:"isApproved"`
		RejectReasons []string `json:"rejectReasons"`
		RiskLabels    []string `json:"riskLabels"`
		Scope         string   `json:"scope"`
	}
	mustDecodeData(t, resp.body.Data, &data)
	if !data.Accepted || data.RequestID != "audit-callback-1" || data.AuctionID != 91001 || data.Status != "REJECTED" || data.Success || data.IsApproved {
		t.Fatalf("unexpected callback response: %+v", data)
	}
	if len(data.RejectReasons) != 1 || data.RejectReasons[0] != "标题含违禁词" || len(data.RiskLabels) != 1 || data.RiskLabels[0] != "content_policy" || data.Scope != "auction_lot_content" {
		t.Fatalf("unexpected callback details: %+v", data)
	}

	legacy := ut.PerformRequest(h.Engine, consts.MethodPost, "/api/v1/items/audit/callback", &ut.Body{Body: strings.NewReader(`{}`), Len: 2})
	if legacy.Code != 404 {
		t.Fatalf("expected legacy item audit callback to remain unregistered, got status=%d body=%s", legacy.Code, legacy.Body.String())
	}
}

func TestAuctionAuditCallbackRouteRequiresExplicitRejectedConclusion(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	createResp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("Callback Rejected Lot", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createResp.status != 200 || createResp.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createResp.status, createResp.raw)
	}
	var created struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createResp.body.Data, &created)
	auditInput := auditor.waitInput(t)
	taskID, ok := auditInput.CallbackContext["taskId"].(string)
	if !ok || !strings.HasPrefix(taskID, "product-audit-"+strconv.FormatUint(created.AuctionID, 10)+"-") {
		t.Fatalf("unexpected audit task id: %+v", auditInput.CallbackContext)
	}

	statusOnlyPayload := `{"requestId":"audit-callback-task-status-only","status":"REJECTED","callback_context":{"auctionId":` + strconv.FormatUint(created.AuctionID, 10) + `,"scope":"auction_lot_content","taskId":"` + taskID + `"}}`
	statusOnlyResp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/audit/callback", statusOnlyPayload)
	if statusOnlyResp.status != 200 || statusOnlyResp.body.Code != 0 {
		t.Fatalf("expected audit callback success, got status=%d raw=%s", statusOnlyResp.status, statusOnlyResp.raw)
	}
	var statusOnlyData struct {
		Success   bool   `json:"success"`
		LotStatus string `json:"lotStatus"`
	}
	mustDecodeData(t, statusOnlyResp.body.Data, &statusOnlyData)
	if statusOnlyData.Success || statusOnlyData.LotStatus != "" {
		t.Fatalf("status-only callback should not change lot state, got %+v", statusOnlyData)
	}
	getPendingResp := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/"+strconv.FormatUint(created.AuctionID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if getPendingResp.status != 200 || getPendingResp.body.Code != 0 {
		t.Fatalf("expected auction get success, got status=%d raw=%s", getPendingResp.status, getPendingResp.raw)
	}
	var pendingLot struct {
		Status domain.AuctionStatus `json:"status"`
	}
	mustDecodeData(t, getPendingResp.body.Data, &pendingLot)
	if pendingLot.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected status-only callback to keep PENDING_AUDIT, got %s", pendingLot.Status)
	}

	rejectReason := "商品信息中存在品牌信息不一致，涉嫌虚假宣传。"
	payload := `{"request_id":"audit-callback-rejected-conclusion","status":"COMPLETED","is_approved":false,"reject_reason":"` + rejectReason + `","callback_context":{"auctionId":` + strconv.FormatUint(created.AuctionID, 10) + `,"scope":"auction_lot_content","taskId":"` + taskID + `"}}`
	callbackResp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/audit/callback", payload)
	if callbackResp.status != 200 || callbackResp.body.Code != 0 {
		t.Fatalf("expected audit callback success, got status=%d raw=%s", callbackResp.status, callbackResp.raw)
	}
	var callbackData struct {
		Success       bool     `json:"success"`
		IsApproved    bool     `json:"isApproved"`
		LotStatus     string   `json:"lotStatus"`
		RejectReason  string   `json:"rejectReason"`
		RejectReasons []string `json:"rejectReasons"`
	}
	mustDecodeData(t, callbackResp.body.Data, &callbackData)
	if !callbackData.Success || callbackData.IsApproved || callbackData.LotStatus != string(domain.AuctionStatusAuditRejected) {
		t.Fatalf("expected explicit rejected callback to mark AUDIT_REJECTED, got %+v", callbackData)
	}
	if callbackData.RejectReason != rejectReason || len(callbackData.RejectReasons) != 1 || callbackData.RejectReasons[0] != rejectReason {
		t.Fatalf("expected callback reject reason, got %+v", callbackData)
	}

	getResp := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/"+strconv.FormatUint(created.AuctionID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if getResp.status != 200 || getResp.body.Code != 0 {
		t.Fatalf("expected auction get success, got status=%d raw=%s", getResp.status, getResp.raw)
	}
	var lot struct {
		Status            domain.AuctionStatus `json:"status"`
		AuditRejectReason string               `json:"auditRejectReason"`
	}
	mustDecodeData(t, getResp.body.Data, &lot)
	if lot.Status != domain.AuctionStatusAuditRejected {
		t.Fatalf("expected AUDIT_REJECTED after callback, got %s", lot.Status)
	}
	if lot.AuditRejectReason != rejectReason {
		t.Fatalf("expected audit reject reason after callback, got %q", lot.AuditRejectReason)
	}
}

func TestLiveSessionCoverUploadRoute(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions", `{"title":"商家直播场次"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 200 || create.body.Code != 0 {
		t.Fatalf("expected live session create success, got status=%d raw=%s", create.status, create.raw)
	}
	var session struct {
		ID       uint64 `json:"id"`
		CoverURL string `json:"coverUrl"`
	}
	mustDecodeData(t, create.body.Data, &session)
	if session.ID == 0 {
		t.Fatalf("expected live session id, got %+v", session)
	}

	upload := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions/"+strconv.FormatUint(session.ID, 10)+"/cover", nil, []multipartTestFile{
		{FieldName: "image", Filename: "live-cover.jpg", Body: "live session cover bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token}, ut.Header{Key: "Idempotency-Key", Value: "live-session-cover-1"})
	if upload.status != 200 || upload.body.Code != 0 {
		t.Fatalf("expected live session cover upload success, got status=%d raw=%s", upload.status, upload.raw)
	}
	var updated struct {
		ID       uint64 `json:"id"`
		CoverURL string `json:"coverUrl"`
	}
	mustDecodeData(t, upload.body.Data, &updated)
	if updated.ID != session.ID || !strings.HasPrefix(updated.CoverURL, "/api/v1/images/") {
		t.Fatalf("expected updated cover URL, got %+v", updated)
	}
	imageResp := ut.PerformRequest(h.Engine, consts.MethodGet, updated.CoverURL, nil)
	if imageResp.Code != 200 || imageResp.Body.String() != "live session cover bytes" {
		t.Fatalf("expected uploaded cover proxy success, got status=%d body=%q", imageResp.Code, imageResp.Body.String())
	}
}

func TestBuyerCanListMerchantLiveSessionsRoute(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	createLive := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions", `{"title":"买家可见珠宝直播"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createLive.status != 200 || createLive.body.Code != 0 {
		t.Fatalf("expected live session create success, got status=%d raw=%s", createLive.status, createLive.raw)
	}
	var liveSession struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, createLive.body.Data, &liveSession)

	startLive := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions/"+strconv.FormatUint(liveSession.ID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "buyer-list-merchant-live-start"})
	if startLive.status != 200 || startLive.body.Code != 0 {
		t.Fatalf("expected live session start success, got status=%d raw=%s", startLive.status, startLive.raw)
	}

	createDraft := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions", `{"title":"买家不可见草稿直播"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createDraft.status != 200 || createDraft.body.Code != 0 {
		t.Fatalf("expected draft session create success, got status=%d raw=%s", createDraft.status, createDraft.raw)
	}

	listLive := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/merchants/u_2001/live-sessions?keyword="+url.QueryEscape("珠宝"), "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if listLive.status != 200 || listLive.body.Code != 0 {
		t.Fatalf("expected buyer merchant live sessions success, got status=%d raw=%s", listLive.status, listLive.raw)
	}
	var liveData struct {
		Sessions []struct {
			ID     uint64                   `json:"id"`
			Status domain.LiveSessionStatus `json:"status"`
		} `json:"sessions"`
	}
	mustDecodeData(t, listLive.body.Data, &liveData)
	if len(liveData.Sessions) != 1 || liveData.Sessions[0].ID != liveSession.ID || liveData.Sessions[0].Status != domain.LiveSessionStatusLive {
		t.Fatalf("expected buyer to see only live merchant session, got %+v", liveData)
	}

	listDraft := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/merchants/u_2001/live-sessions?keyword="+url.QueryEscape("草稿"), "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if listDraft.status != 200 || listDraft.body.Code != 0 {
		t.Fatalf("expected buyer draft keyword query success, got status=%d raw=%s", listDraft.status, listDraft.raw)
	}
	var draftData struct {
		Sessions []struct {
			ID uint64 `json:"id"`
		} `json:"sessions"`
	}
	mustDecodeData(t, listDraft.body.Data, &draftData)
	if len(draftData.Sessions) != 0 {
		t.Fatalf("buyer should not see draft merchant sessions, got %+v", draftData)
	}
}

func TestAuctionCreateTriggersLotContentAudit(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
	uploader := objectstorage.NewMemoryUploader("")
	imageBody := "keyboard image bytes"
	imageURL, err := uploader.Upload(context.Background(), objectstorage.UploadInput{
		Filename:    "keyboard.jpg",
		ContentType: "image/jpeg",
		Size:        int64(len(imageBody)),
		Body:        strings.NewReader(imageBody),
	})
	if err != nil {
		t.Fatalf("upload audit image: %v", err)
	}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: uploader,
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSONWithImage("机械键盘 87键", "电脑外设/键盘", imageURL), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var lot struct {
		AuctionID uint64 `json:"auctionId"`
		SellerID  string `json:"sellerId"`
		Title     string `json:"title"`
		Status    string `json:"status"`
	}
	mustDecodeData(t, resp.body.Data, &lot)
	if lot.AuctionID == 0 || lot.SellerID != "u_2001" || lot.Title != "机械键盘 87键" || lot.Status != "PENDING_AUDIT" {
		t.Fatalf("unexpected auction create payload: %+v", lot)
	}
	input := auditor.waitInput(t)
	if input.ProductText == "" || !strings.Contains(input.ProductText, "商品标题：机械键盘 87键") || !strings.Contains(input.ProductText, "类目：电脑外设/键盘") || !strings.Contains(input.ProductText, "成色：九成新") {
		t.Fatalf("unexpected audit product text: %q", input.ProductText)
	}
	if string(input.Image) != imageBody || input.ContentType != "image/jpeg" || input.ImageSize != int64(len(imageBody)) {
		t.Fatalf("unexpected audit image payload: name=%q content_type=%q size=%d body=%q", input.ImageName, input.ContentType, input.ImageSize, string(input.Image))
	}
	if contextAuctionID, ok := input.CallbackContext["auctionId"].(uint64); !ok || contextAuctionID != lot.AuctionID {
		t.Fatalf("unexpected callback context auctionId: %+v", input.CallbackContext)
	}
	taskID, _ := input.CallbackContext["taskId"].(string)
	if input.CallbackContext["sellerId"] != "u_2001" || input.CallbackContext["scope"] != "auction_lot_content" || !strings.HasPrefix(taskID, "product-audit-"+strconv.FormatUint(lot.AuctionID, 10)+"-") {
		t.Fatalf("unexpected callback context: %+v", input.CallbackContext)
	}
}

func TestAuctionCreateSkipsLotContentAuditWhenDisabled(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-disabled", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
	cfg := appconfig.Default()
	cfg.Agent.ProductAuditEnabled = false
	cfg.Agent.ProductAuditURL = ""
	cfg.Agent.ProductAuditCallbackURL = ""
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("免审核测试拍品", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var lot struct {
		AuctionID uint64               `json:"auctionId"`
		Status    domain.AuctionStatus `json:"status"`
	}
	mustDecodeData(t, resp.body.Data, &lot)
	if lot.AuctionID == 0 || lot.Status != domain.AuctionStatusReady {
		t.Fatalf("expected product audit disabled to auto approve as READY, got %+v", lot)
	}
	auditor.assertNoInput(t, 100*time.Millisecond)
}

func TestAuctionDraftDoesNotTriggerAuditUntilPublish(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSONWithStatus("草稿拍品", "collectible", domain.AuctionStatusDraft), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected draft auction create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var lot struct {
		AuctionID uint64 `json:"auctionId"`
		Status    string `json:"status"`
	}
	mustDecodeData(t, resp.body.Data, &lot)
	if lot.Status != "DRAFT" {
		t.Fatalf("expected draft status, got %+v", lot)
	}
	auditor.assertNoInput(t, 50*time.Millisecond)

	patch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(lot.AuctionID, 10), `{"status":"PENDING_AUDIT"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected publish success, got status=%d raw=%s", patch.status, patch.raw)
	}
	input := auditor.waitInput(t)
	if input.CallbackContext["scope"] != "auction_lot_content" {
		t.Fatalf("unexpected audit input after publish: %+v", input)
	}
}

func TestAuctionDraftPublishSkipsLotContentAuditWhenDisabled(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-disabled", Status: "ACCEPTED"}, called: make(chan struct{}, 1)}
	cfg := appconfig.Default()
	cfg.Agent.ProductAuditEnabled = false
	cfg.Agent.ProductAuditURL = ""
	cfg.Agent.ProductAuditCallbackURL = ""
	h := NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSONWithStatus("免审核草稿拍品", "collectible", domain.AuctionStatusDraft), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected draft auction create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var lot struct {
		AuctionID uint64               `json:"auctionId"`
		Status    domain.AuctionStatus `json:"status"`
	}
	mustDecodeData(t, resp.body.Data, &lot)
	if lot.Status != domain.AuctionStatusDraft {
		t.Fatalf("expected draft status, got %+v", lot)
	}

	patch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(lot.AuctionID, 10), `{"status":"PENDING_AUDIT"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected publish success, got status=%d raw=%s", patch.status, patch.raw)
	}
	var updated struct {
		Status domain.AuctionStatus `json:"status"`
	}
	mustDecodeData(t, patch.body.Data, &updated)
	if updated.Status != domain.AuctionStatusReady {
		t.Fatalf("expected product audit disabled draft publish to auto approve as READY, got %+v", updated)
	}
	auditor.assertNoInput(t, 100*time.Millisecond)
}

func TestAuctionUpdateReadyLotRouteReturnsPendingAudit(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("已审核拍品", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 200 || create.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", create.status, create.raw)
	}
	var created struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, create.body.Data, &created)
	approveAuctionForTest(t, h.Engine, created.AuctionID)

	patch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(created.AuctionID, 10), `{"title":"已审核拍品 修改后","status":"PENDING_AUDIT"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected resubmit success, got status=%d raw=%s", patch.status, patch.raw)
	}
	var updated struct {
		Status domain.AuctionStatus `json:"status"`
		Title  string               `json:"title"`
	}
	mustDecodeData(t, patch.body.Data, &updated)
	if updated.Status != domain.AuctionStatusPendingAudit || updated.Title != "已审核拍品 修改后" {
		t.Fatalf("expected PATCH response to return PENDING_AUDIT, got %+v", updated)
	}

	get := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/"+strconv.FormatUint(created.AuctionID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if get.status != 200 || get.body.Code != 0 {
		t.Fatalf("expected auction get success, got status=%d raw=%s", get.status, get.raw)
	}
	var fetched struct {
		Status domain.AuctionStatus `json:"status"`
	}
	mustDecodeData(t, get.body.Data, &fetched)
	if fetched.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected GET response to keep PENDING_AUDIT before audit callback, got %s", fetched.Status)
	}
}

func TestAuctionUpdateTriggersLotContentAudit(t *testing.T) {
	auditor := &captureProductAuditor{result: auctionports.ProductAuditResult{Success: true, RequestID: "agent-lot-audit-1", Status: "ACCEPTED"}, called: make(chan struct{}, 2)}
	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
		UserRepo:       repository.NewSeedUserRepository(),
		ObjectUploader: objectstorage.NewMemoryUploader(""),
		ProductAuditor: auditor,
	})
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	resp := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("机械键盘 87键", "电脑外设/键盘"), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var lot struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, resp.body.Data, &lot)
	_ = auditor.waitInput(t)

	patch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(lot.AuctionID, 10), `{"title":"机械键盘 87键 改版","description":"改版后的 RGB 背光机械键盘"}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if patch.status != 200 || patch.body.Code != 0 {
		t.Fatalf("expected auction patch success, got status=%d raw=%s", patch.status, patch.raw)
	}
	updatedAudit := auditor.waitInput(t)
	if !strings.Contains(updatedAudit.ProductText, "机械键盘 87键 改版") || !strings.Contains(updatedAudit.ProductText, "改版后的 RGB") {
		t.Fatalf("expected updated lot display content in audit text, got %q", updatedAudit.ProductText)
	}
	if contextAuctionID, ok := updatedAudit.CallbackContext["auctionId"].(uint64); !ok || contextAuctionID != lot.AuctionID {
		t.Fatalf("unexpected update audit context: %+v", updatedAudit.CallbackContext)
	}
}

func TestAuctionCreateRejectsMissingDisplayContent(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", `{"title":"缺少展示内容","category":"camera","condition":"GOOD","startPrice":10000,"reservePrice":10000,"capPrice":20000,"depositAmount":0,"incrementRule":{"type":"fixed","amount":100,"maxBidSteps":10}}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if create.status != 400 || create.body.Code != 20001 {
		t.Fatalf("expected display content validation error, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestAuctionCreateRejectsResetAntiExtendShorterThanAntiSniping(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", `{"title":"反狙击校验","category":"watch","condition":"NEW","description":"反狙击模式校验","imageUrls":["/api/v1/images/test-lot.jpg"],"coverUrl":"/api/v1/images/test-lot.jpg","startPrice":10000,"reservePrice":0,"capPrice":20000,"antiSnipingSec":60,"antiExtendSec":30,"antiExtendMode":"RESET","depositAmount":0,"incrementRule":{"type":"fixed","amount":1000,"maxBidSteps":1}}`, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if create.status != 400 || create.body.Code != 20001 {
		t.Fatalf("expected anti-extend validation error, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestAuctionCreateAllowsLadderLastStepMaxEqualCapPrice(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	payload, err := json.Marshal(map[string]interface{}{
		"title":         "封顶价阶梯拍品",
		"category":      "watch",
		"condition":     string(domain.ConditionNew),
		"description":   "最后一档阶梯上限等于封顶价",
		"imageUrls":     []string{"/api/v1/images/test-lot.jpg"},
		"coverUrl":      "/api/v1/images/test-lot.jpg",
		"startPrice":    10000,
		"reservePrice":  0,
		"capPrice":      50000,
		"depositAmount": 0,
		"status":        string(domain.AuctionStatusPendingAudit),
		"incrementRule": map[string]interface{}{
			"type":        "ladder",
			"maxBidSteps": 1,
			"steps": []map[string]interface{}{
				{"min": 10000, "max": 20000, "amount": 1000},
				{"min": 20000, "max": 30000, "amount": 2000},
				{"min": 30000, "max": 50000, "amount": 5000},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", string(payload), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 200 || create.body.Code != 0 {
		t.Fatalf("expected ladder create success, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestAuctionCreateRejectsLadderLastStepMaxMismatchCapPrice(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	payload, err := json.Marshal(map[string]interface{}{
		"title":         "封顶价阶梯拍品",
		"category":      "watch",
		"condition":     string(domain.ConditionNew),
		"description":   "最后一档阶梯上限与封顶价不一致",
		"imageUrls":     []string{"/api/v1/images/test-lot.jpg"},
		"coverUrl":      "/api/v1/images/test-lot.jpg",
		"startPrice":    10000,
		"reservePrice":  0,
		"capPrice":      50000,
		"depositAmount": 0,
		"status":        string(domain.AuctionStatusPendingAudit),
		"incrementRule": map[string]interface{}{
			"type":        "ladder",
			"maxBidSteps": 1,
			"steps": []map[string]interface{}{
				{"min": 10000, "max": 20000, "amount": 1000},
				{"min": 20000, "max": 30000, "amount": 2000},
				{"min": 30000, "max": 45000, "amount": 5000},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", string(payload), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 400 || create.body.Code != 20001 {
		t.Fatalf("expected ladder create validation error, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestAuctionCreateRejectsLadderFirstStepMinMismatchStartPrice(t *testing.T) {
	h := newTestServer()
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	payload, err := json.Marshal(map[string]interface{}{
		"title":         "起拍价阶梯拍品",
		"category":      "watch",
		"condition":     string(domain.ConditionNew),
		"description":   "第一档起始价与起拍价不一致",
		"imageUrls":     []string{"/api/v1/images/test-lot.jpg"},
		"coverUrl":      "/api/v1/images/test-lot.jpg",
		"startPrice":    10000,
		"reservePrice":  0,
		"capPrice":      50000,
		"depositAmount": 0,
		"status":        string(domain.AuctionStatusPendingAudit),
		"incrementRule": map[string]interface{}{
			"type":        "ladder",
			"maxBidSteps": 1,
			"steps": []map[string]interface{}{
				{"min": 0, "max": 20000, "amount": 1000},
				{"min": 20000, "max": 50000, "amount": 5000},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	create := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", string(payload), ut.Header{Key: "Authorization", Value: "Bearer " + token})
	if create.status != 400 || create.body.Code != 20001 {
		t.Fatalf("expected first-step validation error, got status=%d body=%+v raw=%s", create.status, create.body, create.raw)
	}
}

func TestAuctionDescriptionOptimizeRoute(t *testing.T) {
	generator := &captureProductDescriptionGenerator{
		result: aiapp.ProductDescriptionResult{
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

	resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/description/optimize", map[string]string{
		"title":     "机械键盘 87键",
		"category":  "电脑外设/键盘",
		"condition": "九成新",
	}, []multipartTestFile{
		{FieldName: "image", Filename: "keyboard.jpg", Body: "fake image bytes"},
	}, ut.Header{Key: "Authorization", Value: "Bearer " + token})

	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected description optimize success, got status=%d body=%+v raw=%s", resp.status, resp.body, resp.raw)
	}
	var result aiapp.ProductDescriptionResult
	mustDecodeData(t, resp.body.Data, &result)
	if result.Description == "" || result.Title != "机械键盘 87键" || result.Category != "电脑外设/键盘" {
		t.Fatalf("unexpected optimize result: %+v", result)
	}
	if generator.input.Title != "机械键盘 87键" || generator.input.Category != "电脑外设/键盘" || generator.input.Condition != "九成新" || generator.imageBody != "fake image bytes" {
		t.Fatalf("unexpected generator input: %+v image=%q", generator.input, generator.imageBody)
	}
}

func TestAuctionDescriptionOptimizeRouteUsesImageURL(t *testing.T) {
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
		result: aiapp.ProductDescriptionResult{
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

	resp := doMultipartWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/description/optimize", map[string]string{
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

func TestAuctionRoutesProtectRunningLotDisplayContent(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	missing := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/999999", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if missing.status != 404 || missing.body.Code != 20004 {
		t.Fatalf("expected auction not found code 20004, got status=%d code=%d raw=%s", missing.status, missing.body.Code, missing.raw)
	}

	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("Running Lot", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)
	approveAuctionForTest(t, h.Engine, auctionData.AuctionID)

	start := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "item-active-auction-start"})
	if start.status != 200 || start.body.Code != 0 {
		t.Fatalf("expected auction start success, got status=%d raw=%s", start.status, start.raw)
	}

	titlePatch := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), `{"title":"Blocked Critical Change"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if titlePatch.status != 409 || titlePatch.body.Code != 20010 {
		t.Fatalf("expected running-auction display patch to fail with 20010, got status=%d code=%d raw=%s", titlePatch.status, titlePatch.body.Code, titlePatch.raw)
	}

	deleteResp := doJSONWithHeaders(t, h.Engine, consts.MethodDelete, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if deleteResp.status != 409 || deleteResp.body.Code != 20010 {
		t.Fatalf("expected running-auction delete to fail with 20010, got status=%d code=%d raw=%s", deleteResp.status, deleteResp.body.Code, deleteResp.raw)
	}
}

func TestAuctionCreateAndUpdateStatusRequiresAudit(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	pendingBody := auctionCreateJSON("Audit Required Lot", "collectible")
	createPending := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", pendingBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createPending.status != 200 || createPending.body.Code != 0 {
		t.Fatalf("expected create PENDING_AUDIT success, got status=%d raw=%s", createPending.status, createPending.raw)
	}

	readyBody := `{"title":"Direct Ready Lot","category":"collectible","condition":"GOOD","description":"直接就绪拍品","startPrice":10000,"reservePrice":15000,"capPrice":20000,"depositAmount":1000,"status":"READY","incrementRule":{"type":"fixed","amount":500,"maxBidSteps":10}}`
	createReady := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", readyBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createReady.status != 400 || createReady.body.Code != 20001 {
		t.Fatalf("expected create READY to fail validation, got status=%d code=%d raw=%s", createReady.status, createReady.body.Code, createReady.raw)
	}

	systemBody := `{"title":"System Status Lot","category":"collectible","condition":"GOOD","description":"系统状态拍品","startPrice":100,"reservePrice":5000,"capPrice":10000,"depositAmount":0,"status":"RUNNING","incrementRule":{"type":"fixed","amount":100,"maxBidSteps":5}}`
	createSystem := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", systemBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createSystem.status != 400 || createSystem.body.Code != 20001 {
		t.Fatalf("expected create RUNNING to fail validation, got status=%d code=%d raw=%s", createSystem.status, createSystem.body.Code, createSystem.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createPending.body.Data, &auctionData)

	patchReady := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), `{"status":"READY"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if patchReady.status != 400 || patchReady.body.Code != 20001 {
		t.Fatalf("expected patch READY to fail validation, got status=%d code=%d raw=%s", patchReady.status, patchReady.body.Code, patchReady.raw)
	}

	patchRunning := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), `{"status":"RUNNING"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if patchRunning.status != 400 || patchRunning.body.Code != 20001 {
		t.Fatalf("expected patch RUNNING to fail validation, got status=%d code=%d raw=%s", patchRunning.status, patchRunning.body.Code, patchRunning.raw)
	}
}

func TestLiveSessionMountLotRoute(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	createSession := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions", `{"title":"自动上架直播场次"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createSession.status != 200 || createSession.body.Code != 0 {
		t.Fatalf("expected live session create success, got status=%d raw=%s", createSession.status, createSession.raw)
	}
	var sessionData struct {
		ID uint64 `json:"id"`
	}
	mustDecodeData(t, createSession.body.Data, &sessionData)

	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("Auto Mount Lot", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)
	approveAuctionForTest(t, h.Engine, auctionData.AuctionID)

	mount := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions/"+strconv.FormatUint(sessionData.ID, 10)+"/lots", `{"auctionId":`+strconv.FormatUint(auctionData.AuctionID, 10)+`}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "session-mount-lot-1"})
	if mount.status != 200 || mount.body.Code != 0 {
		t.Fatalf("expected auto mount success, got status=%d raw=%s", mount.status, mount.raw)
	}
	var mountData struct {
		Lot struct {
			AuctionID     uint64  `json:"auctionId"`
			LiveSessionID *uint64 `json:"liveSessionId"`
		} `json:"lot"`
	}
	mustDecodeData(t, mount.body.Data, &mountData)
	if mountData.Lot.AuctionID != auctionData.AuctionID || mountData.Lot.LiveSessionID == nil || *mountData.Lot.LiveSessionID != sessionData.ID {
		t.Fatalf("unexpected mount response: %+v want auction=%d session=%d", mountData, auctionData.AuctionID, sessionData.ID)
	}

	startSession := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-sessions/"+strconv.FormatUint(sessionData.ID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "session-start-for-buyer-lots"})
	if startSession.status != 200 || startSession.body.Code != 0 {
		t.Fatalf("expected live session start success, got status=%d raw=%s", startSession.status, startSession.raw)
	}
	listLots := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/live-sessions/"+strconv.FormatUint(sessionData.ID, 10)+"/lots", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if listLots.status != 200 || listLots.body.Code != 0 {
		t.Fatalf("expected buyer list live session lots success, got status=%d raw=%s", listLots.status, listLots.raw)
	}
	var lotsData struct {
		Lots []struct {
			AuctionID     uint64          `json:"auctionId"`
			IncrementRule json.RawMessage `json:"incrementRule"`
		} `json:"lots"`
	}
	mustDecodeData(t, listLots.body.Data, &lotsData)
	if len(lotsData.Lots) != 1 || lotsData.Lots[0].AuctionID != auctionData.AuctionID || len(lotsData.Lots[0].IncrementRule) == 0 {
		t.Fatalf("expected buyer lot response to include incrementRule, got %+v", lotsData)
	}
}

func TestAuctionRoutesStateAndIdempotencyMiddleware(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("Watch", "luxury"), ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
		Status    string `json:"status"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)
	if auctionData.AuctionID == 0 || auctionData.Status != "PENDING_AUDIT" {
		t.Fatalf("unexpected auction create payload: %+v", auctionData)
	}

	noKey := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if noKey.status != 400 || noKey.body.Code != 20011 {
		t.Fatalf("expected missing idempotency key, got status=%d code=%d raw=%s", noKey.status, noKey.body.Code, noKey.raw)
	}
	approveAuctionForTest(t, h.Engine, auctionData.AuctionID)

	start := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/start", "", ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken}, ut.Header{Key: "Idempotency-Key", Value: "idem-start-1"})
	if start.status != 200 || start.body.Code != 0 {
		t.Fatalf("expected auction start success, got status=%d raw=%s", start.status, start.raw)
	}

	state := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/state", "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if state.status != 200 || state.body.Code != 0 {
		t.Fatalf("expected auction state success for buyer, got status=%d raw=%s", state.status, state.raw)
	}
	var stateData struct {
		AuctionID     uint64          `json:"auctionId"`
		Status        string          `json:"status"`
		StartPrice    int64           `json:"startPrice"`
		CapPrice      int64           `json:"capPrice"`
		IncrementRule json.RawMessage `json:"incrementRule"`
		CurrentPrice  int64           `json:"currentPrice"`
		Source        string          `json:"source"`
	}
	mustDecodeData(t, state.body.Data, &stateData)
	if stateData.AuctionID != auctionData.AuctionID || stateData.Status != "RUNNING" || stateData.StartPrice != 10000 || stateData.CapPrice != 20000 || stateData.CurrentPrice != 10000 || stateData.Source != "redis" {
		t.Fatalf("unexpected state payload: %+v", stateData)
	}
	var rule struct {
		Type        string `json:"type"`
		Amount      int64  `json:"amount"`
		MaxBidSteps int    `json:"maxBidSteps"`
	}
	if err := json.Unmarshal(stateData.IncrementRule, &rule); err != nil {
		t.Fatalf("decode state incrementRule: %v raw=%s", err, string(stateData.IncrementRule))
	}
	if rule.Type != "fixed" || rule.Amount != 500 || rule.MaxBidSteps != 10 {
		t.Fatalf("unexpected state incrementRule: %+v", rule)
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
	if hammer.status != 200 || hammer.body.Code != 0 {
		t.Fatalf("expected manual hammer success, got status=%d code=%d raw=%s", hammer.status, hammer.body.Code, hammer.raw)
	}
	var hammerData struct {
		Result domain.HammerResult `json:"result"`
		Order  *domain.OrderDeal   `json:"order"`
	}
	mustDecodeData(t, hammer.body.Data, &hammerData)
	if hammerData.Result.Status != domain.AuctionStatusClosedFailed || hammerData.Order != nil {
		t.Fatalf("expected forced empty hammer to close failed without order, got %+v", hammerData)
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
	if mineData.Orders[0].WinnerNickname != "竞拍用户001" {
		t.Fatalf("expected winner nickname in order list, got %+v", mineData.Orders[0])
	}

	detail := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/orders/"+strconv.FormatUint(order.ID, 10), "", ut.Header{Key: "Authorization", Value: "Bearer " + buyerToken})
	if detail.status != 200 || detail.body.Code != 0 {
		t.Fatalf("expected order detail success, got status=%d raw=%s", detail.status, detail.raw)
	}
	var detailData domain.OrderDeal
	mustDecodeData(t, detail.body.Data, &detailData)
	if detailData.WinnerNickname != "竞拍用户001" {
		t.Fatalf("expected winner nickname in order detail, got %+v", detailData)
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
		result: liveanalysisports.AsyncRequestResult{RequestID: "agent-live-analysis-1", Status: "ACCEPTED"},
	}
	reportRepo := repository.NewMemoryLiveAnalysisReportRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	closedAt := time.Now().UTC()
	openedAt := closedAt.Add(-time.Hour)
	session := domain.LiveSession{
		MerchantID:  "u_2001",
		Status:      domain.LiveSessionStatusEnded,
		OpenedAt:    &openedAt,
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
		ProductAuditor:         appDisabledProductAuditor{},
		LiveAnalysisReportRepo: reportRepo,
		LiveAnalysisRequester:  requester,
	})
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	running := getLiveAnalysisTask(t, h.Engine, session.ID, merchantToken)
	if running.TaskID == "" || running.LiveSessionID != session.ID || running.MerchantID != "u_2001" {
		t.Fatalf("unexpected running task: %+v", running)
	}
	if running.Status != liveanalysisapp.LiveAnalysisTaskRunning || running.Report != "" || running.AttemptCount != 1 {
		t.Fatalf("expected accepted running task with empty report, got %+v", running)
	}
	if requester.prompt() != "帮我总结商家id为u_2001直播场次id为"+strconv.FormatUint(session.ID, 10)+"的直播情况，重点看成交、出价、订单和风险问题。" ||
		requester.input.CallbackContext["taskId"] != running.TaskID ||
		requester.input.CallbackContext["liveSessionId"] != session.ID ||
		requester.input.CallbackContext["attempt"] != 1 ||
		len(requester.input.CallbackHeaders) != 0 {
		t.Fatalf("unexpected requester input: %+v", requester.input)
	}

	callback := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/live-analysis/callback", `{"request_id":"agent-live-analysis-1","success":true,"status":"COMPLETED","summary":"本场直播共成交 3 件拍品，转化良好。","error_message":null,"callback_context":{"taskId":"`+running.TaskID+`","liveSessionId":`+strconv.FormatUint(session.ID, 10)+`,"merchantId":"u_2001","attempt":1},"completed_at":"2026-05-26T10:30:00Z"}`)
	if callback.status != 200 || callback.body.Code != 0 {
		t.Fatalf("expected callback success, got status=%d raw=%s", callback.status, callback.raw)
	}

	got := pollLiveAnalysisTask(t, h.Engine, session.ID, merchantToken)
	if got.Status != liveanalysisapp.LiveAnalysisTaskSucceeded || got.Report != "本场直播共成交 3 件拍品，转化良好。" {
		t.Fatalf("unexpected finished task: %+v", got)
	}
	persisted, err := reportRepo.FindByLiveSessionID(t.Context(), session.ID)
	if err != nil || persisted.Status != liveanalysisapp.LiveAnalysisTaskSucceeded || persisted.Report != got.Report {
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
		result: liveanalysisports.AsyncRequestResult{RequestID: "agent-hook-1", Status: "ACCEPTED"},
	}
	reportRepo := repository.NewMemoryLiveAnalysisReportRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	closedAt := time.Now().UTC()
	openedAt := closedAt.Add(-30 * time.Minute)
	session := domain.LiveSession{
		MerchantID: "u_2001",
		Status:     domain.LiveSessionStatusEnded,
		OpenedAt:   &openedAt,
		ClosedAt:   &closedAt,
	}
	if err := sessionRepo.Create(t.Context(), &session); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	svc := liveanalysisapp.NewLiveAnalysisService(reportRepo, sessionRepo, requester, liveanalysisapp.LiveAnalysisOptions{
		CallbackURL:    appconfig.Default().Agent.LiveAnalysisCallbackURL,
		CallbackAPIKey: appconfig.Default().Agent.LiveAnalysisCallbackAPIKey,
	})

	hook := buildLiveSessionEndedHook(nil, nil, svc)
	if hook == nil {
		t.Fatal("expected hook")
	}
	hook(t.Context(), session)

	task, err := reportRepo.FindByLiveSessionID(t.Context(), session.ID)
	if err != nil {
		t.Fatalf("find report: %v", err)
	}
	if task.Status != liveanalysisapp.LiveAnalysisTaskRunning || task.LiveSessionID != session.ID || requester.prompt() == "" {
		t.Fatalf("expected hook to start live analysis, task=%+v requester=%+v", task, requester.input)
	}
}

func TestAdminRoutesMinimalClosedLoop(t *testing.T) {
	cfg := appconfig.Default()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	liveSessionRepo := repository.NewMemoryLiveSessionRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	auditRepo := repository.NewMemoryAuditRepository()
	now := time.Now().UTC()
	liveSessionID := uint64(70001)
	if err := liveSessionRepo.Create(t.Context(), &domain.LiveSession{
		ID:         liveSessionID,
		MerchantID: "u_2001",
		Title:      "春拍直播",
		Status:     domain.LiveSessionStatusLive,
	}); err != nil {
		t.Fatalf("seed live session: %v", err)
	}
	auction := &domain.AuctionLot{
		AuctionID:      88001,
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
		Title:          "青瓷花瓶",
		Description:    "釉色温润的青瓷花瓶",
		Category:       "collectible",
		ConditionGrade: domain.ConditionGood,
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
	if _, _, err := orderRepo.CreateIfAbsentByAuction(t.Context(), &domain.OrderDeal{AuctionID: 88001, LiveSessionID: &liveSessionID, WinnerID: "u_1001", SellerID: "u_2001", DealPrice: 1200, Status: domain.OrderStatusCreated, PayStatus: domain.PayStatusUnpaid}); err != nil {
		t.Fatalf("seed order: %v", err)
	}
	if err := riskRepo.CreateEvent(t.Context(), &domain.RiskEvent{EventType: "BID_FREQ", UserID: "u_1001", AuctionID: 88001, Severity: domain.RiskSeverityMid, Status: domain.RiskEventPending}); err != nil {
		t.Fatalf("seed risk event: %v", err)
	}
	h := NewServerWithDependencies(cfg, ServerDependencies{UserRepo: userRepo, AuctionRepo: auctionRepo, LiveSessionRepo: liveSessionRepo, OrderRepo: orderRepo, RiskRepo: riskRepo, AuditRepo: auditRepo})
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
	var userPage struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	mustDecodeData(t, users.body.Data, &userPage)
	if len(userPage.Items) == 0 {
		t.Fatalf("expected admin users, got raw=%s", users.raw)
	}
	for _, item := range userPage.Items {
		if _, ok := item["blacklisted"]; !ok {
			t.Fatalf("expected admin user item to include blacklisted, item=%s", item)
		}
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
	var auctionPage struct {
		Items []struct {
			AuctionID            uint64 `json:"auctionId"`
			SellerNickname       string `json:"sellerNickname"`
			LiveSessionName      string `json:"liveSessionName"`
			LeaderBidderNickname string `json:"leaderBidderNickname"`
			WinnerNickname       string `json:"winnerNickname"`
		} `json:"items"`
	}
	mustDecodeData(t, auctions.body.Data, &auctionPage)
	if len(auctionPage.Items) != 1 || auctionPage.Items[0].SellerNickname != "商家001" || auctionPage.Items[0].LiveSessionName != "春拍直播" {
		t.Fatalf("expected admin auction names, got %+v raw=%s", auctionPage.Items, auctions.raw)
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
	blacklistedUser := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/users?keyword=u_1002", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if blacklistedUser.status != 200 || blacklistedUser.body.Code != 0 {
		t.Fatalf("expected blacklisted user query success, got status=%d raw=%s", blacklistedUser.status, blacklistedUser.raw)
	}
	var blacklistedUserPage struct {
		Items []struct {
			ID          string `json:"id"`
			Blacklisted bool   `json:"blacklisted"`
		} `json:"items"`
	}
	mustDecodeData(t, blacklistedUser.body.Data, &blacklistedUserPage)
	if len(blacklistedUserPage.Items) != 1 || blacklistedUserPage.Items[0].ID != "u_1002" || !blacklistedUserPage.Items[0].Blacklisted {
		t.Fatalf("expected u_1002 to be blacklisted, got %+v raw=%s", blacklistedUserPage.Items, blacklistedUser.raw)
	}
	blacklistList := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/blacklist", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if blacklistList.status != 200 || blacklistList.body.Code != 0 {
		t.Fatalf("expected blacklist list success, got status=%d raw=%s", blacklistList.status, blacklistList.raw)
	}
	var blacklistPage struct {
		Items []struct {
			UserID            string `json:"userId"`
			Nickname          string `json:"nickname"`
			CreatedByName     string `json:"createdByName"`
			CreatedByNickname string `json:"createdByNickname"`
		} `json:"items"`
	}
	mustDecodeData(t, blacklistList.body.Data, &blacklistPage)
	if len(blacklistPage.Items) != 1 || blacklistPage.Items[0].UserID != "u_1002" || blacklistPage.Items[0].Nickname != "停用用户001" || blacklistPage.Items[0].CreatedByName != "管理员001" || blacklistPage.Items[0].CreatedByNickname != "管理员001" {
		t.Fatalf("expected blacklist item with nickname, got %+v raw=%s", blacklistPage.Items, blacklistList.raw)
	}
	blacklistDelete := doJSONWithHeaders(t, h.Engine, consts.MethodDelete, "/api/v1/admin/blacklist/u_1002", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "blacklist-del-1"})
	if blacklistDelete.status != 200 || blacklistDelete.body.Code != 0 {
		t.Fatalf("expected blacklist delete success, got status=%d raw=%s", blacklistDelete.status, blacklistDelete.raw)
	}

	orders := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/orders", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if orders.status != 200 || orders.body.Code != 0 {
		t.Fatalf("expected admin orders success, got status=%d raw=%s", orders.status, orders.raw)
	}
	var orderPage struct {
		Items []struct {
			AuctionID       uint64 `json:"auctionId"`
			WinnerNickname  string `json:"winnerNickname"`
			SellerNickname  string `json:"sellerNickname"`
			LiveSessionName string `json:"liveSessionName"`
			AuctionName     string `json:"auctionName"`
			AuctionTitle    string `json:"auctionTitle"`
		} `json:"items"`
	}
	mustDecodeData(t, orders.body.Data, &orderPage)
	if len(orderPage.Items) != 1 || orderPage.Items[0].AuctionID != 88001 || orderPage.Items[0].WinnerNickname != "竞拍用户001" || orderPage.Items[0].SellerNickname != "商家001" || orderPage.Items[0].LiveSessionName != "春拍直播" || orderPage.Items[0].AuctionName != "青瓷花瓶" || orderPage.Items[0].AuctionTitle != "青瓷花瓶" {
		t.Fatalf("expected admin order names, got %+v raw=%s", orderPage.Items, orders.raw)
	}
	auditLogs := doJSONWithHeaders(t, h.Engine, consts.MethodGet, "/api/v1/admin/audit-logs", "", ut.Header{Key: "Authorization", Value: "Bearer " + adminToken})
	if auditLogs.status != 200 || auditLogs.body.Code != 0 {
		t.Fatalf("expected admin audit logs success, got status=%d raw=%s", auditLogs.status, auditLogs.raw)
	}
	var auditPage struct {
		Items []struct {
			OperatorName     string `json:"operatorName"`
			OperatorNickname string `json:"operatorNickname"`
			TargetName       string `json:"targetName"`
		} `json:"items"`
	}
	mustDecodeData(t, auditLogs.body.Data, &auditPage)
	if len(auditPage.Items) == 0 || auditPage.Items[0].OperatorName != "管理员001" || auditPage.Items[0].OperatorNickname != "管理员001" || auditPage.Items[0].TargetName == "" {
		t.Fatalf("expected admin audit log names, got %+v raw=%s", auditPage.Items, auditLogs.raw)
	}
	merchantCreateAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionCreateJSON("Audit Log Lot", "collectible"), ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if merchantCreateAuction.status != 200 || merchantCreateAuction.body.Code != 0 {
		t.Fatalf("expected merchant auction create success, got status=%d raw=%s", merchantCreateAuction.status, merchantCreateAuction.raw)
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
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()

	openedAt := start.Add(5 * time.Minute)
	session := domain.LiveSession{
		MerchantID:  "u_2001",
		Title:       "监控测试场次",
		Status:      domain.LiveSessionStatusLive,
		OpenedAt:    &openedAt,
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
		SellerID:      "u_2001",
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
		LiveSessionRepo: sessionRepo,
		BidRepo:         bidRepo,
		OrderRepo:       orderRepo,
		RiskRepo:        riskRepo,
		ObjectUploader:  objectstorage.NewMemoryUploader(""),
		ProductAuditor:  appDisabledProductAuditor{},
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
	if data.Current.ActiveLiveSessionCount != 1 || data.Current.RunningAuctionCount != 1 || data.Current.PendingRiskEventCount != 1 {
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
		ProductAuditor: appDisabledProductAuditor{},
	})
}

type captureProductDescriptionGenerator struct {
	input     aiapp.ProductDescriptionInput
	imageBody string
	result    aiapp.ProductDescriptionResult
	err       error
}

func (g *captureProductDescriptionGenerator) GenerateProductDescription(ctx context.Context, in aiapp.ProductDescriptionInput) (aiapp.ProductDescriptionResult, error) {
	_ = ctx
	g.input = in
	g.imageBody = string(in.Image)
	if g.err != nil {
		return aiapp.ProductDescriptionResult{}, g.err
	}
	return g.result, nil
}

func auctionCreateJSON(title, category string) string {
	return auctionCreateJSONWithStatus(title, category, domain.AuctionStatusPendingAudit)
}

func auctionCreateJSONWithImage(title, category, imageURL string) string {
	payload, err := json.Marshal(map[string]interface{}{
		"title":         title,
		"category":      category,
		"condition":     string(domain.ConditionGood),
		"description":   "适合直播拍卖的展示拍品",
		"imageUrls":     []string{imageURL},
		"coverUrl":      imageURL,
		"startPrice":    10000,
		"reservePrice":  15000,
		"capPrice":      20000,
		"depositAmount": 1000,
		"status":        string(domain.AuctionStatusPendingAudit),
		"incrementRule": map[string]interface{}{"type": "fixed", "amount": 500, "maxBidSteps": 10},
	})
	if err != nil {
		panic(err)
	}
	return string(payload)
}

func auctionCreateJSONWithStatus(title, category string, status domain.AuctionStatus) string {
	payload, err := json.Marshal(map[string]interface{}{
		"title":         title,
		"category":      category,
		"condition":     string(domain.ConditionGood),
		"description":   "适合直播拍卖的展示拍品",
		"imageUrls":     []string{"/api/v1/images/test-lot.jpg"},
		"startPrice":    10000,
		"reservePrice":  15000,
		"capPrice":      20000,
		"depositAmount": 1000,
		"status":        string(status),
		"incrementRule": map[string]interface{}{"type": "fixed", "amount": 500, "maxBidSteps": 10},
	})
	if err != nil {
		panic(err)
	}
	return string(payload)
}

type captureProductAuditor struct {
	mu     sync.Mutex
	input  auctionports.ProductAuditInput
	result auctionports.ProductAuditResult
	err    error
	called chan struct{}
}

func (a *captureProductAuditor) AuditProduct(ctx context.Context, in auctionports.ProductAuditInput) (auctionports.ProductAuditResult, error) {
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
		return auctionports.ProductAuditResult{}, a.err
	}
	return a.result, nil
}

func (a *captureProductAuditor) waitInput(t *testing.T) auctionports.ProductAuditInput {
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

func (a *captureProductAuditor) assertNoInput(t *testing.T, timeout time.Duration) {
	t.Helper()
	if a.called == nil {
		return
	}
	select {
	case <-a.called:
		t.Fatal("expected product audit hook not to be called")
	case <-time.After(timeout):
	}
}

type captureLiveAnalysisRequester struct {
	mu     sync.Mutex
	input  liveanalysisports.AsyncRequestInput
	result liveanalysisports.AsyncRequestResult
	err    error
}

func (r *captureLiveAnalysisRequester) RequestLiveAnalysis(ctx context.Context, in liveanalysisports.AsyncRequestInput) (liveanalysisports.AsyncRequestResult, error) {
	_ = ctx
	r.mu.Lock()
	r.input = in
	result := r.result
	err := r.err
	r.mu.Unlock()
	if err != nil {
		return liveanalysisports.AsyncRequestResult{}, err
	}
	return result, nil
}

func (r *captureLiveAnalysisRequester) prompt() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.input.Prompt
}

func pollLiveAnalysisTask(t *testing.T, engine *route.Engine, liveSessionID uint64, token string) liveanalysisapp.LiveAnalysisTask {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last liveanalysisapp.LiveAnalysisTask
	for time.Now().Before(deadline) {
		last = getLiveAnalysisTask(t, engine, liveSessionID, token)
		if last.Status == liveanalysisapp.LiveAnalysisTaskSucceeded || last.Status == liveanalysisapp.LiveAnalysisTaskFailed {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task did not finish: %+v", last)
	return liveanalysisapp.LiveAnalysisTask{}
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

func getLiveAnalysisTask(t *testing.T, engine *route.Engine, liveSessionID uint64, token string) liveanalysisapp.LiveAnalysisTask {
	t.Helper()
	resp := doJSONWithHeaders(t, engine, consts.MethodGet, "/api/v1/live-analysis/reports/"+strconv.FormatUint(liveSessionID, 10), "",
		ut.Header{Key: "Authorization", Value: "Bearer " + token},
	)
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected poll success, got status=%d raw=%s", resp.status, resp.raw)
	}
	var task liveanalysisapp.LiveAnalysisTask
	mustDecodeData(t, resp.body.Data, &task)
	return task
}

func approveAuctionForTest(t *testing.T, engine *route.Engine, auctionID uint64) {
	t.Helper()
	adminToken := loginForToken(t, engine, "admin001", "AdminPassw0rd!", "admin")
	resp := doJSONWithHeaders(t, engine, consts.MethodPost, "/api/v1/admin/auctions/"+strconv.FormatUint(auctionID, 10)+"/audit", `{"auditResult":"APPROVED","reason":"test"}`,
		ut.Header{Key: "Authorization", Value: "Bearer " + adminToken},
		ut.Header{Key: "Idempotency-Key", Value: "approve-auction-" + strconv.FormatUint(auctionID, 10) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)},
	)
	if resp.status != 200 || resp.body.Code != 0 {
		t.Fatalf("expected approve auction success, got status=%d raw=%s", resp.status, resp.raw)
	}
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

type mcpDisplayMoney struct {
	Value    string `json:"value"`
	Unit     string `json:"unit"`
	Currency string `json:"currency"`
}

type mcpDisplayLot struct {
	AuctionID     uint64               `json:"auctionId"`
	LiveSessionID *uint64              `json:"liveSessionId,omitempty"`
	StartPrice    mcpDisplayMoney      `json:"startPrice"`
	Status        domain.AuctionStatus `json:"status"`
}

type mcpDisplayCurrentAuctionState struct {
	AuctionID    uint64          `json:"auctionId"`
	CurrentPrice mcpDisplayMoney `json:"currentPrice"`
}

type mcpDisplayLiveControlContext struct {
	Session *struct {
		ID uint64 `json:"id"`
	} `json:"session,omitempty"`
	Lots struct {
		ExplainingLot *mcpDisplayLot  `json:"explainingLot,omitempty"`
		CandidateLots []mcpDisplayLot `json:"candidateLots"`
		UpcomingLots  []mcpDisplayLot `json:"upcomingLots"`
		UnsoldLots    []mcpDisplayLot `json:"unsoldLots"`
	} `json:"lots"`
	CurrentAuctionState *mcpDisplayCurrentAuctionState `json:"currentAuctionState,omitempty"`
}

type mcpDisplayLiveLotOperationResult struct {
	Lot          *mcpDisplayLot                `json:"lot,omitempty"`
	Context      *mcpDisplayLiveControlContext `json:"context,omitempty"`
	HammerResult *struct {
		Status domain.AuctionStatus `json:"status"`
	} `json:"hammerResult,omitempty"`
}

func assertMCPDisplayMoney(t *testing.T, got mcpDisplayMoney, want string) {
	t.Helper()
	if got.Value != want || got.Unit != "元" || got.Currency != "CNY" {
		t.Fatalf("money=%+v want value=%s unit=元 currency=CNY", got, want)
	}
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

type mcpListedTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

func containsTool(tools []mcpListedTool, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func findMCPTool(tools []mcpListedTool, name string) (mcpListedTool, bool) {
	for _, tool := range tools {
		if tool.Name == name {
			return tool, true
		}
	}
	return mcpListedTool{}, false
}

func ptrInt64(v int64) *int64 {
	return &v
}

func ptrString(v string) *string {
	return &v
}

type appFakeLiveVoiceSynthesizer struct{}

func (appFakeLiveVoiceSynthesizer) SynthesizeLiveVoice(ctx context.Context, in mcpapp.LiveVoiceSynthesisInput) (mcpapp.LiveVoiceSynthesisResult, error) {
	_ = ctx
	return mcpapp.LiveVoiceSynthesisResult{
		Audio:       []byte("fake-audio-" + in.Text),
		AudioFormat: "pcm_s16le",
		Encoding:    "pcm_s16le",
		SampleRate:  24000,
		Channels:    1,
		Voice:       "zh_female_vv_jupiter_bigtts",
		Provider:    "doubao",
	}, nil
}

type appFakeLiveVoiceBroadcaster struct {
	delivered int
	payload   mcpapp.LiveVoiceBroadcastPayload
}

func (f *appFakeLiveVoiceBroadcaster) BroadcastLiveVoice(ctx context.Context, liveSessionID uint64, payload mcpapp.LiveVoiceBroadcastPayload) (int, error) {
	_ = ctx
	_ = liveSessionID
	f.payload = payload
	return f.delivered, nil
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
