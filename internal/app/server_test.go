package app

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"strconv"
	"strings"
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
	token := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")

	initResp := doMCP(t, h.Engine, token, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`)
	if initResp.status != 200 || initResp.body.Error != nil {
		t.Fatalf("expected initialize success, status=%d raw=%s", initResp.status, initResp.raw)
	}
	var initResult struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name string `json:"name"`
		} `json:"serverInfo"`
	}
	mustDecodeData(t, initResp.body.Result, &initResult)
	if initResult.ProtocolVersion != "2025-06-18" || initResult.ServerInfo.Name != "aieas-readonly-mcp" {
		t.Fatalf("unexpected initialize result: %+v", initResult)
	}

	toolsResp := doMCP(t, h.Engine, token, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	if toolsResp.status != 200 || toolsResp.body.Error != nil {
		t.Fatalf("expected tools/list success, status=%d raw=%s", toolsResp.status, toolsResp.raw)
	}
	var toolsResult struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	mustDecodeData(t, toolsResp.body.Result, &toolsResult)
	if !containsTool(toolsResult.Tools, "read_live_session_bids") || !containsTool(toolsResult.Tools, "read_live_session_settlement") {
		t.Fatalf("expected live session tools, got %+v", toolsResult.Tools)
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

	h := NewServerWithDependencies(appconfig.Default(), ServerDependencies{
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
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")

	body := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"read_live_session_bids","arguments":{"sessionId":` + strconv.FormatUint(session.ID, 10) + `,"limit":10}}}`
	merchantResp := doMCP(t, h.Engine, merchantToken, body)
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

	buyerResp := doMCP(t, h.Engine, buyerToken, body)
	if buyerResp.status != 200 || buyerResp.body.Error == nil || buyerResp.body.Error.Code != -32003 {
		t.Fatalf("expected buyer forbidden json-rpc error, status=%d raw=%s", buyerResp.status, buyerResp.raw)
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

func TestItemCreateRunsProductAudit(t *testing.T) {
	cases := []struct {
		name       string
		result     service.ProductAuditResult
		wantStatus string
	}{
		{
			name:       "approved",
			result:     service.ProductAuditResult{Success: true, IsApproved: true},
			wantStatus: "READY",
		},
		{
			name:       "rejected",
			result:     service.ProductAuditResult{Success: true, IsApproved: false},
			wantStatus: "REJECTED",
		},
		{
			name:       "manual review",
			result:     service.ProductAuditResult{Success: false},
			wantStatus: "PENDING_AUDIT",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			auditor := &captureProductAuditor{result: tt.result}
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
				Status string `json:"status"`
			}
			mustDecodeData(t, resp.body.Data, &item)
			if item.Status != tt.wantStatus {
				t.Fatalf("expected status %s, got %+v", tt.wantStatus, item)
			}
			if auditor.input.ProductText == "" || !strings.Contains(auditor.input.ProductText, "机械键盘 87键") || !strings.Contains(auditor.input.ProductText, "电脑外设/键盘") {
				t.Fatalf("unexpected audit product text: %q", auditor.input.ProductText)
			}
			if string(auditor.input.Image) != "audit image bytes" || auditor.input.ImageName != "keyboard.jpg" {
				t.Fatalf("unexpected audit image: name=%q body=%q", auditor.input.ImageName, string(auditor.input.Image))
			}
		})
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
	adminToken := loginForToken(t, h.Engine, "admin001", "AdminPassw0rd!", "admin")

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

	auctionBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":10000,"depositAmount":0,"status":"PENDING_AUDIT","incrementRule":{"type":"fixed","amount":100}}`
	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createAuction.status != 200 || createAuction.body.Code != 0 {
		t.Fatalf("expected auction create success, got status=%d raw=%s", createAuction.status, createAuction.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createAuction.body.Data, &auctionData)

	auditAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/admin/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/audit", `{"auditResult":"APPROVED"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "item-active-auction-audit"})
	if auditAuction.status != 200 || auditAuction.body.Code != 0 {
		t.Fatalf("expected auction audit success, got status=%d raw=%s", auditAuction.status, auditAuction.raw)
	}

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

func TestAuctionCreateAndUpdateRejectApprovedStatus(t *testing.T) {
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

	readyBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"depositAmount":1000,"status":"READY","incrementRule":{"type":"fixed","amount":500}}`
	createReady := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", readyBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createReady.status != 400 || createReady.body.Code != 20001 {
		t.Fatalf("expected create READY to fail validation, got status=%d code=%d raw=%s", createReady.status, createReady.body.Code, createReady.raw)
	}

	pendingBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"depositAmount":1000,"status":"PENDING_AUDIT","incrementRule":{"type":"fixed","amount":500}}`
	createPending := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", pendingBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createPending.status != 200 || createPending.body.Code != 0 {
		t.Fatalf("expected create PENDING_AUDIT success, got status=%d raw=%s", createPending.status, createPending.raw)
	}
	ladderBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":100,"reservePrice":5000,"depositAmount":0,"status":"PENDING_AUDIT","incrementRule":{"type":"ladder","steps":[{"min":0,"max":1000,"amount":100},{"min":1000,"amount":200}]}}`
	createLadder := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", ladderBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if createLadder.status != 200 || createLadder.body.Code != 0 {
		t.Fatalf("expected create ladder PENDING_AUDIT success, got status=%d raw=%s", createLadder.status, createLadder.raw)
	}
	var auctionData struct {
		AuctionID uint64 `json:"auctionId"`
	}
	mustDecodeData(t, createPending.body.Data, &auctionData)

	patchReady := doJSONWithHeaders(t, h.Engine, consts.MethodPatch, "/api/v1/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10), `{"status":"READY"}`, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
	if patchReady.status != 400 || patchReady.body.Code != 20001 {
		t.Fatalf("expected patch READY to fail validation, got status=%d code=%d raw=%s", patchReady.status, patchReady.body.Code, patchReady.raw)
	}
}

func TestAuctionRoutesStateAndIdempotencyMiddleware(t *testing.T) {
	h := newTestServer()
	merchantToken := loginForToken(t, h.Engine, "merchant001", "Passw0rd!", "merchant")
	buyerToken := loginForToken(t, h.Engine, "buyer001", "Passw0rd!", "buyer")
	adminToken := loginForToken(t, h.Engine, "admin001", "AdminPassw0rd!", "admin")

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

	auctionBody := `{"itemId":` + strconv.FormatUint(itemData.ID, 10) + `,"startPrice":10000,"reservePrice":15000,"depositAmount":1000,"status":"PENDING_AUDIT","incrementRule":{"type":"fixed","amount":500}}`
	createAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/auctions", auctionBody, ut.Header{Key: "Authorization", Value: "Bearer " + merchantToken})
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

	auditAuction := doJSONWithHeaders(t, h.Engine, consts.MethodPost, "/api/v1/admin/auctions/"+strconv.FormatUint(auctionData.AuctionID, 10)+"/audit", `{"auditResult":"APPROVED"}`, ut.Header{Key: "Authorization", Value: "Bearer " + adminToken}, ut.Header{Key: "Idempotency-Key", Value: "auction-route-audit-1"})
	if auditAuction.status != 200 || auditAuction.body.Code != 0 {
		t.Fatalf("expected auction audit success, got status=%d raw=%s", auditAuction.status, auditAuction.raw)
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
		IncrementRule:  json.RawMessage(`{"type":"fixed","amount":100}`),
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
	input  service.ProductAuditInput
	result service.ProductAuditResult
	err    error
}

func (a *captureProductAuditor) AuditProduct(ctx context.Context, in service.ProductAuditInput) (service.ProductAuditResult, error) {
	_ = ctx
	a.input = in
	if a.err != nil {
		return service.ProductAuditResult{}, a.err
	}
	return a.result, nil
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

func doMCP(t *testing.T, engine *route.Engine, token, body string) testMCPResult {
	t.Helper()
	headers := []ut.Header{{Key: "Content-Type", Value: "application/json; charset=utf-8"}}
	if token != "" {
		headers = append(headers, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	}
	resp := ut.PerformRequest(engine, consts.MethodPost, "/mcp", &ut.Body{Body: strings.NewReader(body), Len: len(body)}, headers...)
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
