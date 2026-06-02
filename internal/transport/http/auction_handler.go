package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"path/filepath"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type AuctionHandler struct {
	auctions            *service.AuctionService
	deposits            *service.DepositService
	hammers             *service.HammerService
	uploader            objectstorage.Uploader
	descriptionGen      service.ProductDescriptionGenerator
	auditCallbackAPIKey string
}

func NewAuctionHandler(auctions *service.AuctionService, deposits *service.DepositService, hammers *service.HammerService, uploader objectstorage.Uploader, descriptionGen service.ProductDescriptionGenerator, auditCallbackAPIKey string) *AuctionHandler {
	if uploader == nil {
		uploader = objectstorage.DisabledUploader{}
	}
	if descriptionGen == nil {
		descriptionGen = service.DisabledProductDescriptionGenerator{}
	}
	return &AuctionHandler{auctions: auctions, deposits: deposits, hammers: hammers, uploader: uploader, descriptionGen: descriptionGen, auditCallbackAPIKey: strings.TrimSpace(auditCallbackAPIKey)}
}

type auctionCreateRequest struct {
	AuctionID      uint64                   `json:"auctionId"`
	SellerID       string                   `json:"sellerId"`
	Title          string                   `json:"title"`
	Description    string                   `json:"description"`
	Category       string                   `json:"category"`
	Brand          string                   `json:"brand"`
	ConditionGrade domain.ConditionGrade    `json:"condition"`
	ImageURLs      []string                 `json:"imageUrls"`
	CoverURL       string                   `json:"coverUrl"`
	AuctionType    domain.AuctionType       `json:"auctionType"`
	StartPrice     int64                    `json:"startPrice"`
	ReservePrice   int64                    `json:"reservePrice"`
	CapPrice       int64                    `json:"capPrice"`
	IncrementRule  json.RawMessage          `json:"incrementRule"`
	AntiSnipingSec int                      `json:"antiSnipingSec"`
	AntiExtendSec  int                      `json:"antiExtendSec"`
	AntiExtendMode domain.AuctionExtendMode `json:"antiExtendMode"`
	DepositAmount  int64                    `json:"depositAmount"`
	Status         domain.AuctionStatus     `json:"status"`
	StartTime      time.Time                `json:"startTime"`
	EndTime        time.Time                `json:"endTime"`
	DurationSec    int                      `json:"durationSec"`
}

type auctionPatchRequest struct {
	Title          *string                   `json:"title"`
	Description    *string                   `json:"description"`
	Category       *string                   `json:"category"`
	Brand          *string                   `json:"brand"`
	ConditionGrade *domain.ConditionGrade    `json:"condition"`
	ImageURLs      *[]string                 `json:"imageUrls"`
	CoverURL       *string                   `json:"coverUrl"`
	StartPrice     *int64                    `json:"startPrice"`
	ReservePrice   *int64                    `json:"reservePrice"`
	CapPrice       *int64                    `json:"capPrice"`
	IncrementRule  *json.RawMessage          `json:"incrementRule"`
	AntiSnipingSec *int                      `json:"antiSnipingSec"`
	AntiExtendSec  *int                      `json:"antiExtendSec"`
	AntiExtendMode *domain.AuctionExtendMode `json:"antiExtendMode"`
	DepositAmount  *int64                    `json:"depositAmount"`
	Status         *domain.AuctionStatus     `json:"status"`
	StartTime      *time.Time                `json:"startTime"`
	EndTime        *time.Time                `json:"endTime"`
	DurationSec    *int                      `json:"durationSec"`
}

type auctionAuditCallbackRequest struct {
	RequestID          string         `json:"requestId"`
	RequestIDSnake     string         `json:"request_id"`
	Status             string         `json:"status"`
	AuditResult        string         `json:"auditResult"`
	AuditResultSnake   string         `json:"audit_result"`
	Decision           string         `json:"decision"`
	Conclusion         string         `json:"conclusion"`
	Success            *bool          `json:"success"`
	IsApproved         *bool          `json:"isApproved"`
	IsApprovedSnake    *bool          `json:"is_approved"`
	RejectReasons      []string       `json:"rejectReasons"`
	RejectReasonsSnake []string       `json:"reject_reasons"`
	RiskLabels         []string       `json:"riskLabels"`
	RiskLabelsSnake    []string       `json:"risk_labels"`
	Context            map[string]any `json:"context"`
	CallbackContext    map[string]any `json:"callback_context"`
}

func (h *AuctionHandler) Create(ctx context.Context, c *app.RequestContext) {
	var req auctionCreateRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.auctions.Create(ctx, service.CreateAuctionInput{
		ActorID:        AuthUserID(c),
		ActorRole:      AuthRole(c),
		AuctionID:      req.AuctionID,
		SellerID:       req.SellerID,
		Title:          req.Title,
		Description:    req.Description,
		Category:       req.Category,
		Brand:          req.Brand,
		ConditionGrade: req.ConditionGrade,
		ImageURLs:      req.ImageURLs,
		CoverURL:       req.CoverURL,
		AuctionType:    req.AuctionType,
		StartPrice:     req.StartPrice,
		ReservePrice:   req.ReservePrice,
		CapPrice:       req.CapPrice,
		IncrementRule:  req.IncrementRule,
		AntiSnipingSec: req.AntiSnipingSec,
		AntiExtendSec:  req.AntiExtendSec,
		AntiExtendMode: req.AntiExtendMode,
		DepositAmount:  req.DepositAmount,
		Status:         req.Status,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		DurationSec:    req.DurationSec,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) AuditCallback(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeAuditCallback(c) {
		WriteError(c, 401, 10002, "访问令牌无效或已过期", nil)
		return
	}
	var req auctionAuditCallbackRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	success := auditCallbackSuccess(req)
	approved := auditCallbackApproved(req)
	result, err := h.auctions.HandleAuditCallback(ctx, service.AuctionAuditCallbackInput{
		RequestID:     firstNonEmpty(req.RequestID, req.RequestIDSnake),
		Status:        req.Status,
		Success:       success,
		IsApproved:    approved,
		RejectReasons: firstNonEmptyStringSlice(req.RejectReasons, req.RejectReasonsSnake),
		RiskLabels:    firstNonEmptyStringSlice(req.RiskLabels, req.RiskLabelsSnake),
		Context:       firstNonNilMap(req.Context, req.CallbackContext),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, result)
}

func auditCallbackSuccess(req auctionAuditCallbackRequest) bool {
	if req.Success != nil {
		return *req.Success
	}
	if req.IsApproved != nil || req.IsApprovedSnake != nil {
		return true
	}
	result := auditCallbackConclusion(req)
	return auditCallbackApprovedStatus(result) || auditCallbackRejectedStatus(result)
}

func auditCallbackApproved(req auctionAuditCallbackRequest) bool {
	if req.IsApproved != nil {
		return *req.IsApproved
	}
	if req.IsApprovedSnake != nil {
		return *req.IsApprovedSnake
	}
	return auditCallbackApprovedStatus(auditCallbackConclusion(req))
}

func auditCallbackConclusion(req auctionAuditCallbackRequest) string {
	return normalizeAuditCallbackStatus(firstNonEmpty(req.AuditResult, req.AuditResultSnake, req.Decision, req.Conclusion))
}

func normalizeAuditCallbackStatus(status string) string {
	return strings.ToUpper(strings.TrimSpace(status))
}

func auditCallbackApprovedStatus(status string) bool {
	switch status {
	case "APPROVED", "APPROVE", "ACCEPTED", "ACCEPT", "PASS", "PASSED", "ALLOW", "ALLOWED", "OK":
		return true
	default:
		return false
	}
}

func auditCallbackRejectedStatus(status string) bool {
	switch status {
	case "REJECTED", "REJECT", "DENIED", "DENY", "BLOCKED", "BLOCK":
		return true
	default:
		return false
	}
}

func (h *AuctionHandler) authorizeAuditCallback(c *app.RequestContext) bool {
	expected := strings.TrimSpace(h.auditCallbackAPIKey)
	if expected == "" {
		return false
	}
	if constantTimeStringEqual(strings.TrimSpace(string(c.GetHeader("X-Callback-Key"))), expected) {
		return true
	}
	auth := strings.TrimSpace(string(c.GetHeader("Authorization")))
	const prefix = "Bearer "
	if strings.HasPrefix(auth, prefix) {
		return constantTimeStringEqual(strings.TrimSpace(strings.TrimPrefix(auth, prefix)), expected)
	}
	return false
}

func firstNonEmptyStringSlice(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstNonNilMap(values ...map[string]any) map[string]any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func (h *AuctionHandler) List(ctx context.Context, c *app.RequestContext) {
	filter := domain.AuctionFilter{
		SellerID: strings.TrimSpace(c.Query("sellerId")),
		Limit:    parseQueryInt(c, "limit", 20),
		Offset:   parseQueryInt(c, "offset", 0),
	}
	if status := domain.AuctionStatus(strings.TrimSpace(c.Query("status"))); status.Valid() {
		filter.Status = status
	}
	filter.Category = strings.TrimSpace(c.Query("category"))
	filter.Keyword = strings.TrimSpace(c.Query("keyword"))
	if sessionID, ok := parseOptionalUintQuery(c, "liveSessionId"); ok {
		filter.LiveSessionID = sessionID
	}
	auctions, err := h.auctions.List(ctx, filter, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"auctions": auctions})
}

func (h *AuctionHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Get(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Update(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req auctionPatchRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.auctions.Update(ctx, id, service.UpdateAuctionInput{
		ActorID:        AuthUserID(c),
		ActorRole:      AuthRole(c),
		Title:          req.Title,
		Description:    req.Description,
		Category:       req.Category,
		Brand:          req.Brand,
		ConditionGrade: req.ConditionGrade,
		ImageURLs:      req.ImageURLs,
		CoverURL:       req.CoverURL,
		StartPrice:     req.StartPrice,
		ReservePrice:   req.ReservePrice,
		CapPrice:       req.CapPrice,
		IncrementRule:  req.IncrementRule,
		AntiSnipingSec: req.AntiSnipingSec,
		AntiExtendSec:  req.AntiExtendSec,
		AntiExtendMode: req.AntiExtendMode,
		DepositAmount:  req.DepositAmount,
		Status:         req.Status,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		DurationSec:    req.DurationSec,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Delete(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if err := h.auctions.Delete(ctx, id, AuthUserID(c), AuthRole(c)); err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"deleted": true})
}

func (h *AuctionHandler) Image(ctx context.Context, c *app.RequestContext) {
	key := strings.TrimLeft(strings.TrimSpace(c.Param("key")), "/")
	if key == "" || strings.Contains(key, "..") {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	out, err := h.uploader.Download(ctx, key)
	if err != nil {
		switch {
		case errors.Is(err, objectstorage.ErrInvalidObjectKey):
			WriteError(c, 400, 20001, "参数不合法", nil)
		case errors.Is(err, objectstorage.ErrObjectNotFound):
			WriteError(c, 404, 20004, "资源不存在", nil)
		default:
			WriteError(c, 500, 90001, "系统内部错误", nil)
		}
		return
	}
	contentType := strings.TrimSpace(out.ContentType)
	if contentType == "" {
		contentType = mime.TypeByExtension(strings.ToLower(filepath.Ext(key)))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	bodySize := -1
	if out.ContentLength >= 0 && out.ContentLength <= int64(^uint(0)>>1) {
		bodySize = int(out.ContentLength)
	}
	c.Response.SetStatusCode(consts.StatusOK)
	c.Response.Header.Set("Content-Type", contentType)
	c.Response.Header.Set("Cache-Control", "private, max-age=300")
	c.Response.SetBodyStream(out.Content, bodySize)
}

func (h *AuctionHandler) UploadImages(ctx context.Context, c *app.RequestContext) {
	if !isMultipartRequest(c) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	files, err := imageFiles(c)
	if err != nil || len(files) == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	urls, err := h.uploadImages(ctx, files)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	coverURL := ""
	if len(urls) > 0 {
		coverURL = urls[0]
	}
	WriteSuccess(c, utils.H{"imageUrls": urls, "coverUrl": coverURL})
}

func (h *AuctionHandler) OptimizeDescription(ctx context.Context, c *app.RequestContext) {
	if !isMultipartRequest(c) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	title := strings.TrimSpace(c.PostForm("title"))
	category := strings.TrimSpace(c.PostForm("category"))
	condition := strings.TrimSpace(c.PostForm("condition"))
	if title == "" || category == "" || condition == "" {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}

	input, err := h.bindDescriptionImage(ctx, c)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	if len(input.Image) == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	input.Title = title
	input.Category = category
	input.Condition = condition
	result, err := h.descriptionGen.GenerateProductDescription(ctx, input)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuctionHandler) bindDescriptionImage(ctx context.Context, c *app.RequestContext) (service.ProductDescriptionInput, error) {
	fileHeader, err := c.FormFile("image")
	if err == nil && fileHeader != nil {
		if fileHeader.Size > maxImageUploadSizeBytes {
			return service.ProductDescriptionInput{}, domain.ErrInvalidArgument
		}
		file, err := fileHeader.Open()
		if err != nil {
			return service.ProductDescriptionInput{}, err
		}
		imageBytes, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return service.ProductDescriptionInput{}, readErr
		}
		if closeErr != nil {
			return service.ProductDescriptionInput{}, closeErr
		}
		return service.ProductDescriptionInput{ImageName: fileHeader.Filename, ContentType: imageContentType(fileHeader), ImageSize: fileHeader.Size, Image: imageBytes}, nil
	}

	imageURL := strings.TrimSpace(c.PostForm("imageUrl"))
	if imageURL == "" {
		return service.ProductDescriptionInput{}, nil
	}
	key, err := objectKeyFromImageURL(imageURL)
	if err != nil {
		return service.ProductDescriptionInput{}, err
	}
	out, err := h.uploader.Download(ctx, key)
	if err != nil {
		return service.ProductDescriptionInput{}, err
	}
	imageBytes, readErr := io.ReadAll(out.Content)
	closeErr := out.Content.Close()
	if readErr != nil {
		return service.ProductDescriptionInput{}, readErr
	}
	if closeErr != nil {
		return service.ProductDescriptionInput{}, closeErr
	}
	return service.ProductDescriptionInput{ImageName: filepath.Base(key), ContentType: out.ContentType, ImageSize: out.ContentLength, Image: imageBytes}, nil
}

func (h *AuctionHandler) uploadImages(ctx context.Context, files []*multipart.FileHeader) ([]string, error) {
	images := make([]string, 0, len(files))
	for _, fileHeader := range files {
		if fileHeader == nil {
			continue
		}
		if fileHeader.Size > maxImageUploadSizeBytes {
			return nil, domain.ErrInvalidArgument
		}
		file, err := fileHeader.Open()
		if err != nil {
			return nil, err
		}
		url, uploadErr := h.uploader.Upload(ctx, objectstorage.UploadInput{Filename: fileHeader.Filename, ContentType: imageContentType(fileHeader), Size: fileHeader.Size, Body: file})
		closeErr := file.Close()
		if uploadErr != nil {
			return nil, uploadErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		images = append(images, url)
	}
	return images, nil
}

func (h *AuctionHandler) Start(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Start(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Enroll(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if h.deposits == nil {
		WriteError(c, 500, 90001, "系统内部错误", nil)
		return
	}
	deposit, err := h.deposits.Enroll(ctx, service.EnrollInput{
		AuctionID: id,
		UserID:    AuthUserID(c),
		UserRole:  AuthRole(c),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, deposit)
}

func (h *AuctionHandler) Hammer(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if h.hammers == nil {
		WriteError(c, 500, 90001, "系统内部错误", nil)
		return
	}
	result, order, err := h.hammers.Hammer(ctx, domain.HammerInput{
		RequestID: IdempotencyKey(c),
		AuctionID: id,
		ActorID:   AuthUserID(c),
		ActorRole: AuthRole(c),
		ClosedBy:  AuthUserID(c),
		Force:     true,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"result": result, "order": order})
}

func (h *AuctionHandler) Cancel(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Cancel(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) State(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	state, err := h.auctions.State(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, state)
}

func parseOptionalUintQuery(c *app.RequestContext, name string) (uint64, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return 0, false
	}
	id, err := parseUintString(value)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

func parseUintString(value string) (uint64, error) {
	var id uint64
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, domain.ErrInvalidArgument
		}
		id = id*10 + uint64(ch-'0')
	}
	return id, nil
}
