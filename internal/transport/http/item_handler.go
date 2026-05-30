package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

const maxImageUploadSizeBytes int64 = 2 * 1024 * 1024

type ItemHandler struct {
	items                   *service.ItemService
	uploader                objectstorage.Uploader
	descriptionGen          service.ProductDescriptionGenerator
	productAuditCallbackKey string
}

func NewItemHandler(items *service.ItemService, uploader objectstorage.Uploader, descriptionGen service.ProductDescriptionGenerator, productAuditCallbackKey string) *ItemHandler {
	if uploader == nil {
		uploader = objectstorage.DisabledUploader{}
	}
	if descriptionGen == nil {
		descriptionGen = service.DisabledProductDescriptionGenerator{}
	}
	return &ItemHandler{items: items, uploader: uploader, descriptionGen: descriptionGen, productAuditCallbackKey: strings.TrimSpace(productAuditCallbackKey)}
}

type itemCreateRequest struct {
	Title          string                `json:"title"`
	Category       string                `json:"category"`
	Brand          string                `json:"brand"`
	ConditionGrade domain.ConditionGrade `json:"conditionGrade"`
	Images         []string              `json:"images"`
	Description    string                `json:"description"`
	Status         domain.ItemStatus     `json:"status"`
}

type itemPatchRequest struct {
	Title          *string                `json:"title"`
	Category       *string                `json:"category"`
	Brand          *string                `json:"brand"`
	ConditionGrade *domain.ConditionGrade `json:"conditionGrade"`
	Images         *[]string              `json:"images"`
	Description    *string                `json:"description"`
	Status         *domain.ItemStatus     `json:"status"`
}

type productAuditCallbackRequest struct {
	ItemID          uint64                 `json:"itemId"`
	ItemIDSnake     uint64                 `json:"item_id"`
	RequestID       string                 `json:"request_id"`
	Success         bool                   `json:"success"`
	IsApproved      bool                   `json:"is_approved"`
	RejectReason    *string                `json:"reject_reason"`
	CallbackContext map[string]interface{} `json:"callback_context"`
}

func (h *ItemHandler) Create(ctx context.Context, c *app.RequestContext) {
	req, files, err := h.bindCreateRequest(c)
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	var auditImage *service.ProductAuditImage
	if len(files) > 0 {
		auditImage, err = productAuditImageFromFile(files[0])
		if err != nil {
			writeServiceError(c, err)
			return
		}
		images, err := h.uploadImages(ctx, files)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		req.Images = images
	} else if len(req.Images) > 0 {
		auditImage = h.productAuditImageFromURL(ctx, req.Images[0])
	}
	item, err := h.items.Create(ctx, service.CreateItemInput{
		SellerID:       AuthUserID(c),
		ActorRole:      AuthRole(c),
		Title:          req.Title,
		Category:       req.Category,
		Brand:          req.Brand,
		ConditionGrade: req.ConditionGrade,
		Images:         req.Images,
		Description:    req.Description,
		Status:         req.Status,
		AuditImage:     auditImage,
	})
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, item)
}

func (h *ItemHandler) AuditCallback(ctx context.Context, c *app.RequestContext) {
	if !h.authorizeProductAuditCallback(c) {
		WriteError(c, 401, 10002, "访问令牌无效或已过期", nil)
		return
	}
	var req productAuditCallbackRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	itemID := req.ItemID
	if itemID == 0 {
		itemID = req.ItemIDSnake
	}
	item, err := h.items.HandleProductAuditCallback(ctx, service.ProductAuditCallbackInput{
		ItemID:          itemID,
		Success:         req.Success,
		IsApproved:      req.IsApproved,
		RejectReason:    req.RejectReason,
		CallbackContext: req.CallbackContext,
	})
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, item)
}

func (h *ItemHandler) authorizeProductAuditCallback(c *app.RequestContext) bool {
	expected := strings.TrimSpace(h.productAuditCallbackKey)
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

func (h *ItemHandler) List(ctx context.Context, c *app.RequestContext) {
	filter := domain.ItemFilter{
		SellerID: strings.TrimSpace(c.Query("sellerId")),
		Category: strings.TrimSpace(c.Query("category")),
		Limit:    parseQueryInt(c, "limit", 20),
		Offset:   parseQueryInt(c, "offset", 0),
	}
	if status := domain.ItemStatus(strings.TrimSpace(c.Query("status"))); status.Valid() {
		filter.Status = status
	}
	items, err := h.items.List(ctx, filter, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"items": items})
}

func (h *ItemHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	item, err := h.items.Get(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, item)
}

func (h *ItemHandler) Update(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	req, files, err := h.bindPatchRequest(c)
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	var auditImage *service.ProductAuditImage
	if len(files) > 0 {
		auditImage, err = productAuditImageFromFile(files[0])
		if err != nil {
			writeServiceError(c, err)
			return
		}
		images, err := h.uploadImages(ctx, files)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		req.Images = &images
	} else if req.Images != nil && len(*req.Images) > 0 {
		auditImage = h.productAuditImageFromURL(ctx, (*req.Images)[0])
	} else {
		current, err := h.items.Get(ctx, id, AuthUserID(c), AuthRole(c))
		if err == nil {
			auditImage = h.productAuditImageFromItem(ctx, current)
		}
	}
	item, err := h.items.Update(ctx, id, service.UpdateItemInput{
		ActorID:        AuthUserID(c),
		ActorRole:      AuthRole(c),
		Title:          req.Title,
		Category:       req.Category,
		Brand:          req.Brand,
		ConditionGrade: req.ConditionGrade,
		Images:         req.Images,
		Description:    req.Description,
		Status:         req.Status,
		AuditImage:     auditImage,
	})
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, item)
}

func (h *ItemHandler) Delete(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if err := h.items.Delete(ctx, id, AuthUserID(c), AuthRole(c)); err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"deleted": true})
}

func (h *ItemHandler) Image(ctx context.Context, c *app.RequestContext) {
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

func (h *ItemHandler) OptimizeDescription(ctx context.Context, c *app.RequestContext) {
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

	input, closer, err := h.bindDescriptionImage(ctx, c)
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
	result, genErr := h.descriptionGen.GenerateProductDescription(ctx, service.ProductDescriptionInput{
		Title:       input.Title,
		Category:    input.Category,
		Condition:   input.Condition,
		ImageName:   input.ImageName,
		ContentType: input.ContentType,
		ImageSize:   input.ImageSize,
		Image:       input.Image,
	})
	closeErr := closer()
	if genErr != nil {
		writeServiceError(c, genErr)
		return
	}
	if closeErr != nil {
		writeServiceError(c, closeErr)
		return
	}
	WriteSuccess(c, result)
}

func (h *ItemHandler) bindDescriptionImage(ctx context.Context, c *app.RequestContext) (service.ProductDescriptionInput, func() error, error) {
	fileHeader, err := c.FormFile("image")
	if err == nil && fileHeader != nil {
		if fileHeader.Size > maxImageUploadSizeBytes {
			return service.ProductDescriptionInput{}, noopClose, domain.ErrInvalidArgument
		}
		file, err := fileHeader.Open()
		if err != nil {
			return service.ProductDescriptionInput{}, noopClose, err
		}
		imageBytes, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return service.ProductDescriptionInput{}, noopClose, readErr
		}
		if closeErr != nil {
			return service.ProductDescriptionInput{}, noopClose, closeErr
		}
		return service.ProductDescriptionInput{
			ImageName:   fileHeader.Filename,
			ContentType: imageContentType(fileHeader),
			ImageSize:   fileHeader.Size,
			Image:       imageBytes,
		}, noopClose, nil
	}

	imageURL := strings.TrimSpace(c.PostForm("imageUrl"))
	if imageURL == "" {
		return service.ProductDescriptionInput{}, noopClose, nil
	}
	key, err := objectKeyFromImageURL(imageURL)
	if err != nil {
		return service.ProductDescriptionInput{}, noopClose, err
	}
	out, err := h.uploader.Download(ctx, key)
	if err != nil {
		return service.ProductDescriptionInput{}, noopClose, err
	}
	imageBytes, readErr := io.ReadAll(out.Content)
	closeErr := out.Content.Close()
	if readErr != nil {
		return service.ProductDescriptionInput{}, noopClose, readErr
	}
	if closeErr != nil {
		return service.ProductDescriptionInput{}, noopClose, closeErr
	}
	return service.ProductDescriptionInput{
		ImageName:   filepath.Base(key),
		ContentType: out.ContentType,
		ImageSize:   out.ContentLength,
		Image:       imageBytes,
	}, noopClose, nil
}

func productAuditImageFromFile(fileHeader *multipart.FileHeader) (*service.ProductAuditImage, error) {
	if fileHeader == nil {
		return nil, nil
	}
	if fileHeader.Size > maxImageUploadSizeBytes {
		return nil, domain.ErrInvalidArgument
	}
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	imageBytes, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(imageBytes) == 0 {
		return nil, domain.ErrInvalidArgument
	}
	return &service.ProductAuditImage{
		ImageName:   fileHeader.Filename,
		ContentType: imageContentType(fileHeader),
		ImageSize:   fileHeader.Size,
		Image:       imageBytes,
	}, nil
}

func (h *ItemHandler) productAuditImageFromItem(ctx context.Context, item domain.Item) *service.ProductAuditImage {
	var images []string
	if err := json.Unmarshal(item.Images, &images); err != nil || len(images) == 0 {
		return nil
	}
	return h.productAuditImageFromURL(ctx, images[0])
}

func (h *ItemHandler) productAuditImageFromURL(ctx context.Context, imageURL string) *service.ProductAuditImage {
	key, err := objectKeyFromImageURL(imageURL)
	if err != nil {
		return nil
	}
	out, err := h.uploader.Download(ctx, key)
	if err != nil {
		return nil
	}
	imageBytes, readErr := io.ReadAll(out.Content)
	closeErr := out.Content.Close()
	if readErr != nil || closeErr != nil || len(imageBytes) == 0 {
		return nil
	}
	return &service.ProductAuditImage{
		ImageName:   filepath.Base(key),
		ContentType: out.ContentType,
		ImageSize:   out.ContentLength,
		Image:       imageBytes,
	}
}

func objectKeyFromImageURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", domain.ErrInvalidArgument
	}
	if strings.HasPrefix(value, objectstorage.ProxyPathPrefix()) {
		return strings.TrimLeft(strings.TrimPrefix(value, objectstorage.ProxyPathPrefix()), "/"), nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", domain.ErrInvalidArgument
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return "", domain.ErrInvalidArgument
	}
	if strings.HasPrefix(parsed.Path, objectstorage.ProxyPathPrefix()) {
		return strings.TrimLeft(strings.TrimPrefix(parsed.Path, objectstorage.ProxyPathPrefix()), "/"), nil
	}
	return strings.TrimLeft(parsed.Path, "/"), nil
}

func noopClose() error {
	return nil
}

func (h *ItemHandler) bindCreateRequest(c *app.RequestContext) (itemCreateRequest, []*multipart.FileHeader, error) {
	var req itemCreateRequest
	if !isMultipartRequest(c) {
		if err := c.BindJSON(&req); err != nil {
			return itemCreateRequest{}, nil, err
		}
		return req, nil, nil
	}

	req.Title = c.PostForm("title")
	req.Category = c.PostForm("category")
	req.Brand = c.PostForm("brand")
	req.ConditionGrade = domain.ConditionGrade(c.PostForm("conditionGrade"))
	req.Description = c.PostForm("description")
	req.Status = domain.ItemStatus(c.PostForm("status"))
	files, err := imageFiles(c)
	if err != nil {
		return itemCreateRequest{}, nil, err
	}
	return req, files, nil
}

func (h *ItemHandler) bindPatchRequest(c *app.RequestContext) (itemPatchRequest, []*multipart.FileHeader, error) {
	var req itemPatchRequest
	if !isMultipartRequest(c) {
		if err := c.BindJSON(&req); err != nil {
			return itemPatchRequest{}, nil, err
		}
		return req, nil, nil
	}

	if value, ok := c.GetPostForm("title"); ok {
		req.Title = &value
	}
	if value, ok := c.GetPostForm("category"); ok {
		req.Category = &value
	}
	if value, ok := c.GetPostForm("brand"); ok {
		req.Brand = &value
	}
	if value, ok := c.GetPostForm("conditionGrade"); ok {
		condition := domain.ConditionGrade(value)
		req.ConditionGrade = &condition
	}
	if value, ok := c.GetPostForm("description"); ok {
		req.Description = &value
	}
	if value, ok := c.GetPostForm("status"); ok {
		status := domain.ItemStatus(value)
		req.Status = &status
	}
	files, err := imageFiles(c)
	if err != nil {
		return itemPatchRequest{}, nil, err
	}
	return req, files, nil
}

func (h *ItemHandler) uploadImages(ctx context.Context, files []*multipart.FileHeader) ([]string, error) {
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
		url, uploadErr := h.uploader.Upload(ctx, objectstorage.UploadInput{
			Filename:    fileHeader.Filename,
			ContentType: imageContentType(fileHeader),
			Size:        fileHeader.Size,
			Body:        file,
		})
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

func isMultipartRequest(c *app.RequestContext) bool {
	contentType := string(c.GetHeader("Content-Type"))
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data")
	}
	return strings.EqualFold(mediaType, "multipart/form-data")
}

func imageFiles(c *app.RequestContext) ([]*multipart.FileHeader, error) {
	form, err := c.MultipartForm()
	if err != nil {
		return nil, err
	}
	files := make([]*multipart.FileHeader, 0)
	files = append(files, form.File["images"]...)
	files = append(files, form.File["image"]...)
	return files, nil
}

func imageContentType(fileHeader *multipart.FileHeader) string {
	contentType := strings.TrimSpace(fileHeader.Header.Get("Content-Type"))
	if contentType != "" {
		return contentType
	}
	return mime.TypeByExtension(strings.ToLower(filepath.Ext(fileHeader.Filename)))
}

func parseUintParam(c *app.RequestContext, name string) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param(name), 10, 64)
	if err != nil || id == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return 0, false
	}
	return id, true
}

func parseQueryInt(c *app.RequestContext, name string, fallback int) int {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func writeServiceError(c *app.RequestContext, err error) {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	if errors.Is(err, objectstorage.ErrInvalidObjectKey) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	if errors.Is(err, objectstorage.ErrObjectNotFound) {
		WriteError(c, 404, 20004, "资源不存在", nil)
		return
	}
	status, code, msg := service.HTTPStatusAndCode(err)
	WriteError(c, status, code, msg, nil)
}

func writeItemError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		WriteError(c, 404, 30001, "商品不存在", nil)
	case errors.Is(err, domain.ErrForbidden):
		WriteError(c, 403, 30002, "无商品操作权限", nil)
	case errors.Is(err, domain.ErrInvalidState):
		WriteError(c, 409, 30003, "商品状态不允许操作", nil)
	default:
		writeServiceError(c, err)
	}
}
