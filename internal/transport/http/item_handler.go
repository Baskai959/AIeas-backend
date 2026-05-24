package http

import (
	"context"
	"errors"
	"mime"
	"mime/multipart"
	"path/filepath"
	"strconv"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

const maxImageUploadSizeBytes int64 = 2 * 1024 * 1024

type ItemHandler struct {
	items    *service.ItemService
	uploader objectstorage.Uploader
}

func NewItemHandler(items *service.ItemService, uploaders ...objectstorage.Uploader) *ItemHandler {
	var uploader objectstorage.Uploader = objectstorage.DisabledUploader{}
	if len(uploaders) > 0 && uploaders[0] != nil {
		uploader = uploaders[0]
	}
	return &ItemHandler{items: items, uploader: uploader}
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

func (h *ItemHandler) Create(ctx context.Context, c *app.RequestContext) {
	req, files, err := h.bindCreateRequest(c)
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	if len(files) > 0 {
		images, err := h.uploadImages(ctx, files)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		req.Images = images
	}
	item, err := h.items.Create(ctx, service.CreateItemInput{
		SellerID:       AuthUserID(c),
		Title:          req.Title,
		Category:       req.Category,
		Brand:          req.Brand,
		ConditionGrade: req.ConditionGrade,
		Images:         req.Images,
		Description:    req.Description,
		Status:         req.Status,
	})
	if err != nil {
		writeItemError(c, err)
		return
	}
	WriteSuccess(c, item)
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
	if len(files) > 0 {
		images, err := h.uploadImages(ctx, files)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		req.Images = &images
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
