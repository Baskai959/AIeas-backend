package http

import (
	"errors"
	"mime"
	"mime/multipart"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"aieas_backend/internal/domain"
	aiapp "aieas_backend/internal/modules/ai/app"
	auctionapp "aieas_backend/internal/modules/auction/app"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	livesessionapp "aieas_backend/internal/modules/live_session/app"

	"github.com/cloudwego/hertz/pkg/app"
)

const maxImageUploadSizeBytes int64 = 10 << 20

func writeServiceError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, domain.ErrIdempotencyKey):
		WriteError(c, 400, 20011, "缺少幂等键", nil)
	case errors.Is(err, domain.ErrInvalidArgument), errors.Is(err, ErrInvalidImageObjectKey):
		WriteError(c, 400, 20001, "参数不合法", nil)
	case errors.Is(err, domain.ErrForbidden):
		WriteError(c, 403, 20003, "无操作权限", nil)
	case errors.Is(err, aiapp.ErrUserRejected):
		WriteError(c, 403, 20003, "用户拒绝执行", nil)
	case errors.Is(err, aiapp.ErrApprovalTimeout):
		WriteError(c, 409, 20010, "审批请求已自动处理或已过期", nil)
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrUserNotFound), errors.Is(err, ErrImageObjectNotFound):
		WriteError(c, 404, 20004, "资源不存在", nil)
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrOptimisticConflict):
		WriteError(c, 409, 20012, "资源冲突", nil)
	case errors.Is(err, domain.ErrInvalidState), errors.Is(err, livesessionapp.ErrLiveSessionBusy), errors.Is(err, livesessionapp.ErrLotAlreadyMounted):
		WriteError(c, 409, 20010, "当前状态不允许此操作", nil)
	case errors.Is(err, aiapp.ErrProductDescriptionUnavailable), errors.Is(err, auctionapp.ErrProductAuditUnavailable), errors.Is(err, liveanalysisports.ErrLiveAnalysisUnavailable), errors.Is(err, ErrImageStorageDisabled):
		WriteError(c, 503, 90002, "服务暂不可用", nil)
	default:
		WriteError(c, 500, 90001, "系统内部错误", nil)
	}
}

func parseUintParam(c *app.RequestContext, name string) (uint64, bool) {
	value := strings.TrimSpace(c.Param(name))
	id, err := strconv.ParseUint(value, 10, 64)
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
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func parseQueryUint(c *app.RequestContext, name string) uint64 {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func isMultipartRequest(c *app.RequestContext) bool {
	return strings.Contains(strings.ToLower(string(c.GetHeader("Content-Type"))), "multipart/form-data")
}

func imageFiles(c *app.RequestContext) ([]*multipart.FileHeader, error) {
	form, err := c.MultipartForm()
	if err != nil {
		return nil, err
	}
	if form == nil || len(form.File) == 0 {
		return nil, nil
	}
	files := make([]*multipart.FileHeader, 0)
	for _, field := range []string{"images", "files", "image"} {
		files = append(files, form.File[field]...)
	}
	return files, nil
}

func imageContentType(fileHeader *multipart.FileHeader) string {
	if fileHeader == nil {
		return "application/octet-stream"
	}
	contentType := strings.TrimSpace(fileHeader.Header.Get("Content-Type"))
	if contentType != "" {
		return contentType
	}
	if ext := strings.ToLower(filepath.Ext(fileHeader.Filename)); ext != "" {
		if detected := mime.TypeByExtension(ext); detected != "" {
			return detected
		}
	}
	return "application/octet-stream"
}

func objectKeyFromImageURL(imageURL string) (string, error) {
	value := strings.TrimSpace(imageURL)
	if value == "" {
		return "", domain.ErrInvalidArgument
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Path != "" {
		value = parsed.Path
	}
	prefix := ImageProxyPathPrefix
	if !strings.HasPrefix(value, prefix) {
		return "", domain.ErrInvalidArgument
	}
	key := strings.TrimPrefix(value, prefix)
	if strings.TrimSpace(key) == "" || strings.Contains(key, "..") {
		return "", domain.ErrInvalidArgument
	}
	return key, nil
}
