package http

import (
	"context"
	"mime/multipart"
	"strings"

	"aieas_backend/internal/domain"
	authapp "aieas_backend/internal/modules/auth/app"

	"github.com/cloudwego/hertz/pkg/app"
)

const (
	contextUserID = "auth_user_id"
	contextRole   = "auth_role"
	contextToken  = "auth_token"
	contextExp    = "auth_exp"
)

type AuthHandler struct {
	auth     AuthUseCase
	uploader ImageUploader
}

func NewAuthHandler(auth AuthUseCase, uploaders ...ImageUploader) *AuthHandler {
	uploader := ImageUploader(DisabledImageUploader{})
	if len(uploaders) > 0 && uploaders[0] != nil {
		uploader = uploaders[0]
	}
	return &AuthHandler{auth: auth, uploader: uploader}
}

type loginRequest struct {
	Account  string      `json:"account"`
	Password string      `json:"password"`
	Role     domain.Role `json:"role"`
}

type refreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type logoutRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type updateProfileRequest struct {
	Nickname *string `json:"nickname"`
	Location *string `json:"location"`
}

func (h *AuthHandler) Login(ctx context.Context, c *app.RequestContext) {
	var req loginRequest
	if err := c.BindJSON(&req); err != nil || req.Account == "" || req.Password == "" || !req.Role.Valid() {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	result, err := h.auth.Login(req.Account, req.Password, req.Role)
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) AdminLogin(ctx context.Context, c *app.RequestContext) {
	var req loginRequest
	if err := c.BindJSON(&req); err != nil || req.Account == "" || req.Password == "" {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	result, err := h.auth.MustAdminLogin(req.Account, req.Password)
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) Me(ctx context.Context, c *app.RequestContext) {
	userID := c.GetString(contextUserID)
	user, err := h.auth.Me(userID)
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, user)
}

func (h *AuthHandler) UpdateProfile(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	var req updateProfileRequest
	if err := c.BindJSON(&req); err != nil || (req.Nickname == nil && req.Location == nil) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	user, err := h.auth.UpdateProfile(authapp.UpdateProfileInput{
		UserID:   AuthUserID(c),
		Nickname: req.Nickname,
		Location: req.Location,
	})
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, user)
}

func (h *AuthHandler) UploadAvatar(ctx context.Context, c *app.RequestContext) {
	if !isMultipartRequest(c) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	fileHeader, err := avatarFile(c)
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	avatarURL, err := h.uploadAvatar(ctx, fileHeader)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	user, err := h.auth.UpdateAvatar(AuthUserID(c), avatarURL)
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, user)
}

func (h *AuthHandler) Refresh(ctx context.Context, c *app.RequestContext) {
	var req refreshRequest
	if err := c.BindJSON(&req); err != nil || req.RefreshToken == "" {
		WriteError(c, 401, 10002, "访问令牌无效或已过期", nil)
		return
	}
	result, err := h.auth.Refresh(req.RefreshToken)
	if err != nil {
		status, code, msg := HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) Logout(ctx context.Context, c *app.RequestContext) {
	var req logoutRequest
	_ = c.BindJSON(&req)
	token := c.GetString(contextToken)
	expiresAt := c.GetInt64(contextExp)
	h.auth.Logout(token, req.RefreshToken, expiresAt)
	WriteSuccess(c, map[string]bool{"loggedOut": true})
}

func (h *AuthHandler) AuthMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		authHeader := strings.TrimSpace(string(c.GetHeader("Authorization")))
		token := strings.TrimSpace(c.Query("token"))
		if authHeader == "" && token == "" {
			AbortError(c, 401, 10001, "缺少访问令牌", nil)
			return
		}
		const prefix = "Bearer "
		if authHeader != "" {
			if !strings.HasPrefix(authHeader, prefix) {
				AbortError(c, 401, 10002, "访问令牌无效或已过期", nil)
				return
			}
			token = strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
		}
		if token == "" {
			AbortError(c, 401, 10001, "缺少访问令牌", nil)
			return
		}
		claims, err := h.auth.ParseAccessToken(token)
		if err != nil {
			AbortError(c, 401, 10002, "访问令牌无效或已过期", nil)
			return
		}
		c.Set(contextUserID, claims.Subject)
		c.Set(contextRole, claims.Role)
		c.Set(contextToken, token)
		c.Set(contextExp, claims.ExpiresAt)
		c.Next(ctx)
	}
}

func (h *AuthHandler) uploadAvatar(ctx context.Context, fileHeader *multipart.FileHeader) (string, error) {
	if fileHeader == nil || fileHeader.Size <= 0 || fileHeader.Size > maxImageUploadSizeBytes {
		return "", domain.ErrInvalidArgument
	}
	file, err := fileHeader.Open()
	if err != nil {
		return "", err
	}
	avatarURL, uploadErr := h.uploader.Upload(ctx, ImageUploadInput{Filename: fileHeader.Filename, ContentType: imageContentType(fileHeader), Size: fileHeader.Size, Body: file})
	closeErr := file.Close()
	if uploadErr != nil {
		return "", uploadErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return avatarURL, nil
}

func avatarFile(c *app.RequestContext) (*multipart.FileHeader, error) {
	if fileHeader, err := c.FormFile("avatar"); err == nil && fileHeader != nil {
		return fileHeader, nil
	}
	return c.FormFile("image")
}

func TraceMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		traceID := strings.TrimSpace(string(c.GetHeader("X-Trace-Id")))
		if traceID == "" {
			traceID = TraceID(c)
		}
		c.Set("trace_id", traceID)
		c.Next(ctx)
	}
}
