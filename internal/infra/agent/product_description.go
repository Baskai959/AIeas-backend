package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type ProductDescriptionClient struct {
	endpoint string
	client   *http.Client
}

type ProductAuditClient struct {
	endpoint string
	client   *http.Client
}

type LiveAnalysisClient struct {
	endpoint string
	client   *http.Client
}

type LiveAuctionHookClient struct {
	endpoint string
	client   *http.Client
}

func NewProductDescriptionClient(cfg appconfig.AgentConfig) *ProductDescriptionClient {
	timeout := cfg.Timeout.Std()
	return &ProductDescriptionClient{
		endpoint: strings.TrimSpace(cfg.ProductDescriptionURL),
		client: &http.Client{
			Timeout:   timeout,
			Transport: newAgentTransport("agent.product_description"),
		},
	}
}

func NewProductAuditClient(cfg appconfig.AgentConfig) *ProductAuditClient {
	timeout := cfg.Timeout.Std()
	return &ProductAuditClient{
		endpoint: strings.TrimSpace(cfg.ProductAuditURL),
		client: &http.Client{
			Timeout:   timeout,
			Transport: newAgentTransport("agent.product_audit"),
		},
	}
}

func NewLiveAnalysisClient(cfg appconfig.AgentConfig) *LiveAnalysisClient {
	timeout := cfg.Timeout.Std()
	return &LiveAnalysisClient{
		endpoint: strings.TrimSpace(cfg.LiveAnalysisURL),
		client: &http.Client{
			Timeout:   timeout,
			Transport: newAgentTransport("agent.live_analysis"),
		},
	}
}

func NewLiveAuctionHookClient(cfg appconfig.AgentConfig) *LiveAuctionHookClient {
	timeout := cfg.Timeout.Std()
	return &LiveAuctionHookClient{
		endpoint: strings.TrimSpace(cfg.LiveAuctionHookURL),
		client: &http.Client{
			Timeout:   timeout,
			Transport: newAgentTransport("agent.live_auction_hook"),
		},
	}
}

// newAgentTransport 用 otelhttp.NewTransport 包裹 http.DefaultTransport，
// 让所有 outbound agent 调用自动产生 client kind span，并注入 W3C traceparent
// 头实现跨进程追踪。spanName 在每次请求作为 span 名稳定上报（不混入 URL，
// 避免高基数）。
func newAgentTransport(spanName string) http.RoundTripper {
	return otelhttp.NewTransport(http.DefaultTransport,
		otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
			return spanName
		}),
	)
}

func (c *ProductDescriptionClient) GenerateProductDescription(ctx context.Context, in service.ProductDescriptionInput) (service.ProductDescriptionResult, error) {
	if c == nil || c.client == nil || c.endpoint == "" {
		return service.ProductDescriptionResult{}, service.ErrProductDescriptionUnavailable
	}
	if strings.TrimSpace(in.Title) == "" || strings.TrimSpace(in.Category) == "" || strings.TrimSpace(in.Condition) == "" || len(in.Image) == 0 {
		return service.ProductDescriptionResult{}, domain.ErrInvalidArgument
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writeImagePart(writer, "image", in.ImageName, in.ContentType, bytes.NewReader(in.Image)); err != nil {
		return service.ProductDescriptionResult{}, err
	}
	if err := writer.WriteField("title", strings.TrimSpace(in.Title)); err != nil {
		return service.ProductDescriptionResult{}, err
	}
	if err := writer.WriteField("category", strings.TrimSpace(in.Category)); err != nil {
		return service.ProductDescriptionResult{}, err
	}
	if err := writer.WriteField("condition", strings.TrimSpace(in.Condition)); err != nil {
		return service.ProductDescriptionResult{}, err
	}
	if err := writer.Close(); err != nil {
		return service.ProductDescriptionResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, &body)
	if err != nil {
		return service.ProductDescriptionResult{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.client.Do(req)
	if err != nil {
		return service.ProductDescriptionResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return service.ProductDescriptionResult{}, fmt.Errorf("agent product description status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}

	var result service.ProductDescriptionResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return service.ProductDescriptionResult{}, err
	}
	result.Title = strings.TrimSpace(result.Title)
	result.Category = strings.TrimSpace(result.Category)
	result.Description = strings.TrimSpace(result.Description)
	if result.Description == "" {
		return service.ProductDescriptionResult{}, fmt.Errorf("agent product description response missing description")
	}
	return result, nil
}

func (c *ProductAuditClient) AuditProduct(ctx context.Context, in service.ProductAuditInput) (service.ProductAuditResult, error) {
	if c == nil || c.client == nil || c.endpoint == "" {
		return service.ProductAuditResult{}, service.ErrProductAuditUnavailable
	}
	if strings.TrimSpace(in.ProductText) == "" || len(in.Image) == 0 || strings.TrimSpace(in.CallbackURL) == "" {
		return service.ProductAuditResult{}, domain.ErrInvalidArgument
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writeImagePart(writer, "image", in.ImageName, in.ContentType, bytes.NewReader(in.Image)); err != nil {
		return service.ProductAuditResult{}, err
	}
	if err := writer.WriteField("product_text", strings.TrimSpace(in.ProductText)); err != nil {
		return service.ProductAuditResult{}, err
	}
	if err := writer.WriteField("callback_url", strings.TrimSpace(in.CallbackURL)); err != nil {
		return service.ProductAuditResult{}, err
	}
	if len(in.CallbackHeaders) > 0 {
		headers, err := json.Marshal(in.CallbackHeaders)
		if err != nil {
			return service.ProductAuditResult{}, err
		}
		if err := writer.WriteField("callback_headers", string(headers)); err != nil {
			return service.ProductAuditResult{}, err
		}
	}
	if len(in.CallbackContext) > 0 {
		callbackContext, err := json.Marshal(in.CallbackContext)
		if err != nil {
			return service.ProductAuditResult{}, err
		}
		if err := writer.WriteField("callback_context", string(callbackContext)); err != nil {
			return service.ProductAuditResult{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return service.ProductAuditResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, &body)
	if err != nil {
		return service.ProductAuditResult{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.client.Do(req)
	if err != nil {
		return service.ProductAuditResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return service.ProductAuditResult{}, fmt.Errorf("agent product audit status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return service.ProductAuditResult{}, err
	}
	if strings.TrimSpace(string(payload)) == "" {
		return service.ProductAuditResult{Success: true, Status: "ACCEPTED"}, nil
	}
	var result service.ProductAuditResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return service.ProductAuditResult{}, err
	}
	result.RequestID = strings.TrimSpace(result.RequestID)
	result.Status = strings.TrimSpace(result.Status)
	result.Message = strings.TrimSpace(result.Message)
	if result.RejectReason != nil {
		reason := strings.TrimSpace(*result.RejectReason)
		if reason == "" {
			result.RejectReason = nil
		} else {
			result.RejectReason = &reason
		}
	}
	return result, nil
}

func (c *LiveAnalysisClient) RequestLiveAnalysis(ctx context.Context, in service.LiveAnalysisAsyncInput) (service.LiveAnalysisAsyncResult, error) {
	if c == nil || c.client == nil || c.endpoint == "" {
		return service.LiveAnalysisAsyncResult{}, service.ErrLiveAnalysisUnavailable
	}
	prompt := strings.TrimSpace(in.Prompt)
	callbackURL := strings.TrimSpace(in.CallbackURL)
	if prompt == "" || callbackURL == "" {
		return service.LiveAnalysisAsyncResult{}, domain.ErrInvalidArgument
	}
	payload, err := json.Marshal(liveAnalysisAsyncRequest{
		Prompt:          prompt,
		CallbackURL:     callbackURL,
		CallbackHeaders: in.CallbackHeaders,
		CallbackContext: in.CallbackContext,
		ToolName:        strings.TrimSpace(in.ToolName),
		ToolArguments:   in.ToolArguments,
	})
	if err != nil {
		return service.LiveAnalysisAsyncResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return service.LiveAnalysisAsyncResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return service.LiveAnalysisAsyncResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return service.LiveAnalysisAsyncResult{}, fmt.Errorf("agent live analysis async status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}

	var result struct {
		Success   bool   `json:"success"`
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&result); err != nil {
		return service.LiveAnalysisAsyncResult{}, err
	}
	if !result.Success {
		message := strings.TrimSpace(result.Message)
		if message == "" {
			message = "unknown error"
		}
		return service.LiveAnalysisAsyncResult{}, fmt.Errorf("agent live analysis async failed: %s", message)
	}
	if strings.TrimSpace(result.RequestID) == "" {
		return service.LiveAnalysisAsyncResult{}, fmt.Errorf("agent live analysis async response missing request_id")
	}
	return service.LiveAnalysisAsyncResult{
		RequestID: strings.TrimSpace(result.RequestID),
		Status:    strings.TrimSpace(result.Status),
		Message:   strings.TrimSpace(result.Message),
	}, nil
}

func (c *LiveAuctionHookClient) InvokeLiveAgentHook(ctx context.Context, message string) error {
	if c == nil || c.client == nil || c.endpoint == "" {
		return nil
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return domain.ErrInvalidArgument
	}
	payload, err := json.Marshal(liveAuctionHookRequest{Message: message})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("agent live auction hook status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}
	return nil
}

type liveAnalysisAsyncRequest struct {
	Prompt          string                 `json:"prompt"`
	CallbackURL     string                 `json:"callback_url"`
	CallbackHeaders map[string]string      `json:"callback_headers,omitempty"`
	CallbackContext map[string]interface{} `json:"callback_context,omitempty"`
	ToolName        string                 `json:"tool_name,omitempty"`
	ToolArguments   map[string]interface{} `json:"tool_arguments,omitempty"`
}

type liveAuctionHookRequest struct {
	Message string `json:"message"`
}

func writeImagePart(writer *multipart.Writer, fieldName, imageName, contentType string, image io.Reader) error {
	filename := strings.TrimSpace(imageName)
	if filename == "" {
		filename = "image"
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, escapeQuotes(fieldName), escapeQuotes(filename)))
	if contentType := strings.TrimSpace(contentType); contentType != "" {
		header.Set("Content-Type", contentType)
	}
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(part, image)
	return err
}

func escapeQuotes(value string) string {
	return strings.NewReplacer("\\", "\\\\", `"`, "\\\"").Replace(value)
}
