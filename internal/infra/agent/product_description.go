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
)

type ProductDescriptionClient struct {
	endpoint string
	client   *http.Client
}

type ProductAuditClient struct {
	endpoint string
	client   *http.Client
}

func NewProductDescriptionClient(cfg appconfig.AgentConfig) *ProductDescriptionClient {
	timeout := cfg.Timeout.Std()
	return &ProductDescriptionClient{
		endpoint: strings.TrimSpace(cfg.ProductDescriptionURL),
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func NewProductAuditClient(cfg appconfig.AgentConfig) *ProductAuditClient {
	timeout := cfg.Timeout.Std()
	return &ProductAuditClient{
		endpoint: strings.TrimSpace(cfg.ProductAuditURL),
		client: &http.Client{
			Timeout: timeout,
		},
	}
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
	if strings.TrimSpace(in.ProductText) == "" || len(in.Image) == 0 {
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

	var result service.ProductAuditResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return service.ProductAuditResult{}, err
	}
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
