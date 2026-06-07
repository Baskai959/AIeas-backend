package mcp

import (
	"encoding/json"

	"aieas_backend/internal/infra/observability/metrics"
	aiports "aieas_backend/internal/modules/ai/ports"
	mcpapp "aieas_backend/internal/modules/mcp/app"
)

const (
	protocolVersion      = "2025-06-18"
	readServerName       = "aieas-read-mcp"
	controlServerName    = "aieas-control-mcp"
	serverVersion        = "1.2.0"
	readSchemaVersion    = "aieas.mcp.read.v1"
	controlSchemaVersion = "aieas.mcp.control.v1"
)

type Handler struct {
	read          MCPReadUseCase
	control       MCPControlUseCase
	apiKey        string
	apiActor      MCPActor
	serverName    string
	schemaVersion string
	metrics       *metrics.Registry
	assistant     AIAssistantNotifier
}

type MCPActor = mcpapp.MCPActor

type MCPReadUseCase = mcpapp.MCPReadUseCase

type MCPControlUseCase = mcpapp.MCPControlUseCase

type LiveControlContext = mcpapp.LiveControlContext

type LiveLotOperationInput = mcpapp.LiveLotOperationInput

type LiveLotOperationResult = mcpapp.LiveLotOperationResult

type LiveVoiceBroadcastInput = mcpapp.LiveVoiceBroadcastInput

type LiveVoiceBroadcastResult = mcpapp.LiveVoiceBroadcastResult

type AIAssistantNotifier = aiports.StatusNotifier

type APIKeyAuthConfig struct {
	APIKey string
	Actor  MCPActor
}

func NewReadHandler(read MCPReadUseCase, auth APIKeyAuthConfig) *Handler {
	return newHandler(read, nil, readServerName, readSchemaVersion, auth)
}

func NewControlHandler(control MCPControlUseCase, auth APIKeyAuthConfig) *Handler {
	return newHandler(nil, control, controlServerName, controlSchemaVersion, auth)
}

func newHandler(read MCPReadUseCase, control MCPControlUseCase, name, schema string, auth APIKeyAuthConfig) *Handler {
	return &Handler{
		read:          read,
		control:       control,
		apiKey:        auth.APIKey,
		apiActor:      auth.Actor,
		serverName:    name,
		schemaVersion: schema,
		metrics:       metrics.Default(),
	}
}

// SetMetrics 注入业务指标 Registry，用于上报 MCP tool/resource 调用情况。
// 传入 nil 时回退到 noop，确保 handler 在未启用 metrics 时仍可使用。
func (h *Handler) SetMetrics(reg *metrics.Registry) {
	if reg == nil {
		h.metrics = metrics.Default()
		return
	}
	h.metrics = reg
}

func (h *Handler) SetAIAssistant(assistant AIAssistantNotifier) {
	h.assistant = assistant
}

type rpcRequest struct {
	JSONRPC string
	ID      json.RawMessage
	Method  string
	Params  json.RawMessage
	HasID   bool
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type errorData struct {
	TraceID      string `json:"traceId"`
	BusinessCode int    `json:"businessCode,omitempty"`
	Detail       string `json:"detail,omitempty"`
}

type payloadEnvelope struct {
	SchemaVersion string      `json:"schemaVersion"`
	TraceID       string      `json:"traceId"`
	Data          interface{} `json:"data"`
}

type pageInfo struct {
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	HasMore bool `json:"hasMore"`
}

type listPayload struct {
	Items interface{} `json:"items"`
	Page  pageInfo    `json:"page"`
}

type resourceContent struct {
	URI      string `json:"uri"`
	MIMEType string `json:"mimeType"`
	Text     string `json:"text"`
}

type resourceReadResult struct {
	Contents []resourceContent `json:"contents"`
}

type textContent struct {
	Type     string `json:"type"`
	MIMEType string `json:"mimeType,omitempty"`
	Text     string `json:"text"`
}

type toolCallResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError"`
}

type resourceTemplate struct {
	URITemplate string `json:"uriTemplate"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MIMEType    string `json:"mimeType,omitempty"`
}

type toolDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}
