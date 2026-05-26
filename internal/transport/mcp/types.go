package mcp

import (
	"encoding/json"

	"aieas_backend/internal/service"
)

const (
	protocolVersion = "2025-06-18"
	serverName      = "aieas-readonly-mcp"
	serverVersion   = "1.0.0"
	schemaVersion   = "aieas.mcp.readonly.v1"
)

type Handler struct {
	auth *service.AuthService
	read *service.MCPReadService
}

func NewHandler(auth *service.AuthService, read *service.MCPReadService) *Handler {
	return &Handler{auth: auth, read: read}
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
