package mcp

import (
	"context"
	"encoding/json"
	"strings"

	"aieas_backend/internal/service"
	httptransport "aieas_backend/internal/transport/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (h *Handler) Post(ctx context.Context, c *app.RequestContext) {
	traceID := httptransport.TraceID(c)
	requests, batch, err := decodeRPCRequests(c.Request.Body())
	if err != nil {
		c.JSON(consts.StatusBadRequest, rpcResponse{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   protocolError(rpcParseError, "parse error", traceID, err.Error()),
		})
		return
	}
	actor, err := h.actorFromRequest(c)
	if err != nil {
		h.writeAuthError(c, requests, batch, err, traceID)
		return
	}
	responses := make([]rpcResponse, 0, len(requests))
	for _, req := range requests {
		resp, ok := h.handleRequest(ctx, req, actor, traceID)
		if ok {
			responses = append(responses, resp)
		}
	}
	if len(responses) == 0 {
		c.Status(consts.StatusAccepted)
		return
	}
	if batch {
		c.JSON(consts.StatusOK, responses)
		return
	}
	c.JSON(consts.StatusOK, responses[0])
}

func (h *Handler) Get(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	traceID := httptransport.TraceID(c)
	c.Response.Header.Set("Allow", "POST")
	c.JSON(consts.StatusMethodNotAllowed, rpcResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage("null"),
		Error:   protocolError(rpcMethodNotFound, "streaming GET is not enabled", traceID, "use POST /mcp/read or /mcp/control for MCP calls"),
	})
}

func (h *Handler) writeAuthError(c *app.RequestContext, requests []rpcRequest, batch bool, err error, traceID string) {
	status, _, _ := service.HTTPStatusAndCode(err)
	if len(requests) == 0 {
		c.JSON(status, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: rpcErrorFor(err, traceID)})
		return
	}
	responses := make([]rpcResponse, 0, len(requests))
	for _, req := range requests {
		if !req.HasID {
			continue
		}
		responses = append(responses, rpcResponse{JSONRPC: "2.0", ID: responseID(req), Error: rpcErrorFor(err, traceID)})
	}
	if len(responses) == 0 {
		c.Status(status)
		return
	}
	if batch {
		c.JSON(status, responses)
		return
	}
	c.JSON(status, responses[0])
}

func (h *Handler) handleRequest(ctx context.Context, req rpcRequest, actor service.MCPActor, traceID string) (rpcResponse, bool) {
	if strings.TrimSpace(req.Method) == "" {
		return errorResponse(req, protocolError(rpcInvalidRequest, "invalid request", traceID, "method is required")), true
	}
	if !req.HasID && strings.HasPrefix(req.Method, "notifications/") {
		return rpcResponse{}, false
	}
	switch req.Method {
	case "initialize":
		return successResponse(req, h.initializeResult()), true
	case "ping":
		return successResponse(req, utils.H{}), true
	case "notifications/initialized":
		if !req.HasID {
			return rpcResponse{}, false
		}
		return successResponse(req, utils.H{}), true
	case "resources/templates/list":
		if h.read == nil {
			return errorResponse(req, protocolError(rpcMethodNotFound, "method not found", traceID, req.Method)), true
		}
		return successResponse(req, utils.H{"resourceTemplates": resourceTemplates()}), true
	case "resources/list":
		if h.read == nil {
			return errorResponse(req, protocolError(rpcMethodNotFound, "method not found", traceID, req.Method)), true
		}
		return successResponse(req, utils.H{"resources": []interface{}{}}), true
	case "resources/read":
		if h.read == nil {
			return errorResponse(req, protocolError(rpcMethodNotFound, "method not found", traceID, req.Method)), true
		}
		var params resourcesReadParams
		if err := decodeParams(req.Params, &params); err != nil || strings.TrimSpace(params.URI) == "" {
			return errorResponse(req, protocolError(rpcInvalidParams, "invalid params", traceID, "uri is required")), true
		}
		result, err := h.readResource(ctx, params.URI, actor, traceID)
		if err != nil {
			return errorResponse(req, rpcErrorFor(err, traceID)), true
		}
		return successResponse(req, result), true
	case "tools/list":
		return successResponse(req, utils.H{"tools": h.toolDefinitions()}), true
	case "tools/call":
		var params toolsCallParams
		if err := decodeParams(req.Params, &params); err != nil || strings.TrimSpace(params.Name) == "" {
			return errorResponse(req, protocolError(rpcInvalidParams, "invalid params", traceID, "tool name is required")), true
		}
		result, err := h.callTool(ctx, params.Name, params.Arguments, actor, traceID)
		if err != nil {
			return errorResponse(req, rpcErrorFor(err, traceID)), true
		}
		return successResponse(req, result), true
	case "prompts/list":
		return successResponse(req, utils.H{"prompts": []interface{}{}}), true
	default:
		return errorResponse(req, protocolError(rpcMethodNotFound, "method not found", traceID, req.Method)), true
	}
}

func (h *Handler) initializeResult() utils.H {
	capabilities := utils.H{
		"tools":   utils.H{},
		"prompts": utils.H{},
	}
	if h.read != nil {
		capabilities["resources"] = utils.H{}
	}
	return utils.H{
		"protocolVersion": protocolVersion,
		"capabilities":    capabilities,
		"serverInfo": utils.H{
			"name":    h.serverName,
			"version": serverVersion,
		},
	}
}

func successResponse(req rpcRequest, result interface{}) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: responseID(req), Result: result}
}

func errorResponse(req rpcRequest, err *rpcError) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: responseID(req), Error: err}
}

func responseID(req rpcRequest) json.RawMessage {
	if req.HasID && len(req.ID) > 0 {
		return req.ID
	}
	return json.RawMessage("null")
}
