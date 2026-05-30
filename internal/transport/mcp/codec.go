package mcp

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

func decodeRPCRequests(raw []byte) ([]rpcRequest, bool, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, false, errors.New("empty request body")
	}
	if raw[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, true, err
		}
		if len(items) == 0 {
			return nil, true, errors.New("empty batch")
		}
		requests := make([]rpcRequest, 0, len(items))
		for _, item := range items {
			req, err := decodeRPCRequest(item)
			if err != nil {
				return nil, true, err
			}
			requests = append(requests, req)
		}
		return requests, true, nil
	}
	req, err := decodeRPCRequest(raw)
	if err != nil {
		return nil, false, err
	}
	return []rpcRequest{req}, false, nil
}

func decodeRPCRequest(raw []byte) (rpcRequest, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return rpcRequest{}, err
	}
	var req rpcRequest
	if v, ok := fields["jsonrpc"]; ok {
		_ = json.Unmarshal(v, &req.JSONRPC)
	}
	if v, ok := fields["id"]; ok {
		req.ID = append(json.RawMessage(nil), v...)
		req.HasID = true
	}
	if v, ok := fields["method"]; ok {
		_ = json.Unmarshal(v, &req.Method)
	}
	if v, ok := fields["params"]; ok {
		req.Params = append(json.RawMessage(nil), v...)
	}
	return req, nil
}

func decodeParams(raw json.RawMessage, out interface{}) error {
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte(`{}`)
	}
	return json.Unmarshal(raw, out)
}

func (h *Handler) payloadText(traceID string, data interface{}) (string, error) {
	encoded, err := json.Marshal(payloadEnvelope{SchemaVersion: h.schemaVersion, TraceID: traceID, Data: data})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func pagePayload(items interface{}, limit, offset, count int) listPayload {
	limit = normalizeLimit(limit, 20)
	offset = normalizeOffset(offset)
	return listPayload{
		Items: items,
		Page: pageInfo{
			Limit:   limit,
			Offset:  offset,
			HasMore: count >= limit,
		},
	}
}

func normalizeLimit(limit, fallback int) int {
	if fallback <= 0 {
		fallback = 20
	}
	if limit <= 0 {
		return fallback
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func normalizeOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}

func parseUintID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("invalid id")
	}
	return id, nil
}
