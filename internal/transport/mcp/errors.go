package mcp

import (
	"errors"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"
)

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603

	rpcUnauthorized = -32001
	rpcForbidden    = -32003
	rpcNotFound     = -32004
	rpcConflict     = -32009
)

func rpcErrorFor(err error, traceID string) *rpcError {
	if err == nil {
		return nil
	}
	httpStatus, businessCode, message := service.HTTPStatusAndCode(err)
	code := rpcInternalError
	switch {
	case errors.Is(err, domain.ErrTokenMissing), errors.Is(err, domain.ErrTokenInvalid):
		code = rpcUnauthorized
		message = "unauthorized"
	case errors.Is(err, domain.ErrForbidden):
		code = rpcForbidden
		message = "forbidden"
	case errors.Is(err, domain.ErrInvalidArgument):
		code = rpcInvalidParams
		message = "invalid params"
	case errors.Is(err, domain.ErrUserNotFound), errors.Is(err, domain.ErrNotFound):
		code = rpcNotFound
		message = "not found"
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalidState):
		code = rpcConflict
		message = "conflict"
	case httpStatus >= 500:
		message = "internal error"
	}
	return &rpcError{
		Code:    code,
		Message: message,
		Data: errorData{
			TraceID:      traceID,
			BusinessCode: businessCode,
			Detail:       err.Error(),
		},
	}
}

func protocolError(code int, message, traceID, detail string) *rpcError {
	return &rpcError{
		Code:    code,
		Message: message,
		Data: errorData{
			TraceID: traceID,
			Detail:  detail,
		},
	}
}
