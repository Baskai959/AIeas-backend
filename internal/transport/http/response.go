package http

import (
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	TraceID string      `json:"trace_id"`
}

func WriteSuccess(c *app.RequestContext, data interface{}) {
	c.JSON(200, Response{Code: 0, Message: "success", Data: data, TraceID: TraceID(c)})
}

func WriteError(c *app.RequestContext, httpStatus, code int, message string, data interface{}) {
	c.JSON(httpStatus, Response{Code: code, Message: message, Data: data, TraceID: TraceID(c)})
}

func AbortError(c *app.RequestContext, httpStatus, code int, message string, data interface{}) {
	c.AbortWithStatusJSON(httpStatus, Response{Code: code, Message: message, Data: data, TraceID: TraceID(c)})
}

func TraceID(c *app.RequestContext) string {
	if v, ok := c.Get("trace_id"); ok {
		if traceID, ok := v.(string); ok && traceID != "" {
			return traceID
		}
	}
	return fmt.Sprintf("trc_%d", time.Now().UnixNano())
}
