package mcp

func resourceTemplates() []resourceTemplate {
	return []resourceTemplate{
		{URITemplate: "aieas://users/{userId}", Name: "user", Description: "用户安全信息", MIMEType: "application/json"},
		{URITemplate: "aieas://users?role={role}&status={status}&keyword={keyword}&limit={limit}&offset={offset}", Name: "users-list", Description: "用户列表", MIMEType: "application/json"},
		{URITemplate: "aieas://merchants/{merchantId}", Name: "merchant", Description: "商家安全资料和经营概览", MIMEType: "application/json"},
		{URITemplate: "aieas://merchants/{merchantId}/live-sessions?status={status}&limit={limit}&offset={offset}", Name: "merchant-live-sessions", Description: "商家直播场次列表", MIMEType: "application/json"},
		{URITemplate: "aieas://auction-lots/{auctionId}", Name: "auction-lot", Description: "拍品详情", MIMEType: "application/json"},
		{URITemplate: "aieas://auction-lots/{auctionId}/state", Name: "auction-state", Description: "拍品实时状态", MIMEType: "application/json"},
		{URITemplate: "aieas://auction-lots?sellerId={sellerId}&status={status}&liveSessionId={liveSessionId}&limit={limit}&offset={offset}", Name: "auction-lots-list", Description: "拍品列表", MIMEType: "application/json"},
		{URITemplate: "aieas://live-sessions/{sessionId}", Name: "live-session", Description: "直播场次详情", MIMEType: "application/json"},
		{URITemplate: "aieas://live-sessions/{sessionId}/lots", Name: "live-session-lots", Description: "场次内拍品", MIMEType: "application/json"},
		{URITemplate: "aieas://live-sessions/{sessionId}/bids?limit={limit}&offset={offset}&sort={sort}", Name: "live-session-bids", Description: "场次出价记录", MIMEType: "application/json"},
		{URITemplate: "aieas://live-sessions/{sessionId}/orders?status={status}&payStatus={payStatus}&limit={limit}&offset={offset}", Name: "live-session-orders", Description: "场次交易订单", MIMEType: "application/json"},
		{URITemplate: "aieas://live-sessions/{sessionId}/settlement-summary", Name: "live-session-settlement-summary", Description: "场次成交情况汇总", MIMEType: "application/json"},
		{URITemplate: "aieas://orders/{orderId}", Name: "order", Description: "订单详情", MIMEType: "application/json"},
		{URITemplate: "aieas://orders?winnerId={winnerId}&sellerId={sellerId}&status={status}&payStatus={payStatus}&limit={limit}&offset={offset}", Name: "orders-list", Description: "订单列表", MIMEType: "application/json"},
		{URITemplate: "aieas://risk/events?status={status}&eventType={eventType}&userId={userId}&limit={limit}&offset={offset}", Name: "risk-events", Description: "风险事件列表", MIMEType: "application/json"},
		{URITemplate: "aieas://audit-logs?operatorId={operatorId}&action={action}&limit={limit}&offset={offset}", Name: "audit-logs", Description: "审计日志列表", MIMEType: "application/json"},
	}
}

func (h *Handler) toolDefinitions() []toolDefinition {
	if h.read != nil && h.control == nil {
		return readToolDefinitions()
	}
	if h.control != nil && h.read == nil {
		return controlToolDefinitions()
	}
	if h.read != nil && h.control != nil {
		tools := readToolDefinitions()
		tools = append(tools, controlToolDefinitions()...)
		return tools
	}
	return []toolDefinition{}
}

func readToolDefinitions() []toolDefinition {
	return []toolDefinition{
		tool("read_user", "读取用户安全信息。只读，无副作用。", objectSchema(map[string]interface{}{"userId": stringProp("用户 ID；缺省时读取当前登录用户")}, nil)),
		tool("read_users", "查询用户列表。admin only。只读，无副作用。", pagedSchema(map[string]interface{}{"role": enumProp([]string{"buyer", "merchant", "admin"}), "status": enumProp([]string{"ACTIVE", "DISABLED"}), "keyword": stringProp("用户 ID、账号或昵称关键词")}, nil)),
		tool("read_merchant", "读取商家资料和经营概览。只读，无副作用。", objectSchema(map[string]interface{}{"merchantId": stringProp("商家用户 ID")}, nil)),
		tool("read_auction_lots", "查询拍品列表。只读，无副作用。", pagedSchema(map[string]interface{}{"sellerId": stringProp("商家用户 ID"), "status": stringProp("拍品状态"), "liveSessionId": integerProp("直播场次 ID")}, nil)),
		tool("read_auction_lot", "读取拍品详情。只读，无副作用。", objectSchema(map[string]interface{}{"auctionId": integerProp("拍品 ID")}, []string{"auctionId"})),
		tool("read_auction_state", "读取拍品实时状态。只读，无副作用。", objectSchema(map[string]interface{}{"auctionId": integerProp("拍品 ID")}, []string{"auctionId"})),
		tool("read_live_sessions", "查询直播场次。只读，无副作用。", pagedSchema(map[string]interface{}{"merchantId": stringProp("商家用户 ID"), "status": stringProp("场次状态")}, nil)),
		tool("read_live_session", "读取直播场次详情。只读，无副作用。", objectSchema(map[string]interface{}{"sessionId": integerProp("直播场次 ID")}, []string{"sessionId"})),
		tool("read_live_session_lots", "读取场次内拍品。只读，无副作用。", objectSchema(map[string]interface{}{"sessionId": integerProp("直播场次 ID")}, []string{"sessionId"})),
		tool("read_live_session_bids", "读取场次出价记录。只读，无副作用。", pagedSchema(map[string]interface{}{"sessionId": integerProp("直播场次 ID"), "sort": enumProp([]string{"timeDesc", "timeAsc", "priceDesc"})}, []string{"sessionId"})),
		tool("read_live_session_orders", "读取场次交易订单。只读，无副作用。", pagedSchema(map[string]interface{}{"sessionId": integerProp("直播场次 ID"), "status": stringProp("订单状态"), "payStatus": stringProp("支付状态")}, []string{"sessionId"})),
		tool("read_live_session_settlement", "读取场次成交汇总。只读，无副作用。", objectSchema(map[string]interface{}{"sessionId": integerProp("直播场次 ID")}, []string{"sessionId"})),
		tool("read_orders", "查询订单列表。只读，无副作用。", pagedSchema(map[string]interface{}{"winnerId": stringProp("买家用户 ID"), "sellerId": stringProp("卖家用户 ID"), "status": stringProp("订单状态"), "payStatus": stringProp("支付状态")}, nil)),
		tool("read_order", "读取订单详情。只读，无副作用。", objectSchema(map[string]interface{}{"orderId": integerProp("订单 ID")}, []string{"orderId"})),
		tool("read_risk_events", "查询风险事件。admin only。只读，无副作用。", pagedSchema(map[string]interface{}{"status": stringProp("风险事件状态"), "eventType": stringProp("事件类型"), "userId": stringProp("用户 ID")}, nil)),
		tool("read_audit_logs", "查询审计日志。只读，无副作用。", pagedSchema(map[string]interface{}{"operatorId": stringProp("操作人 ID"), "action": stringProp("动作")}, nil)),
	}
}

func controlToolDefinitions() []toolDefinition {
	return []toolDefinition{
		tool("get_merchant_live_control_context", "获取商家当前直播场次控制台上下文。参数只需要 merchantId，返回当前场次、讲解中拍品、成交/流拍/待讲解/可上架拍品。", objectSchema(map[string]interface{}{"merchantId": stringProp("商家用户 ID")}, []string{"merchantId"})),
		tool("operate_live_session_lot", "模拟商家直播中的拍品操作。支持 onShelf 上架、offShelf 下架、startExplain 开始拍卖/讲解、hammer 落槌、endLive 下播；startExplain 必须传 auctionDurationSec。", operateLiveSessionLotSchema()),
		tool("live_voice_broadcast", "直播语音播报。传入直播场次 ID 和播报文本，服务端调用豆包 TTS 合成语音，并通过 live.voice_broadcast WebSocket 事件推送给订阅该场次的用户端。", objectSchema(map[string]interface{}{"liveSessionId": integerProp("直播场次 ID"), "text": stringProp("播报文本，1-1000 字"), "requestId": stringProp("可选请求 ID，用于关联 MCP 调用和 WebSocket 播报事件")}, []string{"liveSessionId", "text"})),
	}
}

func operateLiveSessionLotSchema() map[string]interface{} {
	schema := objectSchema(map[string]interface{}{
		"liveSessionId":      integerProp("直播场次 ID"),
		"auctionId":          integerProp("拍品 ID"),
		"action":             enumProp([]string{"onShelf", "offShelf", "startExplain", "hammer", "endLive"}),
		"auctionDurationSec": optionalIntegerProp("拍卖/讲解时长，单位秒；action=startExplain 时必填且必须大于 0"),
		"force":              booleanProp("hammer/endLive 时是否强制结束；hammer 默认 true"),
		"requestId":          stringProp("可选幂等请求 ID，建议 hammer 时传入"),
	}, []string{"liveSessionId", "auctionId", "action"})
	schema["allOf"] = []interface{}{
		map[string]interface{}{
			"if": map[string]interface{}{
				"properties": map[string]interface{}{"action": map[string]interface{}{"const": "startExplain"}},
			},
			"then": map[string]interface{}{"required": []string{"auctionDurationSec"}},
		},
	}
	return schema
}

func tool(name, description string, inputSchema map[string]interface{}) toolDefinition {
	return toolDefinition{Name: name, Description: description, InputSchema: inputSchema}
}

func objectSchema(properties map[string]interface{}, required []string) map[string]interface{} {
	schema := map[string]interface{}{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func pagedSchema(properties map[string]interface{}, required []string) map[string]interface{} {
	properties["limit"] = map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 100, "default": 20}
	properties["offset"] = map[string]interface{}{"type": "integer", "minimum": 0, "default": 0}
	return objectSchema(properties, required)
}

func stringProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "string", "description": description}
}

func integerProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "integer", "minimum": 1, "description": description}
}

func optionalIntegerProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "integer", "minimum": 1, "description": description}
}

func booleanProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "boolean", "description": description}
}

func enumProp(values []string) map[string]interface{} {
	return map[string]interface{}{"type": "string", "enum": values}
}
