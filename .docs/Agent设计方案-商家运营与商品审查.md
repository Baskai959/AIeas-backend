# Agent 设计方案：商家运营与商品审查

## 1. 目标与边界

第一阶段只落地两个 Agent：

- 商家运营 Agent：辅助商家创建拍品、优化标题/描述、建议起拍价/保证金/加价规则、分析历史成交表现。
- 商品审查 Agent：辅助平台审核商品与拍品信息，识别违规词、虚假宣传、敏感品类、图片风险、价格异常和资料缺失。

Agent 不进入实时竞拍强一致链路。以下动作仍由确定性代码、状态机、Redis Lua、MySQL 事务和人工审核控制：

- 出价是否有效。
- 当前最高价裁决。
- 落槌成交结果。
- 订单生成。
- 保证金捕获/释放。
- 支付状态更新。

Agent 的定位是：**分析、解释、建议、生成草稿、辅助审核**。涉及封禁、强制下架、取消拍卖等高风险动作时，Agent 只能生成建议，由规则引擎或人工审核确认。

## 2. 总体架构

推荐采用独立 Agent Service，不嵌入 Go 拍卖热路径：

```text
Go Auction Backend
  -> 提供商品、拍品、商家、历史成交、审计数据 API
  -> 发出商品创建/更新/审核事件

Agent Service
  -> 编排 LLM、Tools、RAG、策略规则
  -> 生成运营建议和审查结论
  -> 写入 agent_task / audit_log / review_suggestion

RAG Store
  -> 平台规则
  -> 禁售/限售规范
  -> 标题描述规范
  -> 历史优质案例
  -> 价格策略说明
```

Agent Service 可以使用 Python 或 Go 实现：

- Python 更适合复杂 Agent 编排、RAG、模型实验。
- Go 更适合轻量 tool calling 和与现有后端部署统一。

第一阶段建议独立服务化，Go 后端通过 HTTP/gRPC 调用。这样 Agent 故障不会影响拍卖主链路。

## 3. Agent 链路设计

### 3.1 商家运营 Agent

触发方式：

- 商家创建商品后，主动点击“优化商品信息”。
- 商家创建拍品时，系统自动给出建议。
- 直播间开拍前，系统生成运营建议。

链路：

```text
merchant request
  -> Go Backend 鉴权
  -> 创建 agent_task
  -> Agent Service 拉取上下文
  -> RAG 检索运营规则与历史案例
  -> 调用 Tools 获取商品/拍品/历史成交数据
  -> LLM 生成建议
  -> 规则校验和格式化
  -> 写回建议结果
  -> 前端展示给商家确认
```

输出：

- 商品标题建议。
- 商品描述改写。
- 卖点提炼。
- 起拍价建议。
- 保证金建议。
- 加价阶梯建议。
- 开拍时间建议。
- 直播讲解提纲。
- 风险提醒，例如描述缺失、价格过高、类目不匹配。

### 3.2 商品审查 Agent

触发方式：

- 商品创建/更新后进入审查。
- 拍品提交审核时触发。
- 管理员手动发起复审。

链路：

```text
item submitted
  -> Go Backend 写入待审状态
  -> 发送 item.review.requested 事件
  -> Agent Service 消费事件
  -> 拉取商品文本、图片 URL、商家历史、类目规则
  -> RAG 检索平台规则与禁售规范
  -> 调用文本审查/图片审查/价格检测 Tools
  -> LLM 生成审查解释
  -> 规则引擎二次校验
  -> 输出 PASS / REVIEW_REQUIRED / REJECT_SUGGESTED
```

输出：

- 审查结论。
- 风险等级。
- 命中的规则条款。
- 风险解释。
- 建议修改项。
- 是否需要人工复核。

高风险结论不能自动封禁或删除，只能进入人工审核队列。

## 4. Tools 设计

Tools 应该是确定性、可审计、权限受控的函数。Agent 不直接访问数据库，统一通过后端 API 或只读数据服务访问。

### 4.1 数据读取 Tools

```text
get_item(itemId)
get_auction_lot(auctionId)
get_merchant_profile(merchantId)
get_merchant_history(merchantId)
get_category_policy(categoryId)
get_recent_deals(categoryId, priceRange)
get_live_room(roomId)
```

用途：

- 构建商品和商家上下文。
- 读取历史成交价格。
- 判断类目与价格是否异常。
- 为运营建议提供依据。

### 4.2 审查 Tools

```text
check_forbidden_words(text)
check_sensitive_category(categoryId, title, description)
check_price_anomaly(categoryId, startPrice, reservePrice)
check_required_fields(item)
check_image_risk(imageUrls)
check_duplicate_listing(itemId, sellerId)
```

用途：

- 先用规则和模型工具产出结构化审查结果。
- LLM 负责综合解释和生成建议，而不是替代规则。

### 4.3 生成与修改建议 Tools

```text
generate_title_candidates(item)
rewrite_description(item, policyContext)
suggest_increment_rule(startPrice, categoryId)
suggest_deposit_amount(startPrice, categoryId, merchantRiskLevel)
suggest_live_script(item, auctionLot)
```

这些 Tool 可以内部调用 LLM，也可以作为 Agent 的子任务。

### 4.4 写入 Tools

写入工具必须少而受控：

```text
create_agent_task(type, targetId)
save_agent_result(taskId, result)
save_review_suggestion(itemId, suggestion)
append_audit_log(actor="agent", action, target)
```

禁止第一阶段开放：

```text
auto_publish_item
auto_reject_item
auto_close_auction
auto_blacklist_merchant
```

## 5. MCP 设计

MCP 适合把内部能力标准化暴露给 Agent。建议按能力域拆 MCP server：

### 5.1 Catalog MCP

提供商品、拍品、类目、图片等只读能力：

```text
resources:
  item://{itemId}
  auction://{auctionId}
  category-policy://{categoryId}

tools:
  get_item
  get_auction_lot
  get_category_policy
  check_required_fields
```

### 5.2 Merchant MCP

提供商家资料、历史成交、风险等级：

```text
resources:
  merchant://{merchantId}

tools:
  get_merchant_profile
  get_merchant_history
  get_merchant_risk_summary
```

### 5.3 Review MCP

提供审查工具和规则命中：

```text
tools:
  check_forbidden_words
  check_sensitive_category
  check_price_anomaly
  check_image_risk
  save_review_suggestion
```

### 5.4 RAG MCP

提供知识检索：

```text
tools:
  search_policy_docs
  search_title_examples
  search_price_strategy_docs
  search_review_cases
```

MCP 权限原则：

- 商家侧 Agent 只能读取当前商家的数据。
- 审查 Agent 可以读取待审目标和必要平台规则。
- 所有写入 Tool 必须记录 `agentId`、`taskId`、`traceId`、入参摘要和结果。

## 6. Skills 设计

Skill 是 Agent 的固定工作流模板，避免每次完全自由规划。

### 6.1 商品标题优化 Skill

输入：

- 商品标题、类目、规格、卖点、图片摘要。

步骤：

1. 检查标题长度和禁用词。
2. 检索同类优质标题样例。
3. 生成 3 - 5 个标题候选。
4. 解释每个标题适用场景。
5. 输出结构化 JSON。

### 6.2 商品描述审查 Skill

步骤：

1. 提取商品声明。
2. 检查敏感词和绝对化用语。
3. 检索平台审查规则。
4. 标注风险片段。
5. 输出修改建议。

### 6.3 拍品定价建议 Skill

步骤：

1. 获取历史成交价。
2. 计算同类商品价格区间。
3. 结合商家历史成交率。
4. 建议起拍价、保留价、保证金和加价阶梯。
5. 给出风险提示。

### 6.4 审核结论生成 Skill

步骤：

1. 聚合规则工具结果。
2. 检索相关平台条款。
3. 生成风险等级。
4. 生成 `PASS / REVIEW_REQUIRED / REJECT_SUGGESTED`。
5. 给出人工审核建议。

## 7. RAG 设计

### 7.1 知识库范围

第一阶段建议建立以下知识库：

- 平台禁售/限售规则。
- 商品标题与描述规范。
- 类目审核规则。
- 价格异常判断规则。
- 历史审核案例。
- 优质商品标题/描述样例。
- 直播讲解模板。

### 7.2 文档结构

RAG 文档需要结构化元数据：

```json
{
  "docId": "policy_001",
  "title": "禁售商品规则",
  "category": "review_policy",
  "effectiveDate": "2026-01-01",
  "riskLevel": "high",
  "content": "..."
}
```

检索时应按场景过滤：

- `review_policy` 用于审查。
- `operation_guide` 用于运营建议。
- `case_study` 用于案例参考。
- `title_example` 用于标题生成。

### 7.3 RAG 输出约束

Agent 输出必须引用命中的规则或案例：

```json
{
  "decision": "REVIEW_REQUIRED",
  "riskLevel": "MID",
  "matchedPolicies": [
    {"docId": "policy_001", "section": "2.3", "reason": "命中绝对化描述"}
  ],
  "suggestions": ["将“全网最低”改为可验证描述"]
}
```

## 8. 数据模型建议

### 8.1 agent_task

```text
id
task_type
target_type
target_id
status
requested_by
trace_id
input_snapshot_json
result_json
error_message
created_at
updated_at
```

### 8.2 agent_review_suggestion

```text
id
task_id
item_id
auction_id
decision
risk_level
matched_policy_json
suggestion_json
requires_human_review
created_at
```

### 8.3 agent_tool_call_log

```text
id
task_id
tool_name
input_hash
output_hash
status
latency_ms
error_message
created_at
```

## 9. 安全与权限

- Agent 使用服务账号，权限最小化。
- Tool 层强制校验商家数据归属。
- 不允许 Agent 直接拼 SQL 或访问生产数据库账号。
- 所有 Tool 调用必须可审计。
- 模型输出必须经过 JSON schema 校验。
- 高风险动作必须人工确认。
- 图片、商品描述、商家信息可能包含敏感数据，日志只记录摘要和 hash，不记录完整敏感内容。

## 10. 可观测性

Agent 链路需要打点：

```text
agent_task_total{type,status}
agent_task_duration_seconds{type}
agent_tool_call_total{tool,status}
agent_tool_call_duration_seconds{tool}
agent_rag_retrieval_total{kb,status}
agent_llm_request_total{model,status}
agent_review_decision_total{decision,risk_level}
```

Trace 链路：

```text
HTTP request
  -> agent.task
    -> rag.retrieve
    -> tool.get_item
    -> tool.check_forbidden_words
    -> llm.generate_review
    -> save_agent_result
```

每个任务结果都保存 `trace_id`，方便从后台页面跳转到 Trace 和日志。

## 11. 第一阶段落地步骤

1. 新增 Agent Service，提供 `POST /agent/tasks` 和 `GET /agent/tasks/:id`。
2. 实现 Catalog、Merchant、Review、RAG 四类 Tool。
3. 建立平台规则与标题样例 RAG 知识库。
4. 实现商品标题优化 Skill、商品描述审查 Skill、审核结论生成 Skill。
5. 后台管理页展示 Agent 审查建议。
6. 商家端展示标题/描述/定价建议，商家手动采纳。
7. 接入 Metrics、Trace、Tool 调用审计。

## 12. 不做事项

第一阶段不做：

- 自动通过/驳回商品审核。
- 自动封禁商家。
- 自动关闭拍卖。
- Agent 参与出价裁决。
- Agent 修改订单和保证金。

这些能力必须等审计、权限、回滚、人工确认流程成熟后再评估。
