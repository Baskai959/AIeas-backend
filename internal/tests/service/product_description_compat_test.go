package service

import (
	"context"
	"errors"

	aiapp "aieas_backend/internal/modules/ai/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	mcpports "aieas_backend/internal/modules/mcp/ports"
)

var ErrProductDescriptionUnavailable = errors.New("product description generator unavailable")
var ErrProductAuditUnavailable = errors.New("product auditor unavailable")
var ErrLiveAnalysisUnavailable = errors.New("live analysis generator unavailable")

type ProductDescriptionInput = aiapp.ProductDescriptionInput
type ProductDescriptionResult = aiapp.ProductDescriptionResult
type ProductDescriptionGenerator = aiapp.ProductDescriptionGenerator

type ProductAuditInput = auctionports.ProductAuditInput
type ProductAuditImage = auctionports.ProductAuditImage
type ProductAuditResult = auctionports.ProductAuditResult
type ProductAuditor = auctionports.ProductAuditor
type ProductAuditImageLoader = auctionports.ProductAuditImageLoader

type LiveAnalysisAsyncInput = liveanalysisports.AsyncRequestInput
type LiveAnalysisAsyncResult = liveanalysisports.AsyncRequestResult
type LiveAnalysisRequester = liveanalysisports.AsyncRequester

type LiveVoiceSynthesisInput = mcpports.LiveVoiceSynthesisInput
type LiveVoiceSynthesisResult = mcpports.LiveVoiceSynthesisResult
type LiveVoiceBroadcastPayload = mcpports.LiveVoiceBroadcastPayload

type DisabledProductDescriptionGenerator struct{}

func (DisabledProductDescriptionGenerator) GenerateProductDescription(ctx context.Context, in ProductDescriptionInput) (ProductDescriptionResult, error) {
	_ = ctx
	_ = in
	return ProductDescriptionResult{}, ErrProductDescriptionUnavailable
}

type DisabledProductAuditor struct{}

func (DisabledProductAuditor) AuditProduct(ctx context.Context, in ProductAuditInput) (ProductAuditResult, error) {
	_ = ctx
	_ = in
	return ProductAuditResult{}, ErrProductAuditUnavailable
}

type DisabledLiveAnalysisRequester struct{}

func (DisabledLiveAnalysisRequester) RequestLiveAnalysis(ctx context.Context, in LiveAnalysisAsyncInput) (LiveAnalysisAsyncResult, error) {
	_ = ctx
	_ = in
	return LiveAnalysisAsyncResult{}, ErrLiveAnalysisUnavailable
}
