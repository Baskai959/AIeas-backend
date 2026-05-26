package service

import (
	"context"
	"errors"
)

var ErrProductDescriptionUnavailable = errors.New("product description generator unavailable")
var ErrProductAuditUnavailable = errors.New("product auditor unavailable")

type ProductDescriptionInput struct {
	Title       string
	Category    string
	Condition   string
	ImageName   string
	ContentType string
	ImageSize   int64
	Image       []byte
}

type ProductDescriptionResult struct {
	Title       string `json:"title"`
	Category    string `json:"category"`
	Description string `json:"description"`
}

type ProductDescriptionGenerator interface {
	GenerateProductDescription(ctx context.Context, in ProductDescriptionInput) (ProductDescriptionResult, error)
}

type ProductAuditInput struct {
	ProductText string
	ImageName   string
	ContentType string
	ImageSize   int64
	Image       []byte
}

type ProductAuditResult struct {
	Success      bool    `json:"success"`
	IsApproved   bool    `json:"is_approved"`
	RejectReason *string `json:"reject_reason"`
}

type ProductAuditor interface {
	AuditProduct(ctx context.Context, in ProductAuditInput) (ProductAuditResult, error)
}

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
