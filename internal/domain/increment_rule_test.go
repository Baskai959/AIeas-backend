package domain

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestIncrementRuleFixedValidationAndLookup(t *testing.T) {
	rule := json.RawMessage(`{"type":"fixed","amount":200,"maxBidSteps":3}`)
	if err := ValidateIncrementRule(rule); err != nil {
		t.Fatalf("validate fixed rule %s: %v", string(rule), err)
	}
	parsed, err := ParseIncrementRule(rule)
	if err != nil {
		t.Fatalf("parse fixed rule: %v", err)
	}
	if parsed.Type != IncrementRuleTypeFixed || parsed.Amount != 200 || parsed.MaxBidSteps != 3 {
		t.Fatalf("unexpected parsed rule: %+v", parsed)
	}
	if got := MinIncrementForPrice(rule, 1000, 100); got != 200 {
		t.Fatalf("expected rule increment to override fallback, got %d", got)
	}
}

func TestIncrementRuleLadderValidationAndLookup(t *testing.T) {
	rule := json.RawMessage(`{"type":"ladder","maxBidSteps":4,"steps":[{"min":0,"max":1000,"amount":50},{"min":1000,"max":5000,"amount":100},{"min":5000,"amount":500}]}`)
	if err := ValidateIncrementRule(rule); err != nil {
		t.Fatalf("validate ladder rule %s: %v", string(rule), err)
	}
	if got := MinIncrementForPrice(rule, 999, 10); got != 50 {
		t.Fatalf("expected first ladder amount, got %d", got)
	}
	if got := MinIncrementForPrice(rule, 1000, 10); got != 100 {
		t.Fatalf("expected second ladder amount, got %d", got)
	}
	if got := MinIncrementForPrice(rule, 8000, 10); got != 500 {
		t.Fatalf("expected last ladder amount, got %d", got)
	}
}

func TestIncrementRuleValidationRejectsInvalidRules(t *testing.T) {
	rules := []json.RawMessage{
		json.RawMessage(`{"type":"fixed","amount":0,"maxBidSteps":3}`),
		json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":0}`),
		json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":3,"steps":[{"min":0,"amount":100}]}`),
		json.RawMessage(`{"type":"ladder","amount":100,"maxBidSteps":3,"steps":[{"min":0,"amount":100}]}`),
		json.RawMessage(`{"type":"ladder","amount":0,"maxBidSteps":3,"steps":[{"min":0,"amount":100}]}`),
		json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":1,"amount":100}]}`),
		json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":0,"max":1000,"amount":100},{"min":1001,"amount":200}]}`),
		json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":0,"max":1000,"amount":100}]}`),
		json.RawMessage(`{"amount":100,"maxBidSteps":3}`),
		json.RawMessage(`{"type":"fixed","amount":100}`),
		json.RawMessage(`{"type":"fixed","maxBidSteps":3}`),
	}
	for _, rule := range rules {
		if err := ValidateIncrementRule(rule); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("expected invalid rule error for %s, got %v", string(rule), err)
		}
	}
}

func TestValidateAuctionPricingAllowsUnalignedCapPrice(t *testing.T) {
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":5}`)
	if err := ValidateAuctionPricing(1000, 1500, 2050, rule); err != nil {
		t.Fatalf("expected valid auction pricing: %v", err)
	}
	if err := ValidateAuctionPricing(1000, 5000, 0, rule); err != nil {
		t.Fatalf("expected valid auction pricing without cap price: %v", err)
	}
	for _, tc := range []struct {
		name    string
		start   int64
		reserve int64
		cap     int64
	}{
		{name: "cap not above start", start: 1000, reserve: 0, cap: 1000},
		{name: "reserve above cap", start: 1000, reserve: 2500, cap: 2000},
	} {
		if err := ValidateAuctionPricing(tc.start, tc.reserve, tc.cap, rule); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: expected invalid argument, got %v", tc.name, err)
		}
	}
}

func TestValidateBidPriceFixed(t *testing.T) {
	rule := IncrementRule{Type: IncrementRuleTypeFixed, Amount: 100, MaxBidSteps: 3}
	cases := []struct {
		name    string
		current int64
		price   int64
		want    string
	}{
		{name: "below start", current: 1000, price: 1000, want: BidRejectBelowStartPrice},
		{name: "step mismatch", current: 1000, price: 1150, want: BidRejectStepMismatch},
		{name: "below current increment", current: 1200, price: 1200, want: BidRejectBelowMinIncrement},
		{name: "above max bid steps", current: 1200, price: 1600, want: BidRejectAboveMaxBidSteps},
		{name: "above cap", current: 1800, price: 2100, want: BidRejectAboveCapPrice},
		{name: "ok", current: 1200, price: 1500, want: ""},
		{name: "cap allowed", current: 1800, price: 2000, want: ""},
		{name: "unaligned cap allowed", current: 1400, price: 1450, want: ""},
	}
	for _, tc := range cases {
		capPrice := int64(2000)
		if tc.name == "unaligned cap allowed" {
			capPrice = 1450
		}
		if got := ValidateBidPrice(1000, tc.current, capPrice, tc.price, rule); got != tc.want {
			t.Fatalf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestValidateBidPriceLadderUsesCurrentPriceBand(t *testing.T) {
	rule := IncrementRule{
		Type:        IncrementRuleTypeLadder,
		MaxBidSteps: 3,
		Steps: []IncrementStep{
			{Min: 0, Max: int64Ptr(1000), Amount: 50},
			{Min: 1000, Max: int64Ptr(5000), Amount: 100},
			{Min: 5000, Amount: 500},
		},
	}
	if got := ValidateBidPrice(500, 900, 0, 1000, rule); got != "" {
		t.Fatalf("expected first band bid accepted, got %q", got)
	}
	if got := ValidateBidPrice(500, 1000, 0, 1050, rule); got != BidRejectStepMismatch {
		t.Fatalf("expected second band step mismatch, got %q", got)
	}
	if got := ValidateBidPrice(500, 1000, 0, 1300, rule); got != "" {
		t.Fatalf("expected second band bid accepted, got %q", got)
	}
	if got := ValidateBidPrice(500, 4800, 5050, 5050, rule); got != "" {
		t.Fatalf("expected unaligned cap bid accepted, got %q", got)
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
