package domain

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestIncrementRuleLadderValidationAndLookup(t *testing.T) {
	rule := json.RawMessage(`{"type":"ladder","steps":[{"min":0,"max":5000,"amount":500},{"min":5000,"max":10000,"amount":800},{"min":10000,"amount":1000}]}`)
	if err := ValidateIncrementRule(rule); err != nil {
		t.Fatalf("validate ladder rule: %v", err)
	}
	cases := []struct {
		current int64
		want    int64
	}{
		{current: 0, want: 500},
		{current: 4999, want: 500},
		{current: 5000, want: 800},
		{current: 9999, want: 800},
		{current: 10000, want: 1000},
	}
	for _, tc := range cases {
		if got := MinIncrementForPrice(rule, tc.current, 100); got != tc.want {
			t.Fatalf("current=%d got increment=%d want=%d", tc.current, got, tc.want)
		}
	}
}

func TestIncrementRuleValidationRejectsBrokenLadderRules(t *testing.T) {
	rules := []json.RawMessage{
		json.RawMessage(`{"type":"ladder","steps":[]}`),
		json.RawMessage(`{"type":"ladder","steps":[{"min":100,"amount":500},{"min":500,"amount":800}]}`),
		json.RawMessage(`{"type":"ladder","steps":[{"min":0,"max":500,"amount":0},{"min":500,"amount":800}]}`),
		json.RawMessage(`{"type":"ladder","steps":[{"min":0,"max":500,"amount":500},{"min":700,"amount":800}]}`),
		json.RawMessage(`{"type":"ladder","steps":[{"min":0,"max":500,"amount":500}]}`),
		json.RawMessage(`{"type":"tiered","tiers":[{"from":0,"amount":500}]}`),
	}
	for _, rule := range rules {
		if err := ValidateIncrementRule(rule); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("expected invalid rule error for %s, got %v", string(rule), err)
		}
	}
}

func TestIncrementRuleFixedSupportsAmountOnly(t *testing.T) {
	rule := json.RawMessage(`{"type":"fixed","amount":200}`)
	if err := ValidateIncrementRule(rule); err != nil {
		t.Fatalf("validate fixed rule %s: %v", string(rule), err)
	}
	if got := MinIncrementForPrice(rule, 1000, 100); got != 200 {
		t.Fatalf("expected rule increment to override fallback, got %d", got)
	}
	for _, invalid := range []json.RawMessage{
		json.RawMessage(`{"type":"fixed","minStep":300}`),
		json.RawMessage(`{"amount":300}`),
	} {
		if err := ValidateIncrementRule(invalid); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("expected invalid fixed rule for %s, got %v", string(invalid), err)
		}
	}
}
