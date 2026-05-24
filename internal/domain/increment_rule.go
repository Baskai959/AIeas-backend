package domain

import (
	"encoding/json"
	"strings"
)

const (
	IncrementRuleTypeFixed  = "fixed"
	IncrementRuleTypeLadder = "ladder"

	maxIncrementRuleSteps = 50
)

// Min is inclusive; Max is exclusive. The last step must omit Max.
type IncrementStep struct {
	Min    int64  `json:"min"`
	Max    *int64 `json:"max,omitempty"`
	Amount int64  `json:"amount"`
}

type incrementRulePayload struct {
	Type   string          `json:"type"`
	Amount int64           `json:"amount"`
	Steps  []IncrementStep `json:"steps"`
}

// DefaultIncrementRule returns the default fixed-increment rule.
func DefaultIncrementRule() json.RawMessage {
	return json.RawMessage(`{"type":"fixed","amount":100}`)
}

// ValidateIncrementRule validates seller-defined increment rules.
func ValidateIncrementRule(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return ErrInvalidArgument
	}
	var rule incrementRulePayload
	if err := json.Unmarshal(raw, &rule); err != nil {
		return ErrInvalidArgument
	}
	switch normalizeIncrementRuleType(rule.Type) {
	case IncrementRuleTypeFixed:
		if fixedIncrementAmount(rule) <= 0 {
			return ErrInvalidArgument
		}
		return nil
	case IncrementRuleTypeLadder:
		return validateLadderIncrementRule(rule.Steps)
	default:
		return ErrInvalidArgument
	}
}

// MinIncrementForPrice returns the minimum increment required at currentPrice.
// Invalid or unsupported persisted rules fall back to fallback so older data
// does not make running auctions unusable.
func MinIncrementForPrice(raw json.RawMessage, currentPrice int64, fallback int64) int64 {
	if fallback <= 0 {
		fallback = 1
	}
	if len(raw) == 0 || !json.Valid(raw) {
		return fallback
	}
	var rule incrementRulePayload
	if err := json.Unmarshal(raw, &rule); err != nil {
		return fallback
	}
	switch normalizeIncrementRuleType(rule.Type) {
	case IncrementRuleTypeFixed:
		if amount := fixedIncrementAmount(rule); amount > 0 {
			return amount
		}
	case IncrementRuleTypeLadder:
		for _, step := range rule.Steps {
			if step.Amount <= 0 || currentPrice < step.Min {
				continue
			}
			if step.Max == nil || currentPrice < *step.Max {
				return step.Amount
			}
		}
	}
	return fallback
}

func normalizeIncrementRuleType(ruleType string) string {
	return strings.ToLower(strings.TrimSpace(ruleType))
}

func fixedIncrementAmount(rule incrementRulePayload) int64 {
	return rule.Amount
}

func validateLadderIncrementRule(steps []IncrementStep) error {
	if len(steps) == 0 || len(steps) > maxIncrementRuleSteps {
		return ErrInvalidArgument
	}
	expectedMin := int64(0)
	for i, step := range steps {
		if step.Min != expectedMin || step.Amount <= 0 {
			return ErrInvalidArgument
		}
		if step.Max == nil {
			if i != len(steps)-1 {
				return ErrInvalidArgument
			}
			return nil
		}
		if *step.Max <= step.Min {
			return ErrInvalidArgument
		}
		expectedMin = *step.Max
	}
	return ErrInvalidArgument
}
