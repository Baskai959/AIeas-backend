package domain

import (
	"bytes"
	"encoding/json"
	"strings"
)

const (
	IncrementRuleTypeFixed  = "fixed"
	IncrementRuleTypeLadder = "ladder"

	defaultIncrementAmount = int64(100)
	defaultMaxBidSteps     = 10
	maxIncrementRuleSteps  = 50

	BidRejectBelowStartPrice          = "BELOW_START_PRICE"
	BidRejectStepMismatch             = "PRICE_STEP_MISMATCH"
	BidRejectBelowMinIncrement        = "BELOW_MIN_INCREMENT"
	BidRejectAboveMaxBidSteps         = "ABOVE_MAX_BID_STEPS"
	BidRejectAboveExpectedMaxBidSteps = "ABOVE_EXPECTED_MAX_BID_STEPS"
	BidRejectAboveCapPrice            = "ABOVE_CAP_PRICE"
	BidRejectMissingExpectedState     = "MISSING_EXPECTED_STATE"
	BidRejectStaleAuctionState        = "STALE_AUCTION_STATE"
	BidRejectAuctionBusy              = "AUCTION_BUSY"
)

// IncrementRule supports fixed increment and ladder increment.
// MaxBidSteps limits a single bid to currentPrice + currentStepAmount*MaxBidSteps.
type IncrementRule struct {
	Type        string          `json:"type"`
	Amount      int64           `json:"amount,omitempty"`
	MaxBidSteps int             `json:"maxBidSteps"`
	Steps       []IncrementStep `json:"steps,omitempty"`
}

// IncrementStep is a ladder increment band. Min is inclusive, Max is exclusive.
// The last step must omit Max.
type IncrementStep struct {
	Min    int64  `json:"min"`
	Max    *int64 `json:"max,omitempty"`
	Amount int64  `json:"amount"`
}

// DefaultIncrementRule returns the default fixed-increment rule.
func DefaultIncrementRule() json.RawMessage {
	raw, _ := json.Marshal(IncrementRule{Type: IncrementRuleTypeFixed, Amount: defaultIncrementAmount, MaxBidSteps: defaultMaxBidSteps})
	return json.RawMessage(raw)
}

// ValidateIncrementRule validates seller-defined fixed or ladder increment rules.
func ValidateIncrementRule(raw json.RawMessage) error {
	_, err := ParseIncrementRule(raw)
	return err
}

func ParseIncrementRule(raw json.RawMessage) (IncrementRule, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return IncrementRule{}, ErrInvalidArgument
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return IncrementRule{}, ErrInvalidArgument
	}
	_, hasType := fields["type"]
	_, hasAmount := fields["amount"]
	_, hasMaxBidSteps := fields["maxBidSteps"]
	_, hasSteps := fields["steps"]
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var rule IncrementRule
	if err := decoder.Decode(&rule); err != nil {
		return IncrementRule{}, ErrInvalidArgument
	}
	rule.Type = strings.ToLower(strings.TrimSpace(rule.Type))
	if !hasType || !hasMaxBidSteps || rule.MaxBidSteps <= 0 {
		return IncrementRule{}, ErrInvalidArgument
	}
	switch rule.Type {
	case IncrementRuleTypeFixed:
		if !hasAmount || hasSteps || rule.Amount <= 0 {
			return IncrementRule{}, ErrInvalidArgument
		}
	case IncrementRuleTypeLadder:
		if hasAmount || !hasSteps || validateLadderSteps(rule.Steps) != nil {
			return IncrementRule{}, ErrInvalidArgument
		}
	default:
		return IncrementRule{}, ErrInvalidArgument
	}
	return rule, nil
}

// MinIncrementForPrice returns the current minimum increment for currentPrice.
func MinIncrementForPrice(raw json.RawMessage, currentPrice int64, fallback int64) int64 {
	if fallback <= 0 {
		fallback = 1
	}
	rule, err := ParseIncrementRule(raw)
	if err != nil {
		return fallback
	}
	amount := rule.AmountForPrice(currentPrice)
	if amount <= 0 {
		return fallback
	}
	return amount
}

func ValidateAuctionPricing(startPrice, reservePrice, capPrice int64, raw json.RawMessage) error {
	if startPrice < 0 || reservePrice < 0 || capPrice < 0 {
		return ErrInvalidArgument
	}
	if capPrice > 0 && capPrice <= startPrice {
		return ErrInvalidArgument
	}
	if capPrice > 0 && reservePrice > 0 && reservePrice > capPrice {
		return ErrInvalidArgument
	}
	_, err := ParseIncrementRule(raw)
	return err
}

func ValidateBidPrice(startPrice, currentPrice, capPrice, price int64, rule IncrementRule) string {
	if rule.MaxBidSteps <= 0 {
		return BidRejectStepMismatch
	}
	amount := rule.AmountForPrice(currentPrice)
	if amount <= 0 {
		return BidRejectStepMismatch
	}
	if price <= startPrice {
		return BidRejectBelowStartPrice
	}
	if capPrice > 0 && price > capPrice {
		return BidRejectAboveCapPrice
	}
	isCapBid := capPrice > 0 && price == capPrice
	if !isCapBid && (price-currentPrice)%amount != 0 {
		return BidRejectStepMismatch
	}
	if isCapBid {
		if price <= currentPrice {
			return BidRejectBelowMinIncrement
		}
	} else if price < currentPrice+amount {
		return BidRejectBelowMinIncrement
	}
	maxAllowed := currentPrice + amount*int64(rule.MaxBidSteps)
	if capPrice > 0 && maxAllowed > capPrice {
		maxAllowed = capPrice
	}
	if price > maxAllowed {
		return BidRejectAboveMaxBidSteps
	}
	return ""
}

func ValidateBidExpectedCurrentPrice(expectedCurrentPrice, currentPrice, capPrice, price int64, rule IncrementRule) string {
	if expectedCurrentPrice < 0 {
		return BidRejectStaleAuctionState
	}
	if expectedCurrentPrice > currentPrice {
		return BidRejectStaleAuctionState
	}
	if rule.MaxBidSteps <= 0 {
		return BidRejectStepMismatch
	}
	amount := rule.AmountForPrice(expectedCurrentPrice)
	if amount <= 0 {
		return BidRejectStepMismatch
	}
	maxAllowed := expectedCurrentPrice + amount*int64(rule.MaxBidSteps)
	if capPrice > 0 && maxAllowed > capPrice {
		maxAllowed = capPrice
	}
	if expectedCurrentPrice < currentPrice && price > maxAllowed {
		return BidRejectAboveExpectedMaxBidSteps
	}
	return ""
}

func (r IncrementRule) AmountForPrice(currentPrice int64) int64 {
	switch r.Type {
	case IncrementRuleTypeFixed:
		return r.Amount
	case IncrementRuleTypeLadder:
		for _, step := range r.Steps {
			if currentPrice < step.Min {
				continue
			}
			if step.Max == nil || currentPrice < *step.Max {
				return step.Amount
			}
		}
	}
	return 0
}

func validateLadderSteps(steps []IncrementStep) error {
	if len(steps) == 0 || len(steps) > maxIncrementRuleSteps {
		return ErrInvalidArgument
	}
	if steps[0].Min != 0 {
		return ErrInvalidArgument
	}
	for i, step := range steps {
		if step.Min < 0 || step.Amount <= 0 {
			return ErrInvalidArgument
		}
		if i < len(steps)-1 {
			if step.Max == nil || *step.Max <= step.Min {
				return ErrInvalidArgument
			}
			if steps[i+1].Min != *step.Max {
				return ErrInvalidArgument
			}
			continue
		}
		if step.Max != nil {
			return ErrInvalidArgument
		}
	}
	return nil
}
