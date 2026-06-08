package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	mcpMoneyUnit     = "元"
	mcpMoneyCurrency = "CNY"
)

type mcpMoneyValue struct {
	Value    string `json:"value"`
	Unit     string `json:"unit"`
	Currency string `json:"currency"`
}

func mcpMoneyDisplayData(data interface{}) (interface{}, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return mcpConvertMoneyFields(decoded, nil), nil
}

func mcpConvertMoneyFields(value interface{}, path []string) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			converted := mcpConvertMoneyFields(item, append(path, key))
			if nextKey, ok := mcpMoneyOutputKey(key, path); ok {
				if money, ok := mcpMoneyFromJSONNumber(converted); ok {
					out[nextKey] = money
					continue
				}
			}
			out[key] = converted
		}
		return out
	case []interface{}:
		for i, item := range typed {
			typed[i] = mcpConvertMoneyFields(item, path)
		}
		return typed
	default:
		return value
	}
}

func mcpMoneyOutputKey(key string, path []string) (string, bool) {
	if mcpInIncrementRule(path) {
		switch key {
		case "amount", "min", "max":
			return key, true
		}
	}
	switch key {
	case "amount", "price", "startPrice", "reservePrice", "capPrice", "depositAmount", "dealPrice", "currentPrice", "bidPrice", "expectedCurrentPrice":
		return key, true
	case "gmvCent":
		return "gmv", true
	case "totalDealCent":
		return "totalDeal", true
	case "dealGmvCent":
		return "dealGmv", true
	case "paidGmvCent":
		return "paidGmv", true
	}
	if strings.HasSuffix(key, "Price") || strings.HasSuffix(key, "Amount") {
		return key, true
	}
	if strings.HasSuffix(key, "Cent") {
		trimmed := strings.TrimSuffix(key, "Cent")
		if trimmed != "" {
			return trimmed, true
		}
	}
	return "", false
}

func mcpInIncrementRule(path []string) bool {
	for _, part := range path {
		if part == "incrementRule" {
			return true
		}
	}
	return false
}

func mcpMoneyFromJSONNumber(value interface{}) (mcpMoneyValue, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return mcpMoneyValue{}, false
	}
	cents, err := number.Int64()
	if err != nil {
		return mcpMoneyValue{}, false
	}
	return mcpMoneyValue{Value: mcpFormatYuan(cents), Unit: mcpMoneyUnit, Currency: mcpMoneyCurrency}, true
}

func mcpFormatYuan(cents int64) string {
	sign := ""
	if cents < 0 {
		sign = "-"
		cents = -cents
	}
	return fmt.Sprintf("%s%d.%02d", sign, cents/100, cents%100)
}
