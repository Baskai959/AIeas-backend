package mcp

import (
	"encoding/json"
	"testing"
)

func TestMCPMoneyDisplayDataConvertsCentFieldsToYuan(t *testing.T) {
	input := map[string]interface{}{
		"startPrice":     int64(1000),
		"depositAmount":  int64(12345),
		"gmvCent":        int64(987654),
		"audioBytes":     int64(2048),
		"incrementRule":  json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":0,"max":2000,"amount":100},{"min":2000,"amount":500}]}`),
		"notMoneyNumber": int64(7),
	}

	display, err := mcpMoneyDisplayData(input)
	if err != nil {
		t.Fatalf("display data: %v", err)
	}
	encoded, err := json.Marshal(display)
	if err != nil {
		t.Fatalf("marshal display data: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("decode display data: %v", err)
	}

	assertMoneyValue(t, out["startPrice"], "10.00")
	assertMoneyValue(t, out["depositAmount"], "123.45")
	assertMoneyValue(t, out["gmv"], "9876.54")
	if _, ok := out["gmvCent"]; ok {
		t.Fatalf("gmvCent should be renamed to gmv: %+v", out)
	}
	if got := out["audioBytes"].(float64); got != 2048 {
		t.Fatalf("audioBytes should not be converted, got %v", out["audioBytes"])
	}
	if got := out["notMoneyNumber"].(float64); got != 7 {
		t.Fatalf("notMoneyNumber should not be converted, got %v", out["notMoneyNumber"])
	}

	rule := out["incrementRule"].(map[string]interface{})
	steps := rule["steps"].([]interface{})
	first := steps[0].(map[string]interface{})
	second := steps[1].(map[string]interface{})
	assertMoneyValue(t, first["min"], "0.00")
	assertMoneyValue(t, first["max"], "20.00")
	assertMoneyValue(t, first["amount"], "1.00")
	assertMoneyValue(t, second["min"], "20.00")
	assertMoneyValue(t, second["amount"], "5.00")
}

func assertMoneyValue(t *testing.T, raw interface{}, want string) {
	t.Helper()
	value, ok := raw.(map[string]interface{})
	if !ok {
		t.Fatalf("expected money object, got %T: %+v", raw, raw)
	}
	if got := value["value"]; got != want {
		t.Fatalf("value=%v want %s in %+v", got, want, value)
	}
	if got := value["unit"]; got != "元" {
		t.Fatalf("unit=%v want 元 in %+v", got, value)
	}
	if got := value["currency"]; got != "CNY" {
		t.Fatalf("currency=%v want CNY in %+v", got, value)
	}
}
