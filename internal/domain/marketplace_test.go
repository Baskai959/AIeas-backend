package domain

import (
	"encoding/json"
	"testing"
)

func TestMerchantViewJSONIncludesEmptyLocation(t *testing.T) {
	data, err := json.Marshal(MerchantView{ID: "2001", Name: "商家001"})
	if err != nil {
		t.Fatalf("marshal merchant view: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal merchant view: %v", err)
	}
	if _, ok := got["location"]; !ok {
		t.Fatalf("expected location field in merchant view json, got %s", data)
	}
}
