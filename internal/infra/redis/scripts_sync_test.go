package redis

import (
	"os"
	"strings"
	"testing"
)

func TestLuaFilesMatchEmbeddedScripts(t *testing.T) {
	cases := []struct {
		file string
		name string
	}{
		{file: "../../../scripts/lua/bid.lua", name: ScriptBidPlace},
		{file: "../../../scripts/lua/rate_limit.lua", name: ScriptRateLimit},
	}
	scripts := DefaultScripts()
	for _, tc := range cases {
		raw, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		if got, want := scripts[tc.name], strings.TrimSpace(string(raw)); got != want {
			t.Fatalf("embedded script %s is out of sync with %s", tc.name, tc.file)
		}
	}
}

func TestBidLuaPublishesSamePayloadToAuctionChannel(t *testing.T) {
	body := DefaultScripts()[ScriptBidPlace]
	for _, needle := range []string{
		`local payload = build_result`,
		`local channel = "auction:" .. tostring(auction_id) .. ":events"`,
		`redis.call("PUBLISH", channel, payload)`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("bid lua missing %q", needle)
		}
	}
	if strings.Index(body, `redis.call("XADD"`) > strings.Index(body, `redis.call("PUBLISH"`) {
		t.Fatalf("bid lua must XADD before PUBLISH to keep stream as source of truth")
	}
}
