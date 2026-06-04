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
		{file: "../../../scripts/lua/hammer.lua", name: ScriptHammer},
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

func TestBidLuaAcceptedBidStreamIsSourceOfTruth(t *testing.T) {
	body := DefaultScripts()[ScriptBidPlace]
	if !strings.Contains(body, `redis.call("XADD"`) {
		t.Fatalf("bid lua must XADD accepted events to keep stream as source of truth")
	}
	if strings.Contains(body, `"MAXLEN"`) {
		t.Fatalf("bid lua must not trim stream in the hot path; XTRIM is owned by BidStreamTrimWorker")
	}
	if strings.Contains(body, `redis.call("PUBLISH"`) {
		t.Fatalf("bid lua must not call PUBLISH; PubSub is now published from Go side")
	}
	if strings.Contains(body, `active_streams_key`) {
		t.Fatalf("bid lua must not reference active_streams_key; membership is owned by InitAuction")
	}
	for _, forbidden := range []string{`enrolled_key`, `deposit_key`, `redis.call("SISMEMBER"`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("bid lua must not do enrollment/deposit checks; %s is owned by BidService", forbidden)
		}
	}
}

func TestBidLuaRejectPathIsLightweight(t *testing.T) {
	body := DefaultScripts()[ScriptBidPlace]
	if strings.Contains(body, `redis.call("TYPE"`) {
		t.Fatalf("bid lua hot path should not run key TYPE checks")
	}
	rejectStart := strings.Index(body, `local function reject(reason)`)
	if rejectStart < 0 {
		t.Fatalf("bid lua missing reject function")
	}
	rejectEnd := strings.Index(body[rejectStart:], `if status ~= "RUNNING"`)
	if rejectEnd < 0 {
		t.Fatalf("could not find reject function end")
	}
	rejectBody := body[rejectStart : rejectStart+rejectEnd]
	for _, forbidden := range []string{`append_event`, `XADD`, `PUBLISH`, `SADD`, `HSET`} {
		if strings.Contains(rejectBody, forbidden) {
			t.Fatalf("reject path should not call %s", forbidden)
		}
	}
}

func TestBidLuaAcceptedPathDoesNotMaintainRanking(t *testing.T) {
	body := DefaultScripts()[ScriptBidPlace]
	for _, forbidden := range []string{
		`redis.call("ZREM", ranking_key`,
		`redis.call("ZADD", ranking_key`,
		`redis.call("HGET", user_bids_key`,
		`redis.call("HSET", user_bids_key`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("bid lua accepted path must not call %q; ranking is updated asynchronously by BidRecordWriter", forbidden)
		}
	}
}

func TestHammerLuaUsesStateLeaderInsteadOfRanking(t *testing.T) {
	body := DefaultScripts()[ScriptHammer]
	if strings.Contains(body, `ZREVRANGE`) || strings.Contains(body, `ranking_key`) {
		t.Fatalf("hammer lua must use state leader/current_price, not async ranking")
	}
	for _, needle := range []string{
		`redis.call("HGET", state_key, "leader_bidder_id")`,
		`redis.call("HGET", state_key, "current_price")`,
		`price >= reserve_price`,
	} {
		if !strings.Contains(body, needle) {
			t.Fatalf("hammer lua missing state-based close logic %q", needle)
		}
	}
}
