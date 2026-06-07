package ws

import "testing"

func TestHandshakeLimiterPerIPUserAuctionAndDisabled(t *testing.T) {
	t.Run("per ip", func(t *testing.T) {
		l := NewHandshakeLimiter(1, 0, 0)
		if ok, reason := l.Allow("1.1.1.1", "u1", 100); !ok || reason != "" {
			t.Fatalf("first ip request should pass, ok=%v reason=%q", ok, reason)
		}
		if ok, reason := l.Allow("1.1.1.1", "u2", 101); ok || reason != "rate_limit_ip" {
			t.Fatalf("second ip request should be rate limited, ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("per user", func(t *testing.T) {
		l := NewHandshakeLimiter(0, 1, 0)
		if ok, reason := l.Allow("1.1.1.1", "u1", 100); !ok || reason != "" {
			t.Fatalf("first user request should pass, ok=%v reason=%q", ok, reason)
		}
		if ok, reason := l.Allow("2.2.2.2", "u1", 101); ok || reason != "rate_limit_user" {
			t.Fatalf("second user request should be rate limited, ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("per auction", func(t *testing.T) {
		l := NewHandshakeLimiter(0, 0, 1)
		if ok, reason := l.Allow("1.1.1.1", "u1", 100); !ok || reason != "" {
			t.Fatalf("first auction request should pass, ok=%v reason=%q", ok, reason)
		}
		if ok, reason := l.Allow("2.2.2.2", "u2", 100); ok || reason != "rate_limit_auction" {
			t.Fatalf("second auction request should be rate limited, ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		l := NewHandshakeLimiter(0, 0, 0)
		for i := 0; i < 10; i++ {
			if ok, reason := l.Allow("1.1.1.1", "u1", 100); !ok || reason != "" {
				t.Fatalf("disabled limiter should pass, ok=%v reason=%q", ok, reason)
			}
		}
	})
}

func TestHandshakeLimiterSkipsAnonymousUser(t *testing.T) {
	l := NewHandshakeLimiter(0, 1, 0)
	for i := 0; i < 3; i++ {
		if ok, reason := l.Allow("1.1.1.1", "anonymous", 100); !ok || reason != "" {
			t.Fatalf("anonymous user should skip per-user bucket, ok=%v reason=%q", ok, reason)
		}
	}
	for i := 0; i < 3; i++ {
		if ok, reason := l.Allow("1.1.1.1", "", 100); !ok || reason != "" {
			t.Fatalf("empty user should skip per-user bucket, ok=%v reason=%q", ok, reason)
		}
	}
}
