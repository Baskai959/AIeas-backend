package service

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	adminapp "aieas_backend/internal/modules/admin/app"
	"aieas_backend/internal/tests/repository"
)

func TestFeatureFlagDecideAllowlistRolloutAndDisabled(t *testing.T) {
	ctx := context.Background()
	repo := repository.NewMemoryConfigRepository()
	svc := adminapp.NewFeatureFlagService(repo, nil)
	flag, err := svc.Update(ctx, domain.FeatureFlag{Key: "feature.checkout.newpay", Enabled: true, RolloutPercentage: 0, Allowlist: []string{"u1"}}, "admin")
	if err != nil {
		t.Fatalf("update flag: %v", err)
	}
	if !flag.Enabled || flag.Key != "feature.checkout.newpay" {
		t.Fatalf("unexpected flag: %+v", flag)
	}
	if !svc.Decide(ctx, "feature.checkout.newpay", "u1") {
		t.Fatalf("allowlist user should be enabled")
	}
	if svc.Decide(ctx, "feature.checkout.newpay", "u2") {
		t.Fatalf("0 rollout non-allowlisted user should be disabled")
	}
	_, err = svc.Update(ctx, domain.FeatureFlag{Key: "feature.checkout.newpay", Enabled: true, RolloutPercentage: 100}, "admin")
	if err != nil {
		t.Fatalf("update 100 rollout: %v", err)
	}
	if !svc.Decide(ctx, "feature.checkout.newpay", "u2") {
		t.Fatalf("100 rollout should enable user")
	}
	_, err = svc.Update(ctx, domain.FeatureFlag{Key: "feature.checkout.newpay", Enabled: false, RolloutPercentage: 100, Allowlist: []string{"u1"}}, "admin")
	if err != nil {
		t.Fatalf("disable flag: %v", err)
	}
	if svc.Decide(ctx, "feature.checkout.newpay", "u1") {
		t.Fatalf("disabled flag should override allowlist")
	}
}

func TestFeatureFlagCacheAndInvalidate(t *testing.T) {
	ctx := context.Background()
	repo := repository.NewMemoryConfigRepository()
	svc := adminapp.NewFeatureFlagService(repo, nil)
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	svc.SetNowFunc(func() time.Time { return now })
	_, _ = svc.Update(ctx, domain.FeatureFlag{Key: "feature.search.rank", Enabled: true, RolloutPercentage: 100}, "admin")
	if !svc.Decide(ctx, "feature.search.rank", "u1") {
		t.Fatalf("expected initial enabled decision")
	}
	encoded := []byte(`{"key":"feature.search.rank","enabled":false,"rolloutPercentage":0}`)
	if err := repo.Upsert(ctx, &domain.ConfigItem{Key: "feature.search.rank", Value: encoded}); err != nil {
		t.Fatalf("direct repo upsert: %v", err)
	}
	if !svc.Decide(ctx, "feature.search.rank", "u1") {
		t.Fatalf("expected cached decision before invalidate")
	}
	svc.Invalidate("feature.search.rank")
	if svc.Decide(ctx, "feature.search.rank", "u1") {
		t.Fatalf("expected invalidated cache to reload disabled flag")
	}
}

func TestFeatureFlagRejectsInvalidNamespace(t *testing.T) {
	svc := adminapp.NewFeatureFlagService(repository.NewMemoryConfigRepository(), nil)
	if _, err := svc.Update(context.Background(), domain.FeatureFlag{Key: "checkout.newpay", Enabled: true}, "admin"); err != domain.ErrInvalidArgument {
		t.Fatalf("expected invalid argument for non feature namespace, got %v", err)
	}
}
