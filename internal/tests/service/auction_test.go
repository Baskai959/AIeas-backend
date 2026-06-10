package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/tests/repository"
)

type fixedAuctionIDGenerator struct {
	id uint64
}

func (g fixedAuctionIDGenerator) NextAuctionID() (uint64, error) {
	return g.id, nil
}

func (g fixedAuctionIDGenerator) NextOrderID() (uint64, error) {
	return g.id, nil
}

func TestAuctionServiceCreateGeneratesAuctionID(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	svc.SetIDGenerator(fixedAuctionIDGenerator{id: 123456789})

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if auction.AuctionID != 123456789 {
		t.Fatalf("expected generated auction ID, got %d", auction.AuctionID)
	}
	if auction.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected new auction to be PENDING_AUDIT, got %s", auction.Status)
	}

	stored, err := auctionRepo.FindByID(ctx, 123456789)
	if err != nil {
		t.Fatalf("find generated auction: %v", err)
	}
	if stored.AuctionID != auction.AuctionID {
		t.Fatalf("stored ID mismatch: got %d want %d", stored.AuctionID, auction.AuctionID)
	}
}

func TestAuctionServiceCreatePreservesProvidedAuctionID(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	svc.SetIDGenerator(fixedAuctionIDGenerator{id: 123456789})

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		AuctionID:      987654321,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if auction.AuctionID != 987654321 {
		t.Fatalf("expected provided auction ID, got %d", auction.AuctionID)
	}
}

func TestAuctionServiceCreateAllowsOptionalTiming(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
		DurationSec:    600,
	})
	if err != nil {
		t.Fatalf("create auction without start/end: %v", err)
	}
	if !auction.StartTime.IsZero() || !auction.EndTime.IsZero() || auction.DurationSec != 600 {
		t.Fatalf("expected optional schedule to remain unset with stored duration, got %+v", auction)
	}

	start := time.Now().UTC().Add(time.Hour)
	scheduled, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch 2",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		StartTime:      start,
		DurationSec:    300,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if err != nil {
		t.Fatalf("create scheduled auction: %v", err)
	}
	if !scheduled.EndTime.Equal(start.Add(300 * time.Second)) {
		t.Fatalf("expected end time derived from duration, got %s", scheduled.EndTime)
	}
}

func TestAuctionServiceStartWithTimingOverridesStoredDuration(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		Title:             "Watch",
		Category:          "luxury",
		ConditionGrade:    domain.ConditionNew,
		Description:       "rare watch",
		StartPrice:        1000,
		ReservePrice:      5000,
		CapPrice:          6000,
		AuctionType:       domain.AuctionTypeEnglish,
		DepositAmount:     100,
		DurationSec:       900,
		Status:            domain.AuctionStatusReady,
		AllowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	start := time.Now().UTC().Truncate(time.Second)
	end := start.Add(time.Minute)
	started, err := svc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, start, end)
	if err != nil {
		t.Fatalf("start with timing: %v", err)
	}
	if started.DurationSec != 60 {
		t.Fatalf("expected durationSec=60, got %d", started.DurationSec)
	}
	if !started.EndTime.Equal(end) {
		t.Fatalf("expected end time %s, got %s", end, started.EndTime)
	}
}

func TestAuctionServiceStartClearsStaleParticipation(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	realtime := repository.NewMemoryRealtimeStore()
	svc := NewAuctionServiceWithDeps(AuctionServiceDeps{Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Realtime: realtime})

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		Title:             "Watch",
		Category:          "luxury",
		ConditionGrade:    domain.ConditionNew,
		Description:       "rare watch",
		StartPrice:        1000,
		ReservePrice:      5000,
		CapPrice:          6000,
		AuctionType:       domain.AuctionTypeEnglish,
		DepositAmount:     100,
		DurationSec:       600,
		Status:            domain.AuctionStatusReady,
		AllowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if err := realtime.MarkEnrollment(ctx, auction.AuctionID, "u_stale"); err != nil {
		t.Fatalf("seed stale enrollment: %v", err)
	}
	start := time.Now().UTC().Truncate(time.Second)
	if _, err := svc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, start, start.Add(time.Minute)); err != nil {
		t.Fatalf("start with timing: %v", err)
	}
	state, exists, err := realtime.GetAuctionState(ctx, auction.AuctionID)
	if err != nil || !exists {
		t.Fatalf("get realtime state: exists=%v err=%v", exists, err)
	}
	if state.ParticipantCount != 0 {
		t.Fatalf("expected participant count reset on start, got %+v", state)
	}
	enrolled, depositReady, err := realtime.BidPrerequisites(ctx, auction.AuctionID, "u_stale")
	if err != nil || enrolled || depositReady {
		t.Fatalf("expected stale participation cleared, enrolled=%v depositReady=%v err=%v", enrolled, depositReady, err)
	}
}

func TestAuctionServiceCreateRejectsSystemStatus(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	start := time.Now().UTC().Add(time.Minute)
	_, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		Status:         domain.AuctionStatusRunning,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAuctionServiceCreateRejectsResetAntiExtendShorterThanAntiSniping(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	start := time.Now().UTC().Add(time.Minute)
	_, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AntiSnipingSec: 60,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeReset,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAuctionServiceUpdateAllowsPendingAuditAndRejectsReadyOrSystemStatus(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		Status:         domain.AuctionStatusDraft,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	pendingAudit := domain.AuctionStatusPendingAudit
	updated, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Status: &pendingAudit})
	if err != nil {
		t.Fatalf("set pending audit: %v", err)
	}
	if updated.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected PENDING_AUDIT, got %s", updated.Status)
	}

	ready := domain.AuctionStatusReady
	if _, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Status: &ready}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ready invalid argument, got %v", err)
	}

	running := domain.AuctionStatusRunning
	if _, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Status: &running}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAuctionServiceUpdateRejectsResetAntiExtendShorterThanAntiSniping(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		Status:         domain.AuctionStatusDraft,
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	mode := domain.AuctionExtendModeReset
	antiSnipingSec := 60
	antiExtendSec := 30
	if _, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		AntiSnipingSec: &antiSnipingSec,
		AntiExtendSec:  &antiExtendSec,
		AntiExtendMode: &mode,
	}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAuctionServiceUpdateReadyLotCanResubmitAuditAfterEdit(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		Title:             "Approved Watch",
		Category:          "luxury",
		ConditionGrade:    domain.ConditionNew,
		Description:       "rare watch",
		StartPrice:        1000,
		ReservePrice:      5000,
		CapPrice:          6000,
		AuctionType:       domain.AuctionTypeEnglish,
		DepositAmount:     100,
		Status:            domain.AuctionStatusReady,
		AllowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create ready auction: %v", err)
	}

	title := "Approved Watch Updated"
	pendingAudit := domain.AuctionStatusPendingAudit
	updated, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{
		ActorID:   "u_2001",
		ActorRole: domain.RoleMerchant,
		Title:     &title,
		Status:    &pendingAudit,
	})
	if err != nil {
		t.Fatalf("update ready auction and resubmit audit: %v", err)
	}
	if updated.Status != domain.AuctionStatusPendingAudit || updated.Title != title {
		t.Fatalf("expected updated lot to return PENDING_AUDIT with new title, got %+v", updated)
	}
	if updated.AuditTaskID == "" {
		t.Fatalf("expected resubmitted ready lot to carry audit task id")
	}
}

func TestAuctionServiceAuditCallbackUpdatesPendingLot(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if auction.AuditTaskID == "" {
		t.Fatalf("expected pending auction to carry audit task id")
	}

	result, err := svc.HandleAuditCallback(ctx, AuctionAuditCallbackInput{
		RequestID:  "audit-approved-1",
		Status:     "COMPLETED",
		Success:    true,
		IsApproved: true,
		Context: map[string]any{
			"auctionId": auction.AuctionID,
			"scope":     "auction_lot_content",
			"taskId":    auction.AuditTaskID,
		},
	})
	if err != nil {
		t.Fatalf("handle approved callback: %v", err)
	}
	if !result.Accepted || result.LotStatus != string(domain.AuctionStatusReady) {
		t.Fatalf("unexpected approved callback result: %+v", result)
	}
	stored, err := auctionRepo.FindByID(ctx, auction.AuctionID)
	if err != nil {
		t.Fatalf("find approved auction: %v", err)
	}
	if stored.Status != domain.AuctionStatusReady {
		t.Fatalf("expected READY after approved callback, got %s", stored.Status)
	}
	if stored.AuditTaskID != "" {
		t.Fatalf("expected audit task id to be cleared after approval, got %q", stored.AuditTaskID)
	}

	rejected, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Rejected Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
	})
	if err != nil {
		t.Fatalf("create rejected auction: %v", err)
	}
	rejectedTaskID := rejected.AuditTaskID
	if _, err := svc.HandleAuditCallback(ctx, AuctionAuditCallbackInput{
		RequestID:     "audit-rejected-1",
		Status:        "COMPLETED",
		Success:       true,
		IsApproved:    false,
		RejectReasons: []string{"商品信息中存在品牌信息不一致，涉嫌虚假宣传。"},
		Context: map[string]any{
			"auctionId": rejected.AuctionID,
			"taskId":    rejectedTaskID,
		},
	}); err != nil {
		t.Fatalf("handle rejected callback: %v", err)
	}
	rejectedStored, err := auctionRepo.FindByID(ctx, rejected.AuctionID)
	if err != nil {
		t.Fatalf("find rejected auction: %v", err)
	}
	if rejectedStored.Status != domain.AuctionStatusAuditRejected {
		t.Fatalf("expected AUDIT_REJECTED after rejected callback, got %s", rejectedStored.Status)
	}
	if rejectedStored.AuditTaskID != "" {
		t.Fatalf("expected audit task id to be cleared after rejection, got %q", rejectedStored.AuditTaskID)
	}
	if rejectedStored.AuditRejectReason != "商品信息中存在品牌信息不一致，涉嫌虚假宣传。" {
		t.Fatalf("expected audit reject reason after rejection, got %q", rejectedStored.AuditRejectReason)
	}
	resubmitStatus := domain.AuctionStatusPendingAudit
	resubmitted, err := svc.Update(ctx, rejected.AuctionID, UpdateAuctionInput{
		ActorID:   "u_2001",
		ActorRole: domain.RoleMerchant,
		Status:    &resubmitStatus,
	})
	if err != nil {
		t.Fatalf("resubmit rejected auction: %v", err)
	}
	if resubmitted.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected rejected auction can be resubmitted, got %s", resubmitted.Status)
	}
	if resubmitted.AuditRejectReason != "" {
		t.Fatalf("expected audit reject reason to be cleared after resubmit, got %q", resubmitted.AuditRejectReason)
	}
	if resubmitted.AuditTaskID == "" || resubmitted.AuditTaskID == rejectedTaskID {
		t.Fatalf("expected resubmit to create a fresh audit task id, old=%q new=%q", rejectedTaskID, resubmitted.AuditTaskID)
	}

	stale, err := svc.HandleAuditCallback(ctx, AuctionAuditCallbackInput{
		RequestID:  "audit-stale-approved-1",
		Status:     "COMPLETED",
		Success:    true,
		IsApproved: true,
		Context: map[string]any{
			"auctionId": rejected.AuctionID,
			"taskId":    rejectedTaskID,
		},
	})
	if err != nil {
		t.Fatalf("handle stale approved callback: %v", err)
	}
	if stale.LotStatus != string(domain.AuctionStatusPendingAudit) {
		t.Fatalf("expected stale callback to keep pending status, got %+v", stale)
	}
	afterStale, err := auctionRepo.FindByID(ctx, rejected.AuctionID)
	if err != nil {
		t.Fatalf("find after stale callback: %v", err)
	}
	if afterStale.Status != domain.AuctionStatusPendingAudit || afterStale.AuditTaskID != resubmitted.AuditTaskID {
		t.Fatalf("expected stale callback not to mutate lot, got %+v", afterStale)
	}

	if _, err := svc.HandleAuditCallback(ctx, AuctionAuditCallbackInput{
		RequestID:  "audit-resubmit-approved-1",
		Status:     "COMPLETED",
		Success:    true,
		IsApproved: true,
		Context: map[string]any{
			"auctionId": rejected.AuctionID,
			"taskId":    resubmitted.AuditTaskID,
		},
	}); err != nil {
		t.Fatalf("handle resubmitted approved callback: %v", err)
	}
	resubmittedApproved, err := auctionRepo.FindByID(ctx, rejected.AuctionID)
	if err != nil {
		t.Fatalf("find resubmitted approved auction: %v", err)
	}
	if resubmittedApproved.Status != domain.AuctionStatusReady {
		t.Fatalf("expected current task callback to approve lot, got %s", resubmittedApproved.Status)
	}

	noConclusion, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Pending Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
	})
	if err != nil {
		t.Fatalf("create no-conclusion auction: %v", err)
	}
	if _, err := svc.HandleAuditCallback(ctx, AuctionAuditCallbackInput{
		RequestID: "audit-no-conclusion-1",
		Status:    "FAILED",
		Success:   false,
		Context: map[string]any{
			"auctionId": noConclusion.AuctionID,
		},
	}); err != nil {
		t.Fatalf("handle no-conclusion callback: %v", err)
	}
	pendingStored, err := auctionRepo.FindByID(ctx, noConclusion.AuctionID)
	if err != nil {
		t.Fatalf("find no-conclusion auction: %v", err)
	}
	if pendingStored.Status != domain.AuctionStatusPendingAudit {
		t.Fatalf("expected PENDING_AUDIT after no-conclusion callback, got %s", pendingStored.Status)
	}
}
