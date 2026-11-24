package notification

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/database"
)

// Integration tests run against a real PostgreSQL when TEST_DATABASE_URL is set
// (make test-integration starts one with Docker Compose).
func testRepo(t *testing.T) *Repository {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	if err := database.Migrate(context.Background(), url); err != nil {
		t.Fatal(err)
	}
	db, err := database.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return NewRepository(db)
}

func newNotification(key string) Notification {
	n := Notification{ID: uuid.New(), Recipient: "+905551234567", Channel: ChannelSMS,
		Content: "hi", Priority: PriorityNormal, Status: StatusPending, MaxAttempts: 3}
	if key != "" {
		n.IdempotencyKey = &key
	}
	return n
}

func TestRepositoryIdempotentInsert(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()
	key := "it-" + uuid.NewString()

	first, err := repo.InsertMany(ctx, []Notification{newNotification(key)})
	if err != nil || !first[0].Created {
		t.Fatalf("first insert: %+v %v", first, err)
	}
	second, err := repo.InsertMany(ctx, []Notification{newNotification(key), newNotification("")})
	if err != nil {
		t.Fatal(err)
	}
	if second[0].Created || second[0].Notification.ID != first[0].Notification.ID {
		t.Fatal("duplicate key must return the original row")
	}
	if !second[1].Created {
		t.Fatal("row without key must be inserted")
	}
}

func TestRepositoryClaimIsExclusiveAndCancelRules(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()
	res, err := repo.InsertMany(ctx, []Notification{newNotification(""), newNotification("")})
	if err != nil {
		t.Fatal(err)
	}
	a, b := res[0].Notification.ID, res[1].Notification.ID

	n, ok, err := repo.Claim(ctx, a)
	if err != nil || !ok || n.Status != StatusProcessing || n.Attempts != 1 {
		t.Fatalf("first claim: %+v ok=%v err=%v", n, ok, err)
	}
	if _, ok, _ := repo.Claim(ctx, a); ok {
		t.Fatal("second claim of the same notification must fail")
	}
	if _, err := repo.Cancel(ctx, a); err != ErrNotCancellable {
		t.Fatalf("cancelling a processing notification: got %v", err)
	}

	if n, err := repo.Cancel(ctx, b); err != nil || n.Status != StatusCancelled {
		t.Fatalf("cancel pending: %+v %v", n, err)
	}
	if _, ok, _ := repo.Claim(ctx, b); ok {
		t.Fatal("cancelled notification must not be claimable")
	}
	if _, err := repo.Cancel(ctx, uuid.New()); err != ErrNotFound {
		t.Fatalf("cancel unknown: got %v", err)
	}
}

func TestRepositoryListFilters(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()
	batch := uuid.New()
	ns := make([]Notification, 5)
	for i := range ns {
		ns[i] = newNotification("")
		ns[i].BatchID = &batch
	}
	ns[0].Channel = ChannelEmail
	ns[0].Recipient = "a@b.com"
	if _, err := repo.InsertMany(ctx, ns); err != nil {
		t.Fatal(err)
	}
	sms := ChannelSMS
	page, err := repo.List(ctx, ListFilter{BatchID: &batch, Channel: &sms, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 4 || len(page.Items) != 2 {
		t.Fatalf("total=%d items=%d, want 4 and 2", page.Total, len(page.Items))
	}
	counts, total, err := repo.BatchSummary(ctx, batch)
	if err != nil || total != 5 || counts[StatusPending] != 5 {
		t.Fatalf("summary %v %d %v", counts, total, err)
	}
}
