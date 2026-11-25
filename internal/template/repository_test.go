package template

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/database"
)

func TestRepository(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	if err := database.Migrate(ctx, url); err != nil {
		t.Fatal(err)
	}
	db, err := database.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := NewRepository(db)

	name := "it-" + uuid.NewString()[:8]
	created, err := repo.Create(ctx, Template{ID: uuid.New(), Name: name, Channel: "push", Body: "Hi {{name}}", Variables: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Create(ctx, Template{ID: uuid.New(), Name: name, Channel: "push", Body: "x"}); err != ErrDuplicateName {
		t.Fatalf("duplicate name: got %v", err)
	}
	got, err := repo.Get(ctx, created.ID)
	if err != nil || got.Body != "Hi {{name}}" || len(got.Variables) != 1 {
		t.Fatalf("get: %+v %v", got, err)
	}
	if err := repo.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Get(ctx, created.ID); err != ErrNotFound {
		t.Fatalf("after delete: got %v", err)
	}
}
