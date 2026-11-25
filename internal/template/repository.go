package template

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository persists templates in PostgreSQL.
type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

const columns = `id, name, channel, body, variables, created_at, updated_at`

func scan(row pgx.Row) (Template, error) {
	var t Template
	err := row.Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.Variables, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}

func (r *Repository) Create(ctx context.Context, t Template) (Template, error) {
	if t.Variables == nil {
		t.Variables = []string{}
	}
	out, err := scan(r.db.QueryRow(ctx, `INSERT INTO templates (id, name, channel, body, variables)
		VALUES ($1, $2, $3, $4, $5) RETURNING `+columns, t.ID, t.Name, t.Channel, t.Body, t.Variables))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return out, ErrDuplicateName
	}
	return out, err
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (Template, error) {
	t, err := scan(r.db.QueryRow(ctx, `SELECT `+columns+` FROM templates WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

func (r *Repository) List(ctx context.Context) ([]Template, error) {
	rows, err := r.db.Query(ctx, `SELECT `+columns+` FROM templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Template{}
	for rows.Next() {
		t, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM templates WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
