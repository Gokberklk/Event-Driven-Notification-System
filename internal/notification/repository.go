package notification

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository persists notifications in PostgreSQL.
type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

const columns = `id, batch_id, idempotency_key, recipient, channel, content, priority, status,
	attempts, max_attempts, last_error, provider_message_id, correlation_id,
	scheduled_at, next_attempt_at, sent_at, created_at, updated_at, template_id, trace_parent`

func scan(row pgx.Row) (Notification, error) {
	var n Notification
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &n.Recipient, &n.Channel, &n.Content,
		&n.Priority, &n.Status, &n.Attempts, &n.MaxAttempts, &n.LastError, &n.ProviderMessageID,
		&n.CorrelationID, &n.ScheduledAt, &n.NextAttemptAt, &n.SentAt, &n.CreatedAt, &n.UpdatedAt, &n.TemplateID, &n.TraceParent)
	return n, err
}

// InsertResult reports whether a row was newly created or already existed
// under the same idempotency key.
type InsertResult struct {
	Notification Notification
	Created      bool
}

// InsertMany inserts notifications in a single transaction. Rows whose
// idempotency key already exists are not inserted; the existing row is
// returned instead with Created=false.
func (r *Repository) InsertMany(ctx context.Context, ns []Notification) ([]InsertResult, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	batch := &pgx.Batch{}
	for _, n := range ns {
		batch.Queue(`INSERT INTO notifications
			(id, batch_id, idempotency_key, recipient, channel, content, priority, status,
			 max_attempts, correlation_id, scheduled_at, template_id, trace_parent)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+columns,
			n.ID, n.BatchID, n.IdempotencyKey, n.Recipient, n.Channel, n.Content, n.Priority,
			n.Status, n.MaxAttempts, n.CorrelationID, n.ScheduledAt, n.TemplateID, n.TraceParent)
	}
	br := tx.SendBatch(ctx, batch)
	results := make([]InsertResult, len(ns))
	var conflicts []int
	for i := range ns {
		got, err := scan(br.QueryRow())
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			conflicts = append(conflicts, i)
		case err != nil:
			br.Close()
			return nil, fmt.Errorf("insert notification %d: %w", i, err)
		default:
			results[i] = InsertResult{Notification: got, Created: true}
		}
	}
	if err := br.Close(); err != nil {
		return nil, err
	}
	for _, i := range conflicts {
		existing, err := scan(tx.QueryRow(ctx,
			`SELECT `+columns+` FROM notifications WHERE idempotency_key = $1`, *ns[i].IdempotencyKey))
		if err != nil {
			return nil, fmt.Errorf("load existing idempotent notification: %w", err)
		}
		results[i] = InsertResult{Notification: existing, Created: false}
	}
	return results, tx.Commit(ctx)
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (Notification, error) {
	n, err := scan(r.db.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNotFound
	}
	return n, err
}

// List returns a filtered, paginated list ordered by newest first.
func (r *Repository) List(ctx context.Context, f ListFilter) (Page, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		add("status = $%d", *f.Status)
	}
	if f.Channel != nil {
		add("channel = $%d", *f.Channel)
	}
	if f.BatchID != nil {
		add("batch_id = $%d", *f.BatchID)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}

	page := Page{Items: []Notification{}, Limit: f.Limit, Offset: f.Offset}
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM notifications`+clause, args...).Scan(&page.Total); err != nil {
		return page, err
	}

	args = append(args, f.Limit, f.Offset)
	rows, err := r.db.Query(ctx, fmt.Sprintf(`SELECT %s FROM notifications%s
		ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d`, columns, clause, len(args)-1, len(args)), args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return page, err
		}
		page.Items = append(page.Items, n)
	}
	return page, rows.Err()
}

// BatchSummary counts notifications in a batch by status.
func (r *Repository) BatchSummary(ctx context.Context, batchID uuid.UUID) (map[Status]int, int, error) {
	rows, err := r.db.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	counts := map[Status]int{}
	total := 0
	for rows.Next() {
		var s Status
		var c int
		if err := rows.Scan(&s, &c); err != nil {
			return nil, 0, err
		}
		counts[s] = c
		total += c
	}
	return counts, total, rows.Err()
}

// Cancel moves a pending or scheduled notification to cancelled.
func (r *Repository) Cancel(ctx context.Context, id uuid.UUID) (Notification, error) {
	n, err := scan(r.db.QueryRow(ctx, `UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'scheduled') RETURNING `+columns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, gerr := r.Get(ctx, id); gerr != nil {
			return n, gerr
		}
		return n, ErrNotCancellable
	}
	return n, err
}

// Claim atomically moves a notification into processing. It returns
// ok=false if the notification is not in a claimable state (already sent,
// cancelled or being processed by another worker), which is what prevents
// duplicate sends when the same ID is queued twice.
func (r *Repository) Claim(ctx context.Context, id uuid.UUID) (Notification, bool, error) {
	n, err := scan(r.db.QueryRow(ctx, `UPDATE notifications
		SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'scheduled') RETURNING `+columns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return n, false, nil
	}
	return n, err == nil, err
}

func (r *Repository) MarkSent(ctx context.Context, id uuid.UUID, providerID string) error {
	_, err := r.db.Exec(ctx, `UPDATE notifications SET status = 'sent', provider_message_id = $2,
		last_error = NULL, next_attempt_at = NULL, sent_at = now(), updated_at = now() WHERE id = $1`, id, providerID)
	return err
}

// MarkRetry puts a notification back to pending with the next attempt time.
func (r *Repository) MarkRetry(ctx context.Context, id uuid.UUID, reason string, next time.Time) error {
	_, err := r.db.Exec(ctx, `UPDATE notifications SET status = 'pending', last_error = $2,
		next_attempt_at = $3, updated_at = now() WHERE id = $1`, id, reason, next)
	return err
}

func (r *Repository) MarkFailed(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE notifications SET status = 'failed', last_error = $2,
		next_attempt_at = NULL, updated_at = now() WHERE id = $1`, id, reason)
	return err
}

// Stale returns notifications that should be in the queue but may have been
// lost (e.g. enqueue failed after commit, or a worker crashed mid-send).
func (r *Repository) Stale(ctx context.Context, olderThan time.Duration, limit int) ([]Notification, error) {
	rows, err := r.db.Query(ctx, `SELECT `+columns+` FROM notifications
		WHERE (status IN ('pending', 'scheduled') AND COALESCE(next_attempt_at, scheduled_at, created_at) < now() - $1::interval)
		   OR (status = 'processing' AND updated_at < now() - $1::interval)
		ORDER BY created_at LIMIT $2`, fmt.Sprintf("%d seconds", int(olderThan.Seconds())), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ResetStuck moves a notification stuck in processing back to pending.
func (r *Repository) ResetStuck(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.Exec(ctx, `UPDATE notifications SET status = 'pending', updated_at = now()
		WHERE id = $1 AND status = 'processing'`, id)
	return err
}
