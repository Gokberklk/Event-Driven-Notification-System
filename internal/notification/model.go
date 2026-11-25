// Package notification contains the notification domain: model, validation,
// persistence and the application service used by the HTTP API.
package notification

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func (c Channel) Valid() bool {
	return c == ChannelSMS || c == ChannelEmail || c == ChannelPush
}

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityNormal || p == PriorityLow
}

// Rank orders priorities for the queue: lower rank is dequeued first.
func (p Priority) Rank() int {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityLow:
		return 2
	default:
		return 1
	}
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusScheduled, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// Notification is a single message to be delivered on one channel.
type Notification struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batchId,omitempty"`
	IdempotencyKey    *string    `json:"idempotencyKey,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           Channel    `json:"channel"`
	Content           string     `json:"content"`
	Priority          Priority   `json:"priority"`
	Status            Status     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"maxAttempts"`
	LastError         *string    `json:"lastError,omitempty"`
	ProviderMessageID *string    `json:"providerMessageId,omitempty"`
	TemplateID        *uuid.UUID `json:"templateId,omitempty"`
	TraceParent       *string    `json:"-"`
	CorrelationID     *string    `json:"correlationId,omitempty"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	NextAttemptAt     *time.Time `json:"nextAttemptAt,omitempty"`
	SentAt            *time.Time `json:"sentAt,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
}

// CreateRequest is the input for creating one notification.
type CreateRequest struct {
	Recipient      string     `json:"recipient"`
	Channel        Channel    `json:"channel"`
	Content        string     `json:"content,omitempty"`
	Priority       Priority   `json:"priority,omitempty"`
	IdempotencyKey string     `json:"idempotencyKey,omitempty"`
	ScheduledAt    *time.Time `json:"scheduledAt,omitempty"`

	// TemplateID renders Content from a stored template using Variables.
	// Content must be empty when TemplateID is set.
	TemplateID *uuid.UUID        `json:"templateId,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

// Content limits per channel (in characters).
var MaxContentLength = map[Channel]int{
	ChannelSMS:   160,
	ChannelPush:  256,
	ChannelEmail: 10000,
}

const (
	MaxBatchSize         = 1000
	maxIdempotencyKeyLen = 255
)

var (
	e164Re  = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)
	emailRe = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
)

// ValidationError collects field-level validation problems.
type ValidationError struct {
	Fields map[string]string `json:"fields"`
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for k, v := range e.Fields {
		parts = append(parts, k+": "+v)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

var (
	ErrNotFound       = errors.New("notification not found")
	ErrNotCancellable = errors.New("notification can no longer be cancelled")
)

// Normalize applies defaults and trims whitespace.
func (r *CreateRequest) Normalize() {
	r.Recipient = strings.TrimSpace(r.Recipient)
	r.Channel = Channel(strings.ToLower(strings.TrimSpace(string(r.Channel))))
	r.Priority = Priority(strings.ToLower(strings.TrimSpace(string(r.Priority))))
	if r.Priority == "" {
		r.Priority = PriorityNormal
	}
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
}

// Validate checks required fields, channel-specific recipient format and content length.
func (r *CreateRequest) Validate(now time.Time) error {
	f := map[string]string{}

	if !r.Channel.Valid() {
		f["channel"] = "must be one of sms, email, push"
	}
	if !r.Priority.Valid() {
		f["priority"] = "must be one of high, normal, low"
	}

	switch {
	case r.Recipient == "":
		f["recipient"] = "is required"
	case r.Channel == ChannelSMS && !e164Re.MatchString(r.Recipient):
		f["recipient"] = "must be an E.164 phone number, e.g. +905551234567"
	case r.Channel == ChannelEmail && !emailRe.MatchString(r.Recipient):
		f["recipient"] = "must be a valid email address"
	case len(r.Recipient) > 512:
		f["recipient"] = "is too long"
	}

	n := utf8.RuneCountInString(r.Content)
	switch {
	case strings.TrimSpace(r.Content) == "":
		f["content"] = "is required"
	case r.Channel.Valid() && n > MaxContentLength[r.Channel]:
		f["content"] = fmt.Sprintf("exceeds %d characters for %s (got %d)", MaxContentLength[r.Channel], r.Channel, n)
	}

	if len(r.IdempotencyKey) > maxIdempotencyKeyLen {
		f["idempotencyKey"] = "must be at most 255 characters"
	}
	if r.ScheduledAt != nil && r.ScheduledAt.Before(now.Add(-time.Minute)) {
		f["scheduledAt"] = "must be in the future"
	}

	if len(f) > 0 {
		return &ValidationError{Fields: f}
	}
	return nil
}

// ListFilter holds the query parameters for listing notifications.
type ListFilter struct {
	Status  *Status
	Channel *Channel
	BatchID *uuid.UUID
	From    *time.Time
	To      *time.Time
	Limit   int
	Offset  int
}

// Page is a paginated list result.
type Page struct {
	Items  []Notification `json:"items"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}
