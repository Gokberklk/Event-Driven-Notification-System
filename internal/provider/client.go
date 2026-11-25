// Package provider is the HTTP client for the external notification provider
// (simulated with webhook.site).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type Response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Error describes a failed delivery attempt. Retryable is true for transient
// failures (network errors, timeouts, 429 and 5xx responses).
type Error struct {
	StatusCode int
	Retryable  bool
	Msg        string
}

func (e *Error) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("provider returned %d: %s", e.StatusCode, e.Msg)
	}
	return "provider request failed: " + e.Msg
}

// IsRetryable reports whether err is a transient provider failure.
func IsRetryable(err error) bool {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Retryable
	}
	return true
}

type Client struct {
	url  string
	http *http.Client
}

func NewClient(url string, timeout time.Duration) *Client {
	// otelhttp creates a client span per call and injects the traceparent
	// header, so a tracing-aware provider could join the same trace.
	return &Client{url: url, http: &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)}}
}

// Send posts one message. idempotencyKey lets the provider de-duplicate
// retries of the same notification.
func (c *Client) Send(ctx context.Context, req Request, idempotencyKey, correlationID string) (Response, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Response{}, &Error{Msg: err.Error()}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	if correlationID != "" {
		httpReq.Header.Set("X-Correlation-ID", correlationID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Response{}, &Error{Retryable: true, Msg: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK:
		var out Response
		if err := json.Unmarshal(raw, &out); err != nil || out.MessageID == "" {
			// The message was accepted; a malformed body should not cause a resend.
			return Response{Status: "accepted"}, nil
		}
		return out, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return Response{}, &Error{StatusCode: resp.StatusCode, Retryable: true, Msg: truncate(raw)}
	default:
		return Response{}, &Error{StatusCode: resp.StatusCode, Retryable: false, Msg: truncate(raw)}
	}
}

func truncate(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}
