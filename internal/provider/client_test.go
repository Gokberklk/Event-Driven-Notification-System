package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   bool
		retryable bool
		wantID    string
	}{
		{"accepted", 202, `{"messageId":"m-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`, false, false, "m-1"},
		{"accepted without body", 202, ``, false, false, ""},
		{"server error retries", 503, `down`, true, true, ""},
		{"rate limited retries", 429, ``, true, true, ""},
		{"bad request is permanent", 400, `invalid`, true, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req Request
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To != "+905551234567" || req.Channel != "sms" {
					t.Errorf("unexpected request body %+v %v", req, err)
				}
				if r.Header.Get("Idempotency-Key") != "id-1" || r.Header.Get("X-Correlation-ID") != "cid" {
					t.Errorf("missing headers: %v", r.Header)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			resp, err := NewClient(srv.URL, time.Second).Send(context.Background(),
				Request{To: "+905551234567", Channel: "sms", Content: "hi"}, "id-1", "cid")
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v", err)
			}
			if err != nil && IsRetryable(err) != tt.retryable {
				t.Fatalf("retryable = %v, want %v", IsRetryable(err), tt.retryable)
			}
			if resp.MessageID != tt.wantID {
				t.Fatalf("messageId = %q", resp.MessageID)
			}
		})
	}
}

func TestSendTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL, 50*time.Millisecond).Send(context.Background(), Request{}, "k", "")
	if err == nil || !IsRetryable(err) {
		t.Fatalf("timeout should be retryable, got %v", err)
	}
}
