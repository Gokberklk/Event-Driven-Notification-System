package notification

import (
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	tests := []struct {
		name    string
		req     CreateRequest
		wantErr []string
	}{
		{"valid sms", CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: "hi"}, nil},
		{"valid email", CreateRequest{Recipient: "a@b.com", Channel: "email", Content: "hi"}, nil},
		{"valid push", CreateRequest{Recipient: "device-token", Channel: "push", Content: "hi", Priority: "high"}, nil},
		{"uppercase channel normalised", CreateRequest{Recipient: "a@b.com", Channel: "EMAIL", Content: "hi"}, nil},
		{"missing fields", CreateRequest{}, []string{"recipient", "channel", "content"}},
		{"bad phone", CreateRequest{Recipient: "05551234567", Channel: "sms", Content: "hi"}, []string{"recipient"}},
		{"bad email", CreateRequest{Recipient: "nope", Channel: "email", Content: "hi"}, []string{"recipient"}},
		{"bad priority", CreateRequest{Recipient: "a@b.com", Channel: "email", Content: "hi", Priority: "urgent"}, []string{"priority"}},
		{"sms too long", CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("a", 161)}, []string{"content"}},
		{"sms limit counts runes", CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("ş", 160)}, nil},
		{"push too long", CreateRequest{Recipient: "t", Channel: "push", Content: strings.Repeat("a", 257)}, []string{"content"}},
		{"whitespace content", CreateRequest{Recipient: "t", Channel: "push", Content: "   "}, []string{"content"}},
		{"scheduled in past", CreateRequest{Recipient: "t", Channel: "push", Content: "x", ScheduledAt: &past}, []string{"scheduledAt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.req.Normalize()
			err := tt.req.Validate(now)
			if len(tt.wantErr) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			ve, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("want ValidationError, got %v", err)
			}
			for _, f := range tt.wantErr {
				if _, ok := ve.Fields[f]; !ok {
					t.Errorf("expected error on %q, got %v", f, ve.Fields)
				}
			}
		})
	}
}

func TestNormalizeDefaultsPriority(t *testing.T) {
	r := CreateRequest{Channel: " SMS "}
	r.Normalize()
	if r.Priority != PriorityNormal || r.Channel != ChannelSMS {
		t.Fatalf("got priority=%q channel=%q", r.Priority, r.Channel)
	}
}

func TestPriorityRank(t *testing.T) {
	if !(PriorityHigh.Rank() < PriorityNormal.Rank() && PriorityNormal.Rank() < PriorityLow.Rank()) {
		t.Fatal("priority ranks must order high < normal < low")
	}
}

func TestParseListFilter(t *testing.T) {
	f, err := ParseListFilter(map[string][]string{
		"status": {"sent"}, "channel": {"SMS"}, "limit": {"50"}, "offset": {"10"},
		"from": {"2026-01-01T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *f.Status != StatusSent || *f.Channel != ChannelSMS || f.Limit != 50 || f.Offset != 10 || f.From == nil {
		t.Fatalf("unexpected filter %+v", f)
	}

	_, err = ParseListFilter(map[string][]string{"limit": {"500"}, "status": {"x"}, "to": {"yesterday"}})
	ve, ok := err.(*ValidationError)
	if !ok || len(ve.Fields) != 3 {
		t.Fatalf("expected 3 field errors, got %v", err)
	}
}
