package notification

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/notification-system/notification-system/internal/httpx"
	"github.com/notification-system/notification-system/internal/logger"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
	maxBodyBytes    = 10 << 20 // 10 MiB, enough for a 1000-item batch
)

// Handler exposes the notification management REST API.
type Handler struct {
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) *Handler { return &Handler{svc: svc, log: log} }

// Routes mounts the API under the caller's router.
func (h *Handler) Routes(r chi.Router) {
	r.Post("/notifications", h.create)
	r.Post("/notifications/batch", h.createBatch)
	r.Get("/notifications", h.list)
	r.Get("/notifications/{id}", h.get)
	r.Post("/notifications/{id}/cancel", h.cancel)
	r.Get("/batches/{id}", h.getBatch)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if !decode(w, r, &req) {
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" && req.IdempotencyKey == "" {
		req.IdempotencyKey = k
	}
	n, created, err := h.svc.Create(r.Context(), req)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	httpx.JSON(w, status, n)
}

type batchRequest struct {
	Notifications []CreateRequest `json:"notifications"`
}

func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := h.svc.CreateBatch(r.Context(), req.Notifications)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, res)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, n)
}

func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	b, err := h.svc.GetBatch(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, b)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, n)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	f, err := ParseListFilter(r.URL.Query())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	page, err := h.svc.List(r.Context(), f)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, page)
}

// ParseListFilter converts query parameters into a ListFilter.
func ParseListFilter(q map[string][]string) (ListFilter, error) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return strings.TrimSpace(v[0])
		}
		return ""
	}
	f := ListFilter{Limit: defaultPageSize}
	errs := map[string]string{}

	if v := get("status"); v != "" {
		s := Status(strings.ToLower(v))
		if !s.Valid() {
			errs["status"] = "invalid status"
		}
		f.Status = &s
	}
	if v := get("channel"); v != "" {
		c := Channel(strings.ToLower(v))
		if !c.Valid() {
			errs["channel"] = "must be one of sms, email, push"
		}
		f.Channel = &c
	}
	if v := get("batchId"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			errs["batchId"] = "must be a UUID"
		}
		f.BatchID = &id
	}
	for _, k := range []string{"from", "to"} {
		v := get(k)
		if v == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			errs[k] = "must be an RFC3339 timestamp"
			continue
		}
		if k == "from" {
			f.From = &t
		} else {
			f.To = &t
		}
	}
	if v := get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			errs["limit"] = "must be between 1 and 100"
		}
		f.Limit = n
	}
	if v := get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs["offset"] = "must be a non-negative integer"
		}
		f.Offset = n
	}
	if len(errs) > 0 {
		return f, &ValidationError{Fields: errs}
	}
	return f, nil
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	var ve *ValidationError
	switch {
	case errors.As(err, &ve):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation failed", ve.Fields)
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, err.Error(), nil)
	case errors.Is(err, ErrNotCancellable):
		httpx.Error(w, http.StatusConflict, err.Error(), nil)
	default:
		logger.FromContext(r.Context(), h.log).Error("request failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error", nil)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid JSON body", err.Error())
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "id must be a UUID", nil)
		return id, false
	}
	return id, true
}
