package template

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/httpx"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/logger"
)

// Store is the persistence contract for templates.
type Store interface {
	Create(ctx context.Context, t Template) (Template, error)
	Get(ctx context.Context, id uuid.UUID) (Template, error)
	List(ctx context.Context) ([]Template, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// Handler exposes the template REST API.
type Handler struct {
	store Store
	log   *slog.Logger
}

func NewHandler(store Store, log *slog.Logger) *Handler { return &Handler{store: store, log: log} }

func (h *Handler) Routes(r chi.Router) {
	r.Post("/templates", h.create)
	r.Get("/templates", h.list)
	r.Get("/templates/{id}", h.get)
	r.Delete("/templates/{id}", h.delete)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid JSON body", err.Error())
		return
	}
	req.Normalize()
	if err := req.Validate(); err != nil {
		h.fail(w, r, err)
		return
	}
	t, err := h.store.Create(r.Context(), Template{
		ID: uuid.New(), Name: req.Name, Channel: req.Channel, Body: req.Body, Variables: Variables(req.Body),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, t)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	ts, err := h.store.List(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": ts})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "id must be a UUID", nil)
		return
	}
	t, err := h.store.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.JSON(w, http.StatusOK, t)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "id must be a UUID", nil)
		return
	}
	if err := h.store.Delete(r.Context(), id); err != nil {
		h.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	var ve *ValidationError
	switch {
	case errors.As(err, &ve):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation failed", ve.Fields)
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, err.Error(), nil)
	case errors.Is(err, ErrDuplicateName):
		httpx.Error(w, http.StatusConflict, err.Error(), nil)
	default:
		logger.FromContext(r.Context(), h.log).Error("template request failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error", nil)
	}
}
