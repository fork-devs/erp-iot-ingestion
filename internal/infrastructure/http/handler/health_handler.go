package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
)

// HealthHandler manages liveness and readiness probe endpoints.
type HealthHandler struct {
	publisher port.EventPublisherPort
}

// NewHealthHandler creates a HealthHandler instance.
func NewHealthHandler(publisher port.EventPublisherPort) *HealthHandler {
	return &HealthHandler{
		publisher: publisher,
	}
}

// Liveness handles GET /healthz.
func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{
		"status": "UP",
	})
}

// Readiness handles GET /readyz by checking broker connectivity.
func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.publisher.Ping(ctx); err != nil {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "DOWN",
			"error":  err.Error(),
		})
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{
		"status": "UP",
		"kafka":  "UP",
	})
}
