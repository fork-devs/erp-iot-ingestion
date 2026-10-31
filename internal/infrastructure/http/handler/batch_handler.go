package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
)

// BatchIngestResponse carries acknowledgment of batch receipt.
type BatchIngestResponse struct {
	Message string `json:"message"`
}

// BatchHandler processes telemetry batches.
type BatchHandler struct {
	useCase     usecase.BatchIngestUseCase
	publisher   port.EventPublisherPort
	maxBodySize int64
}

// NewBatchHandler creates a new BatchHandler.
func NewBatchHandler(useCase usecase.BatchIngestUseCase, publisher port.EventPublisherPort, maxBodySize int64) *BatchHandler {
	if maxBodySize <= 0 {
		maxBodySize = 2097152 // 2MB default for batches
	}
	return &BatchHandler{
		useCase:     useCase,
		publisher:   publisher,
		maxBodySize: maxBodySize,
	}
}

// HandleIngestBatch handles POST /api/v1/telemetry-batches.
func (h *BatchHandler) HandleIngestBatch(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "UNAUTHORIZED", "Tenant context missing", r.URL.Path)
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBodySize))
	if err != nil {
		WriteProblem(w, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "Request body exceeds maximum size limit", r.URL.Path)
		return
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Request body cannot be empty", r.URL.Path)
		return
	}

	var batch model.TelemetryBatch
	if err := json.Unmarshal(trimmed, &batch); err != nil {
		if h.publisher != nil {
			_ = h.publisher.PublishDLQ(r.Context(), raw, "batch unmarshal error: "+err.Error())
		}
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid JSON syntax: "+err.Error(), r.URL.Path)
		return
	}

	batch.TenantID = tenantID

	if err := h.useCase.IngestBatch(r.Context(), batch); err != nil {
		if errors.Is(err, domain.ErrInvalidOdometer) || errors.Is(err, domain.ErrInvalidTelemetry) {
			WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), r.URL.Path)
			return
		}
		if errors.Is(err, domain.ErrDeviceNotAvailable) || errors.Is(err, domain.ErrDeviceNotFound) {
			WriteProblem(w, http.StatusForbidden, "ACCESS_DENIED", err.Error(), r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusCreated, BatchIngestResponse{
		Message: "Telemetry batch ingested successfully",
	})
}
