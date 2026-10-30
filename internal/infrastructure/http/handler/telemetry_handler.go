package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
)

// IngestRequest represents an incoming telemetry reading payload.
type IngestRequest struct {
	VehicleID string            `json:"vehicle_id"`
	DeviceID  string            `json:"device_id"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
	Location  model.GPSLocation `json:"location"`
	OBD       model.OBDData     `json:"obd"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// IngestResponse represents the acknowledgment payload returned to clients.
type IngestResponse struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Message  string `json:"message"`
}

// TelemetryHandler handles incoming HTTP requests for telemetry ingestion.
type TelemetryHandler struct {
	useCase     usecase.IngestionUseCase
	publisher   port.EventPublisherPort
	maxBodySize int64
}

// NewTelemetryHandler creates a new TelemetryHandler.
func NewTelemetryHandler(useCase usecase.IngestionUseCase, publisher port.EventPublisherPort, maxBodySize int64) *TelemetryHandler {
	if maxBodySize <= 0 {
		maxBodySize = 1048576 // 1MB default
	}
	return &TelemetryHandler{
		useCase:     useCase,
		publisher:   publisher,
		maxBodySize: maxBodySize,
	}
}

// HandleIngest processes incoming single or batch telemetry payloads at POST /api/v1/iot/telemetry.
func (h *TelemetryHandler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBodySize))
	if err != nil {
		WriteProblem(w, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "Request body exceeds maximum size limit", r.URL.Path)
		return
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		WriteProblem(w, http.StatusBadRequest, "InvalidPayload", "Request body cannot be empty", r.URL.Path)
		return
	}

	if trimmed[0] == '[' {
		h.handleBatch(w, r, tenantID, trimmed)
	} else if trimmed[0] == '{' {
		h.handleSingle(w, r, tenantID, trimmed)
	} else {
		// Malformed syntax: route to DLQ for auditing
		_ = h.publisher.PublishDLQ(r.Context(), raw, "invalid JSON payload: does not start with { or [")
		WriteProblem(w, http.StatusBadRequest, "MalformedPayload", "Payload must be a JSON object or array", r.URL.Path)
	}
}

func (h *TelemetryHandler) handleSingle(w http.ResponseWriter, r *http.Request, tenantID string, data []byte) {
	var req IngestRequest
	if err := json.Unmarshal(data, &req); err != nil {
		_ = h.publisher.PublishDLQ(r.Context(), data, "json unmarshal error: "+err.Error())
		WriteProblem(w, http.StatusBadRequest, "MalformedJSON", err.Error(), r.URL.Path)
		return
	}

	deviceID := req.DeviceID
	if deviceID == "" {
		if d, ok := middleware.DeviceFromContext(r.Context()); ok {
			deviceID = d
		}
	}

	ts := time.Now().UTC()
	if req.Timestamp != nil && !req.Timestamp.IsZero() {
		ts = req.Timestamp.UTC()
	}

	cmd := usecase.IngestTelemetryCommand{
		TenantID:  tenantID,
		VehicleID: req.VehicleID,
		DeviceID:  deviceID,
		Timestamp: ts,
		Location:  req.Location,
		OBD:       req.OBD,
		Metadata:  req.Metadata,
	}

	_, err := h.useCase.Ingest(r.Context(), cmd)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTelemetry) {
			_ = h.publisher.PublishDLQ(r.Context(), data, "validation error: "+err.Error())
			WriteProblem(w, http.StatusBadRequest, "ValidationError", err.Error(), r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", "Failed to enqueue telemetry", r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusAccepted, IngestResponse{
		Status:   "accepted",
		Accepted: 1,
		Message:  "telemetry enqueued for processing",
	})
}

func (h *TelemetryHandler) handleBatch(w http.ResponseWriter, r *http.Request, tenantID string, data []byte) {
	var reqs []IngestRequest
	if err := json.Unmarshal(data, &reqs); err != nil {
		_ = h.publisher.PublishDLQ(r.Context(), data, "batch json unmarshal error: "+err.Error())
		WriteProblem(w, http.StatusBadRequest, "MalformedJSON", err.Error(), r.URL.Path)
		return
	}

	if len(reqs) == 0 {
		WriteProblem(w, http.StatusBadRequest, "InvalidPayload", "Batch reading array cannot be empty", r.URL.Path)
		return
	}

	authDevice, _ := middleware.DeviceFromContext(r.Context())

	commands := make([]usecase.IngestTelemetryCommand, len(reqs))
	for i, req := range reqs {
		dev := req.DeviceID
		if dev == "" {
			dev = authDevice
		}

		ts := time.Now().UTC()
		if req.Timestamp != nil && !req.Timestamp.IsZero() {
			ts = req.Timestamp.UTC()
		}

		commands[i] = usecase.IngestTelemetryCommand{
			TenantID:  tenantID,
			VehicleID: req.VehicleID,
			DeviceID:  dev,
			Timestamp: ts,
			Location:  req.Location,
			OBD:       req.OBD,
			Metadata:  req.Metadata,
		}
	}

	_, err := h.useCase.IngestBatch(r.Context(), usecase.IngestBatchCommand{
		TenantID: tenantID,
		Readings: commands,
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTelemetry) || strings.Contains(err.Error(), "reading at index") {
			_ = h.publisher.PublishDLQ(r.Context(), data, "batch validation error: "+err.Error())
			WriteProblem(w, http.StatusBadRequest, "ValidationError", err.Error(), r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", "Failed to enqueue batch telemetry", r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusAccepted, IngestResponse{
		Status:   "accepted",
		Accepted: len(reqs),
		Message:  "batch telemetry enqueued for processing",
	})
}
