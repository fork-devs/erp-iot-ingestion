package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/go-chi/chi/v5"
)

// CreateDeviceRequest represents payload to register an OBD2 physical scanner.
type CreateDeviceRequest struct {
	BranchID   string `json:"branchId"`
	MACAddress string `json:"macAddress"`
}

// DeviceResponse represents the public JSON representation of an OBD2 device.
type DeviceResponse struct {
	ID         string             `json:"id"`
	BranchID   string             `json:"branchId"`
	MACAddress string             `json:"macAddress"`
	Status     model.DeviceStatus `json:"status"`
}

// DeviceHandler handles HTTP requests for OBD2 physical devices.
type DeviceHandler struct {
	useCase usecase.DeviceUseCase
}

// NewDeviceHandler creates a new DeviceHandler.
func NewDeviceHandler(useCase usecase.DeviceUseCase) *DeviceHandler {
	return &DeviceHandler{
		useCase: useCase,
	}
}

// RegisterDevice handles POST /api/v1/obd2-devices.
func (h *DeviceHandler) RegisterDevice(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	var req CreateDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request payload format", r.URL.Path)
		return
	}

	if strings.TrimSpace(req.BranchID) == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "branchId is required", r.URL.Path)
		return
	}
	if strings.TrimSpace(req.MACAddress) == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "macAddress is required", r.URL.Path)
		return
	}

	dev, err := h.useCase.RegisterDevice(r.Context(), usecase.RegisterDeviceCommand{
		TenantID:   tenantID,
		BranchID:   req.BranchID,
		MACAddress: req.MACAddress,
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidMACAddress) {
			WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), r.URL.Path)
			return
		}
		if errors.Is(err, domain.ErrDeviceConflict) {
			WriteProblem(w, http.StatusConflict, "CONFLICT", err.Error(), r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusCreated, DeviceResponse{
		ID:         dev.ID,
		BranchID:   dev.BranchID,
		MACAddress: dev.MACAddress,
		Status:     dev.Status,
	})
}

// ListDevices handles GET /api/v1/obd2-devices?branchId={branchId}&status={status}.
func (h *DeviceHandler) ListDevices(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	branchID := r.URL.Query().Get("branchId")
	statusQuery := strings.ToLower(r.URL.Query().Get("status"))

	var status model.DeviceStatus
	if statusQuery == "available" {
		status = model.DeviceStatusAvailable
	} else if statusQuery == "linked" {
		status = model.DeviceStatusLinked
	}

	devices, err := h.useCase.ListDevices(r.Context(), tenantID, branchID, status)
	if err != nil {
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	resp := make([]DeviceResponse, 0, len(devices))
	for _, dev := range devices {
		resp = append(resp, DeviceResponse{
			ID:         dev.ID,
			BranchID:   dev.BranchID,
			MACAddress: dev.MACAddress,
			Status:     dev.Status,
		})
	}

	WriteJSON(w, http.StatusOK, resp)
}

// GetDevice handles GET /api/v1/obd2-devices/{id}.
func (h *DeviceHandler) GetDevice(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	deviceID := chi.URLParam(r, "id")
	if deviceID == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required in URL", r.URL.Path)
		return
	}

	dev, err := h.useCase.GetDevice(r.Context(), tenantID, deviceID)
	if err != nil {
		if errors.Is(err, domain.ErrDeviceNotFound) {
			WriteProblem(w, http.StatusNotFound, "NOT_FOUND", "Device not found", r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusOK, DeviceResponse{
		ID:         dev.ID,
		BranchID:   dev.BranchID,
		MACAddress: dev.MACAddress,
		Status:     dev.Status,
	})
}
