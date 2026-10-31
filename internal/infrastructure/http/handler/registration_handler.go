package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/go-chi/chi/v5"
)

// LinkDeviceRequest carries payload to bind an OBD2 device to a vehicle.
type LinkDeviceRequest struct {
	OBD2DeviceID string `json:"obd2DeviceId"`
	BranchID     string `json:"branchId"`
	VehicleID    string `json:"vehicleId"`
}

// UpdateRegistrationStatusRequest carries payload to change registration status (e.g. INACTIVE).
type UpdateRegistrationStatusRequest struct {
	Status string `json:"status"`
}

// RegistrationResponse represents the public JSON response of a device-to-vehicle registration.
type RegistrationResponse struct {
	ID           string                   `json:"id"`
	OBD2DeviceID string                   `json:"obd2DeviceId"`
	BranchID     string                   `json:"branchId"`
	VehicleID    string                   `json:"vehicleId"`
	Status       model.RegistrationStatus `json:"status"`
	CreatedAt    time.Time                `json:"createdAt"`
}

// RegistrationHandler handles HTTP endpoints for device-to-vehicle bindings.
type RegistrationHandler struct {
	useCase usecase.RegistrationUseCase
}

// NewRegistrationHandler creates a new RegistrationHandler.
func NewRegistrationHandler(useCase usecase.RegistrationUseCase) *RegistrationHandler {
	return &RegistrationHandler{
		useCase: useCase,
	}
}

// LinkDevice handles POST /api/v1/obd2-device-registrations.
func (h *RegistrationHandler) LinkDevice(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	var req LinkDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body format", r.URL.Path)
		return
	}

	if strings.TrimSpace(req.OBD2DeviceID) == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "obd2DeviceId is required", r.URL.Path)
		return
	}
	if strings.TrimSpace(req.BranchID) == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "branchId is required", r.URL.Path)
		return
	}
	if strings.TrimSpace(req.VehicleID) == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "vehicleId is required", r.URL.Path)
		return
	}

	reg, err := h.useCase.LinkDevice(r.Context(), usecase.LinkDeviceCommand{
		TenantID:     tenantID,
		BranchID:     req.BranchID,
		OBD2DeviceID: req.OBD2DeviceID,
		VehicleID:    req.VehicleID,
	})
	if err != nil {
		if errors.Is(err, domain.ErrDeviceNotFound) {
			WriteProblem(w, http.StatusNotFound, "NOT_FOUND", err.Error(), r.URL.Path)
			return
		}
		if errors.Is(err, domain.ErrDeviceNotAvailable) {
			WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), r.URL.Path)
			return
		}
		if errors.Is(err, domain.ErrDeviceAlreadyLinked) || errors.Is(err, domain.ErrVehicleAlreadyLinked) {
			WriteProblem(w, http.StatusConflict, "CONFLICT", err.Error(), r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusCreated, RegistrationResponse{
		ID:           reg.ID,
		OBD2DeviceID: reg.OBD2DeviceID,
		BranchID:     reg.BranchID,
		VehicleID:    reg.VehicleID,
		Status:       reg.Status,
		CreatedAt:    reg.CreatedAt,
	})
}

// UpdateStatus handles PATCH /api/v1/obd2-device-registrations/{id}.
func (h *RegistrationHandler) UpdateStatus(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	regID := chi.URLParam(r, "id")
	if regID == "" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Registration ID required in URL", r.URL.Path)
		return
	}

	var req UpdateRegistrationStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body format", r.URL.Path)
		return
	}

	statusUpper := strings.ToUpper(strings.TrimSpace(req.Status))
	if statusUpper != "ACTIVE" && statusUpper != "INACTIVE" {
		WriteProblem(w, http.StatusBadRequest, "VALIDATION_ERROR", "status must be ACTIVE or INACTIVE", r.URL.Path)
		return
	}

	reg, err := h.useCase.UpdateRegistrationStatus(r.Context(), usecase.UpdateRegistrationStatusCommand{
		TenantID:       tenantID,
		RegistrationID: regID,
		Status:         model.RegistrationStatus(statusUpper),
	})
	if err != nil {
		if errors.Is(err, domain.ErrRegistrationNotFound) {
			WriteProblem(w, http.StatusNotFound, "NOT_FOUND", "Registration not found", r.URL.Path)
			return
		}
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	WriteJSON(w, http.StatusOK, RegistrationResponse{
		ID:           reg.ID,
		OBD2DeviceID: reg.OBD2DeviceID,
		BranchID:     reg.BranchID,
		VehicleID:    reg.VehicleID,
		Status:       reg.Status,
		CreatedAt:    reg.CreatedAt,
	})
}

// ListRegistrations handles GET /api/v1/obd2-device-registrations?branchId={branchId}&status={status}.
func (h *RegistrationHandler) ListRegistrations(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantFromContext(r.Context())
	if !ok || tenantID == "" {
		WriteProblem(w, http.StatusUnauthorized, "Unauthorized", "Tenant context missing", r.URL.Path)
		return
	}

	branchID := r.URL.Query().Get("branchId")
	statusQuery := strings.ToUpper(r.URL.Query().Get("status"))

	var status model.RegistrationStatus
	if statusQuery == "ACTIVE" {
		status = model.RegistrationStatusActive
	} else if statusQuery == "INACTIVE" {
		status = model.RegistrationStatusInactive
	}

	registrations, err := h.useCase.ListRegistrations(r.Context(), tenantID, branchID, status)
	if err != nil {
		WriteProblem(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
		return
	}

	resp := make([]RegistrationResponse, 0, len(registrations))
	for _, reg := range registrations {
		resp = append(resp, RegistrationResponse{
			ID:           reg.ID,
			OBD2DeviceID: reg.OBD2DeviceID,
			BranchID:     reg.BranchID,
			VehicleID:    reg.VehicleID,
			Status:       reg.Status,
			CreatedAt:    reg.CreatedAt,
		})
	}

	WriteJSON(w, http.StatusOK, resp)
}
