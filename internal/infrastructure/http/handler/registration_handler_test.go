package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/handler"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
	"github.com/go-chi/chi/v5"
)

func TestRegistrationHandler_LinkAndUnlink(t *testing.T) {
	repo := memory.NewMemoryDeviceRepository()
	svc := service.NewDeviceApplicationService(repo)
	h := handler.NewRegistrationHandler(svc)

	ctx := withTenant(context.Background(), "tenant-1")

	dev1, _ := model.NewOBDDevice("dev-1", "tenant-1", "branch-1", "00:1B:44:11:3A:01")
	_ = repo.SaveDevice(ctx, dev1)

	r := chi.NewRouter()
	r.Post("/api/v1/obd2-device-registrations", h.LinkDevice)
	r.Patch("/api/v1/obd2-device-registrations/{id}", h.UpdateStatus)
	r.Get("/api/v1/obd2-device-registrations", h.ListRegistrations)

	var regID string

	t.Run("link available device returns 201 Created", func(t *testing.T) {
		body := bytes.NewBufferString(`{"obd2DeviceId":"dev-1","branchId":"branch-1","vehicleId":"veh-1"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-device-registrations", body).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d (body: %s)", rr.Code, rr.Body.String())
		}

		var resp handler.RegistrationResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Status != model.RegistrationStatusActive {
			t.Errorf("expected ACTIVE status, got %s", resp.Status)
		}
		regID = resp.ID
	})

	t.Run("linking already linked device returns 400 or 409", func(t *testing.T) {
		body := bytes.NewBufferString(`{"obd2DeviceId":"dev-1","branchId":"branch-1","vehicleId":"veh-2"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-device-registrations", body).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusConflict {
			t.Errorf("expected 400 or 409 for already linked device, got %d", rr.Code)
		}
	})

	t.Run("unlink device via PATCH returns 200 OK", func(t *testing.T) {
		body := bytes.NewBufferString(`{"status":"INACTIVE"}`)
		req := httptest.NewRequest("PATCH", "/api/v1/obd2-device-registrations/"+regID, body).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (body: %s)", rr.Code, rr.Body.String())
		}

		var resp handler.RegistrationResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Status != model.RegistrationStatusInactive {
			t.Errorf("expected INACTIVE, got %s", resp.Status)
		}
	})

	t.Run("list registrations", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/obd2-device-registrations?branchId=branch-1", nil).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rr.Code)
		}

		var list []handler.RegistrationResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &list)
		if len(list) != 1 {
			t.Errorf("expected 1 registration, got %d", len(list))
		}
	})
}
