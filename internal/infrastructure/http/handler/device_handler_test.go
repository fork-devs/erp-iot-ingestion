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
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
	"github.com/go-chi/chi/v5"
)

func withTenant(ctx context.Context, tenantID string) context.Context {
	authenticator := middleware.NewDeviceAuthenticator(map[string]string{
		"test-key": tenantID,
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Device-Key", "test-key")
	var capturedCtx context.Context
	authenticator.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCtx = r.Context()
	})).ServeHTTP(httptest.NewRecorder(), req)
	return capturedCtx
}

func TestDeviceHandler_RegisterDevice(t *testing.T) {
	repo := memory.NewMemoryDeviceRepository()
	svc := service.NewDeviceApplicationService(repo)
	h := handler.NewDeviceHandler(svc)

	t.Run("unauthorized without tenant", func(t *testing.T) {
		body := bytes.NewBufferString(`{"branchId":"b1","macAddress":"00:1B:44:11:3A:B7"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-devices", body)
		rr := httptest.NewRecorder()

		h.RegisterDevice(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}
	})

	t.Run("valid registration returns 201 Created", func(t *testing.T) {
		body := bytes.NewBufferString(`{"branchId":"b1","macAddress":"00:1B:44:11:3A:B7"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-devices", body)
		req = req.WithContext(withTenant(req.Context(), "tenant-1"))
		rr := httptest.NewRecorder()

		h.RegisterDevice(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d (body: %s)", rr.Code, rr.Body.String())
		}

		var resp handler.DeviceResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.MACAddress != "00:1B:44:11:3A:B7" {
			t.Errorf("expected MAC 00:1B:44:11:3A:B7, got %s", resp.MACAddress)
		}
		if resp.Status != model.DeviceStatusAvailable {
			t.Errorf("expected status AVAILABLE, got %s", resp.Status)
		}
	})

	t.Run("invalid MAC returns 400 Bad Request", func(t *testing.T) {
		body := bytes.NewBufferString(`{"branchId":"b1","macAddress":"invalid-mac"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-devices", body)
		req = req.WithContext(withTenant(req.Context(), "tenant-1"))
		rr := httptest.NewRecorder()

		h.RegisterDevice(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request, got %d", rr.Code)
		}
	})

	t.Run("duplicate MAC returns 409 Conflict", func(t *testing.T) {
		body := bytes.NewBufferString(`{"branchId":"b1","macAddress":"00:1B:44:11:3A:B7"}`)
		req := httptest.NewRequest("POST", "/api/v1/obd2-devices", body)
		req = req.WithContext(withTenant(req.Context(), "tenant-1"))
		rr := httptest.NewRecorder()

		h.RegisterDevice(rr, req)
		if rr.Code != http.StatusConflict {
			t.Errorf("expected 409 Conflict on duplicate MAC, got %d", rr.Code)
		}
	})
}

func TestDeviceHandler_ListAndGet(t *testing.T) {
	repo := memory.NewMemoryDeviceRepository()
	svc := service.NewDeviceApplicationService(repo)
	h := handler.NewDeviceHandler(svc)

	ctx := withTenant(context.Background(), "tenant-1")
	dev, _ := model.NewOBDDevice("dev-1", "tenant-1", "b1", "00:1B:44:11:3A:01")
	_ = repo.SaveDevice(ctx, dev)

	r := chi.NewRouter()
	r.Get("/api/v1/obd2-devices", h.ListDevices)
	r.Get("/api/v1/obd2-devices/{id}", h.GetDevice)

	t.Run("list devices in branch", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/obd2-devices?branchId=b1", nil).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rr.Code)
		}

		var list []handler.DeviceResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &list)
		if len(list) != 1 {
			t.Errorf("expected 1 device, got %d", len(list))
		}
	})

	t.Run("get existing device detail", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/obd2-devices/dev-1", nil).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rr.Code)
		}
	})

	t.Run("get non-existent device returns 404", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/obd2-devices/unknown", nil).WithContext(ctx)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected 404 NOT_FOUND, got %d", rr.Code)
		}
	})
}
