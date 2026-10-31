package handler_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/handler"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
)

type mockKafkaPublisher struct {
	dlqPayloads [][]byte
	batches     [][]*model.TelemetryPacket
}

func (m *mockKafkaPublisher) PublishTelemetry(ctx context.Context, packet *model.TelemetryPacket) error {
	return nil
}

func (m *mockKafkaPublisher) PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error {
	m.batches = append(m.batches, packets)
	return nil
}

func (m *mockKafkaPublisher) PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error {
	m.dlqPayloads = append(m.dlqPayloads, rawPayload)
	return nil
}

func (m *mockKafkaPublisher) Ping(ctx context.Context) error {
	return nil
}

func (m *mockKafkaPublisher) Close() error {
	return nil
}

func TestBatchHandler_HandleIngestBatch(t *testing.T) {
	repo := memory.NewMemoryDeviceRepository()
	pub := &mockKafkaPublisher{}
	deviceSvc := service.NewDeviceApplicationService(repo)
	batchSvc := service.NewBatchApplicationService(repo, pub)
	h := handler.NewBatchHandler(batchSvc, pub, 1048576)

	ctx := withTenant(context.Background(), "tenant-1")

	// Register and link dev-1
	dev, _ := deviceSvc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
		TenantID:   "tenant-1",
		BranchID:   "branch-1",
		MACAddress: "00:1B:44:11:3A:99",
	})
	_, _ = deviceSvc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev.ID,
		VehicleID:    "veh-999",
	})

	t.Run("unauthorized without tenant", func(t *testing.T) {
		body := bytes.NewBufferString(`{}`)
		req := httptest.NewRequest("POST", "/api/v1/telemetry-batches", body)
		rr := httptest.NewRecorder()

		h.HandleIngestBatch(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}
	})

	t.Run("valid batch returns 201 Created", func(t *testing.T) {
		payload := `{
			"obd2DeviceId": "` + dev.ID + `",
			"snapshots": [
				{
					"rpm": 2000,
					"temperature": 85,
					"speedKmh": 50.0,
					"odometerKm": 15000.0,
					"fuelLevelPercent": 80.0,
					"createdAt": "` + time.Now().UTC().Format(time.RFC3339) + `",
					"dtcCodes": [{"dtcCode": "P0100", "description": "MAF sensor", "severity": "MEDIUM"}]
				}
			]
		}`

		req := httptest.NewRequest("POST", "/api/v1/telemetry-batches", bytes.NewBufferString(payload)).WithContext(ctx)
		rr := httptest.NewRecorder()

		h.HandleIngestBatch(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d (body: %s)", rr.Code, rr.Body.String())
		}
		if len(pub.batches) != 1 {
			t.Errorf("expected 1 published batch, got %d", len(pub.batches))
		}
	})

	t.Run("decreasing odometer returns 400 Bad Request", func(t *testing.T) {
		now := time.Now().UTC()
		payload := `{
			"obd2DeviceId": "` + dev.ID + `",
			"snapshots": [
				{
					"rpm": 2000, "temperature": 85, "speedKmh": 50.0, "odometerKm": 15000.0, "fuelLevelPercent": 80.0,
					"createdAt": "` + now.Format(time.RFC3339) + `"
				},
				{
					"rpm": 2000, "temperature": 85, "speedKmh": 50.0, "odometerKm": 14999.0, "fuelLevelPercent": 80.0,
					"createdAt": "` + now.Add(time.Minute).Format(time.RFC3339) + `"
				}
			]
		}`

		req := httptest.NewRequest("POST", "/api/v1/telemetry-batches", bytes.NewBufferString(payload)).WithContext(ctx)
		rr := httptest.NewRecorder()

		h.HandleIngestBatch(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request for decreasing odometer, got %d", rr.Code)
		}
	})

	t.Run("unlinked device returns 403 Forbidden", func(t *testing.T) {
		unlinkedDev, _ := deviceSvc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
			TenantID:   "tenant-1",
			BranchID:   "branch-1",
			MACAddress: "00:1B:44:11:3A:88",
		})

		payload := `{
			"obd2DeviceId": "` + unlinkedDev.ID + `",
			"snapshots": [
				{
					"rpm": 1500, "temperature": 80, "speedKmh": 40.0, "odometerKm": 500.0, "fuelLevelPercent": 50.0,
					"createdAt": "` + time.Now().UTC().Format(time.RFC3339) + `"
				}
			]
		}`

		req := httptest.NewRequest("POST", "/api/v1/telemetry-batches", bytes.NewBufferString(payload)).WithContext(ctx)
		rr := httptest.NewRecorder()

		h.HandleIngestBatch(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden for device without active vehicle, got %d", rr.Code)
		}
	})

	t.Run("malformed json routes to DLQ and returns 400", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/telemetry-batches", bytes.NewBufferString("{unparseable json")).WithContext(ctx)
		rr := httptest.NewRecorder()

		h.HandleIngestBatch(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request, got %d", rr.Code)
		}
		if len(pub.dlqPayloads) == 0 {
			t.Errorf("expected payload routed to DLQ")
		}
	})
}
