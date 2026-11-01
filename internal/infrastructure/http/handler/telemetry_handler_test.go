package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/handler"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/go-chi/chi/v5"
)

type mockPublisher struct {
	published []*model.TelemetryPacket
	batches   [][]*model.TelemetryPacket
	dlq       []string
	pingErr   error
}

func (m *mockPublisher) PublishTelemetry(ctx context.Context, packet *model.TelemetryPacket) error {
	m.published = append(m.published, packet)
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error {
	m.batches = append(m.batches, packets)
	return nil
}

func (m *mockPublisher) PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error {
	m.dlq = append(m.dlq, reason)
	return nil
}

func (m *mockPublisher) Ping(ctx context.Context) error {
	return m.pingErr
}

func (m *mockPublisher) Close() error {
	return nil
}

func setupTestRouter(pub *mockPublisher) http.Handler {
	r := chi.NewRouter()

	ingestSvc := service.NewIngestionService(pub)
	telemetryH := handler.NewTelemetryHandler(ingestSvc, pub, 1048576)
	healthH := handler.NewHealthHandler(pub)

	authMiddleware := middleware.NewDeviceAuthenticator(map[string]string{
		"valid-device-key": "00000000-0000-0000-0000-000000000001",
	})

	r.Get("/healthz", healthH.Liveness)
	r.Get("/readyz", healthH.Readiness)

	r.Route("/api/v1/iot", func(sub chi.Router) {
		sub.Use(authMiddleware.Handler)
		sub.Post("/telemetry", telemetryH.HandleIngest)
	})

	return r
}

func TestTelemetryHandler_Unauthorized(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/iot/telemetry", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized, got %d", rec.Code)
	}
}

func TestTelemetryHandler_SingleIngest_Success(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	payload := `{
		"vehicle_id": "V-555",
		"device_id": "DEV-1",
		"location": {"latitude": -12.046374, "longitude": -77.042793},
		"obd": {"engine_rpm": 2400, "speed_kmh": 60.5}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/iot/telemetry", bytes.NewBufferString(payload))
	req.Header.Set("X-Device-Key", "valid-device-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var res handler.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res.Accepted != 1 {
		t.Errorf("expected accepted count 1, got %d", res.Accepted)
	}
	if len(pub.published) != 1 {
		t.Fatalf("expected 1 message published to kafka, got %d", len(pub.published))
	}
	if pub.published[0].VehicleID != "V-555" {
		t.Errorf("expected vehicle V-555, got %s", pub.published[0].VehicleID)
	}
}

func TestTelemetryHandler_BatchIngest_Success(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	payload := `[
		{
			"vehicle_id": "V-555",
			"location": {"latitude": -12.04, "longitude": -77.04},
			"obd": {"engine_rpm": 2000, "speed_kmh": 50.0}
		},
		{
			"vehicle_id": "V-555",
			"location": {"latitude": -12.05, "longitude": -77.05},
			"obd": {"engine_rpm": 2100, "speed_kmh": 55.0}
		}
	]`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/iot/telemetry", bytes.NewBufferString(payload))
	req.Header.Set("X-Device-Key", "valid-device-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var res handler.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res.Accepted != 2 {
		t.Errorf("expected accepted count 2, got %d", res.Accepted)
	}
	if len(pub.batches) != 1 || len(pub.batches[0]) != 2 {
		t.Fatalf("expected 1 batch with 2 packets published to kafka")
	}
}

func TestTelemetryHandler_InvalidData_RoutedToDLQ(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	// Invalid latitude 95.0
	payload := `{
		"vehicle_id": "V-555",
		"location": {"latitude": 95.0, "longitude": -77.04},
		"obd": {"engine_rpm": 2000, "speed_kmh": 50.0}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/iot/telemetry", bytes.NewBufferString(payload))
	req.Header.Set("X-Device-Key", "valid-device-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", rec.Code)
	}
	if len(pub.dlq) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(pub.dlq))
	}
}

func TestTelemetryHandler_MalformedSyntax_RoutedToDLQ(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/iot/telemetry", bytes.NewBufferString("random raw corrupt string"))
	req.Header.Set("X-Device-Key", "valid-device-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", rec.Code)
	}
	if len(pub.dlq) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(pub.dlq))
	}
}

func TestHealthHandler_Liveness(t *testing.T) {
	pub := &mockPublisher{}
	router := setupTestRouter(pub)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
}

func TestHealthHandler_Readiness_Success(t *testing.T) {
	pub := &mockPublisher{pingErr: nil}
	router := setupTestRouter(pub)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
}

func TestHealthHandler_Readiness_Failure(t *testing.T) {
	pub := &mockPublisher{pingErr: errors.New("cannot connect to broker")}
	router := setupTestRouter(pub)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable, got %d", rec.Code)
	}
}
