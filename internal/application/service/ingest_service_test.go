package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
)

type mockPublisher struct {
	published []*model.TelemetryPacket
	batches   [][]*model.TelemetryPacket
	err       error
}

func (m *mockPublisher) PublishTelemetry(ctx context.Context, packet *model.TelemetryPacket) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, packet)
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error {
	if m.err != nil {
		return m.err
	}
	m.batches = append(m.batches, packets)
	return nil
}

func (m *mockPublisher) PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error {
	return nil
}

func (m *mockPublisher) Ping(ctx context.Context) error {
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func TestIngestionService_Ingest_Success(t *testing.T) {
	pub := &mockPublisher{}
	svc := service.NewIngestionService(pub)

	cmd := usecase.IngestTelemetryCommand{
		TenantID:  "00000000-0000-0000-0000-000000000001",
		VehicleID: "VEH-100",
		DeviceID:  "OBD-DEV-1",
		Timestamp: time.Now().UTC(),
		Location:  model.GPSLocation{Latitude: -12.0, Longitude: -77.0},
		OBD:       model.OBDData{EngineRPM: 1800, SpeedKmH: 45.0},
	}

	packet, err := svc.Ingest(context.Background(), cmd)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if packet == nil {
		t.Fatal("expected non-nil packet")
	}
	if len(pub.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(pub.published))
	}
	if pub.published[0].VehicleID != "VEH-100" {
		t.Errorf("expected vehicle VEH-100, got %s", pub.published[0].VehicleID)
	}
}

func TestIngestionService_Ingest_InvalidData(t *testing.T) {
	pub := &mockPublisher{}
	svc := service.NewIngestionService(pub)

	cmd := usecase.IngestTelemetryCommand{
		TenantID:  "", // missing tenant
		VehicleID: "VEH-100",
		DeviceID:  "OBD-DEV-1",
		Location:  model.GPSLocation{Latitude: -12.0, Longitude: -77.0},
		OBD:       model.OBDData{EngineRPM: 1800, SpeedKmH: 45.0},
	}

	_, err := svc.Ingest(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if len(pub.published) != 0 {
		t.Errorf("expected 0 published messages on invalid data, got %d", len(pub.published))
	}
}

func TestIngestionService_IngestBatch_Success(t *testing.T) {
	pub := &mockPublisher{}
	svc := service.NewIngestionService(pub)

	cmd := usecase.IngestBatchCommand{
		TenantID: "00000000-0000-0000-0000-000000000001",
		Readings: []usecase.IngestTelemetryCommand{
			{
				VehicleID: "VEH-100",
				DeviceID:  "DEV-1",
				Location:  model.GPSLocation{Latitude: -12.0, Longitude: -77.0},
				OBD:       model.OBDData{EngineRPM: 2000, SpeedKmH: 50.0},
			},
			{
				VehicleID: "VEH-100",
				DeviceID:  "DEV-1",
				Location:  model.GPSLocation{Latitude: -12.01, Longitude: -77.01},
				OBD:       model.OBDData{EngineRPM: 2100, SpeedKmH: 52.0},
			},
		},
	}

	packets, err := svc.IngestBatch(context.Background(), cmd)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}
	if len(pub.batches) != 1 {
		t.Fatalf("expected 1 batch published, got %d", len(pub.batches))
	}
	if len(pub.batches[0]) != 2 {
		t.Errorf("expected 2 messages in published batch, got %d", len(pub.batches[0]))
	}
}

func TestIngestionService_IngestBatch_EmptyFails(t *testing.T) {
	pub := &mockPublisher{}
	svc := service.NewIngestionService(pub)

	cmd := usecase.IngestBatchCommand{
		TenantID: "00000000-0000-0000-0000-000000000001",
		Readings: []usecase.IngestTelemetryCommand{},
	}

	_, err := svc.IngestBatch(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected error on empty batch, got nil")
	}
}

func TestIngestionService_PublishError(t *testing.T) {
	pub := &mockPublisher{err: errors.New("kafka unavailable")}
	svc := service.NewIngestionService(pub)

	cmd := usecase.IngestTelemetryCommand{
		TenantID:  "00000000-0000-0000-0000-000000000001",
		VehicleID: "VEH-100",
		DeviceID:  "OBD-DEV-1",
		Location:  model.GPSLocation{Latitude: -12.0, Longitude: -77.0},
		OBD:       model.OBDData{EngineRPM: 1800, SpeedKmH: 45.0},
	}

	_, err := svc.Ingest(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected publish error, got nil")
	}
}
