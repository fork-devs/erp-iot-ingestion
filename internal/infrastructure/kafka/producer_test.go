package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	infraKafka "github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/kafka"
	"github.com/segmentio/kafka-go"
)

type mockWriter struct {
	messages []kafka.Message
	writeErr error
	closed   bool
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockWriter) Close() error {
	m.closed = true
	return nil
}

func samplePacket(t *testing.T) *model.TelemetryPacket {
	p, err := model.NewTelemetryPacket(
		"packet-1",
		"00000000-0000-0000-0000-000000000001",
		"VEH-001",
		"DEV-99",
		time.Now().UTC(),
		model.GPSLocation{Latitude: -12.046374, Longitude: -77.042793},
		model.OBDData{EngineRPM: 2200, SpeedKmH: 60.0},
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create sample packet: %v", err)
	}
	return p
}

func TestPublishTelemetry_Success(t *testing.T) {
	telemetryMock := &mockWriter{}
	dlqMock := &mockWriter{}
	pub := infraKafka.NewKafkaEventPublisherWithWriters(telemetryMock, dlqMock, []string{"localhost:9092"})

	packet := samplePacket(t)
	err := pub.PublishTelemetry(context.Background(), packet)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(telemetryMock.messages) != 1 {
		t.Fatalf("expected 1 message published, got: %d", len(telemetryMock.messages))
	}

	msg := telemetryMock.messages[0]
	if string(msg.Key) != "VEH-001" {
		t.Errorf("expected Key 'VEH-001', got: '%s'", string(msg.Key))
	}

	var parsed model.TelemetryPacket
	if err := json.Unmarshal(msg.Value, &parsed); err != nil {
		t.Fatalf("failed to unmarshal published value: %v", err)
	}
	if parsed.VehicleID != "VEH-001" {
		t.Errorf("expected vehicle_id VEH-001, got %s", parsed.VehicleID)
	}
}

func TestPublishBatch_Success(t *testing.T) {
	telemetryMock := &mockWriter{}
	dlqMock := &mockWriter{}
	pub := infraKafka.NewKafkaEventPublisherWithWriters(telemetryMock, dlqMock, []string{"localhost:9092"})

	p1 := samplePacket(t)
	p2 := samplePacket(t)
	p2.VehicleID = "VEH-002"

	err := pub.PublishBatch(context.Background(), []*model.TelemetryPacket{p1, p2})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(telemetryMock.messages) != 2 {
		t.Fatalf("expected 2 messages published, got: %d", len(telemetryMock.messages))
	}
	if string(telemetryMock.messages[0].Key) != "VEH-001" {
		t.Errorf("expected first key VEH-001, got %s", string(telemetryMock.messages[0].Key))
	}
	if string(telemetryMock.messages[1].Key) != "VEH-002" {
		t.Errorf("expected second key VEH-002, got %s", string(telemetryMock.messages[1].Key))
	}
}

func TestPublishDLQ_Success(t *testing.T) {
	telemetryMock := &mockWriter{}
	dlqMock := &mockWriter{}
	pub := infraKafka.NewKafkaEventPublisherWithWriters(telemetryMock, dlqMock, []string{"localhost:9092"})

	rawPayload := []byte(`{"invalid": "json payload"}`)
	err := pub.PublishDLQ(context.Background(), rawPayload, "validation failed: invalid GPS")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(dlqMock.messages) != 1 {
		t.Fatalf("expected 1 message in DLQ, got: %d", len(dlqMock.messages))
	}
	if string(dlqMock.messages[0].Value) != string(rawPayload) {
		t.Errorf("expected raw payload in DLQ, got: %s", string(dlqMock.messages[0].Value))
	}
}

func TestKafkaPublisher_Close(t *testing.T) {
	telemetryMock := &mockWriter{}
	dlqMock := &mockWriter{}
	pub := infraKafka.NewKafkaEventPublisherWithWriters(telemetryMock, dlqMock, []string{"localhost:9092"})

	if err := pub.Close(); err != nil {
		t.Fatalf("expected clean close, got: %v", err)
	}
	if !telemetryMock.closed || !dlqMock.closed {
		t.Errorf("expected both writers to be closed")
	}
}

func TestPublishTelemetry_WriterError(t *testing.T) {
	telemetryMock := &mockWriter{writeErr: errors.New("connection reset by peer")}
	dlqMock := &mockWriter{}
	pub := infraKafka.NewKafkaEventPublisherWithWriters(telemetryMock, dlqMock, []string{"localhost:9092"})

	packet := samplePacket(t)
	err := pub.PublishTelemetry(context.Background(), packet)
	if err == nil {
		t.Fatal("expected publish error, got nil")
	}
}
