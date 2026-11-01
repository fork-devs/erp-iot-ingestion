package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/config"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/telemetry"
	"github.com/segmentio/kafka-go"
)

// Writer defines the minimal subset of kafka.Writer methods for testability.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaEventPublisher implements port.EventPublisherPort using segmentio/kafka-go.
type KafkaEventPublisher struct {
	telemetryWriter Writer
	dlqWriter       Writer
	brokers         []string
}

// Ensure interface compliance at compile time.
var _ port.EventPublisherPort = (*KafkaEventPublisher)(nil)

// NewKafkaEventPublisher instantiates optimized producers for raw telemetry and DLQ topics.
func NewKafkaEventPublisher(cfg *config.Config) *KafkaEventPublisher {
	telemetryWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTelemetryTopic,
		Balancer:     &kafka.Hash{}, // Guarantees deterministic partition routing by Key (vehicle_id)
		BatchSize:    cfg.KafkaBatchSize,
		BatchTimeout: cfg.KafkaBatchTimeout,
		Async:        cfg.KafkaAsync,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaDLQTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // DLQ writes should be synchronous for forensic durability
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaEventPublisher{
		telemetryWriter: telemetryWriter,
		dlqWriter:       dlqWriter,
		brokers:         cfg.KafkaBrokers,
	}
}

// NewKafkaEventPublisherWithWriters allows injecting custom or mock writers for unit testing.
func NewKafkaEventPublisherWithWriters(telemetryWriter Writer, dlqWriter Writer, brokers []string) *KafkaEventPublisher {
	return &KafkaEventPublisher{
		telemetryWriter: telemetryWriter,
		dlqWriter:       dlqWriter,
		brokers:         brokers,
	}
}

// PublishTelemetry publishes a single telemetry event to telemetry.raw partitioned by vehicle_id.
func (p *KafkaEventPublisher) PublishTelemetry(ctx context.Context, packet *model.TelemetryPacket) error {
	payload, err := json.Marshal(packet)
	if err != nil {
		return fmt.Errorf("%w: failed to serialize telemetry packet: %w", domain.ErrInvalidTelemetry, err)
	}

	headers := []kafka.Header{
		{Key: "tenant_id", Value: []byte(packet.TenantID)},
		{Key: "device_id", Value: []byte(packet.DeviceID)},
		{Key: "content_type", Value: []byte("application/json")},
	}

	if corrID := middleware.CorrelationFromContext(ctx); corrID != "" {
		headers = append(headers, kafka.Header{Key: "correlation_id", Value: []byte(corrID)})
	}

	msg := kafka.Message{
		Key:     []byte(packet.VehicleID),
		Value:   payload,
		Time:    packet.Timestamp,
		Headers: headers,
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msg); err != nil {
		telemetry.DefaultMetrics.RecordKafkaError()
		return fmt.Errorf("%w: failed to publish to kafka: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// PublishBatch writes multiple telemetry packets concurrently to Kafka in a single batch operation.
func (p *KafkaEventPublisher) PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error {
	if len(packets) == 0 {
		return nil
	}

	corrID := middleware.CorrelationFromContext(ctx)

	msgs := make([]kafka.Message, len(packets))
	for i, packet := range packets {
		payload, err := json.Marshal(packet)
		if err != nil {
			return fmt.Errorf("%w: failed to serialize batch packet at index %d: %w", domain.ErrInvalidTelemetry, i, err)
		}

		headers := []kafka.Header{
			{Key: "tenant_id", Value: []byte(packet.TenantID)},
			{Key: "device_id", Value: []byte(packet.DeviceID)},
			{Key: "content_type", Value: []byte("application/json")},
		}
		if corrID != "" {
			headers = append(headers, kafka.Header{Key: "correlation_id", Value: []byte(corrID)})
		}

		msgs[i] = kafka.Message{
			Key:     []byte(packet.VehicleID),
			Value:   payload,
			Time:    packet.Timestamp,
			Headers: headers,
		}
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msgs...); err != nil {
		telemetry.DefaultMetrics.RecordKafkaError()
		return fmt.Errorf("%w: failed to publish batch to kafka: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// PublishDLQ stores corrupt, invalid, or rejected payloads to the dead-letter queue.
func (p *KafkaEventPublisher) PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error {
	telemetry.DefaultMetrics.RecordDLQDispatch()

	headers := []kafka.Header{
		{Key: "error_reason", Value: []byte(reason)},
		{Key: "timestamp", Value: []byte(strconv.FormatInt(time.Now().Unix(), 10))},
	}
	if corrID := middleware.CorrelationFromContext(ctx); corrID != "" {
		headers = append(headers, kafka.Header{Key: "correlation_id", Value: []byte(corrID)})
	}

	msg := kafka.Message{
		Value:   rawPayload,
		Time:    time.Now().UTC(),
		Headers: headers,
	}

	if err := p.dlqWriter.WriteMessages(ctx, msg); err != nil {
		telemetry.DefaultMetrics.RecordKafkaError()
		return fmt.Errorf("%w: failed to publish to DLQ: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// Ping verifies connectivity to at least one Kafka broker address.
func (p *KafkaEventPublisher) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}

	for _, broker := range p.brokers {
		d := net.Dialer{Timeout: 2 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", broker)
		if err == nil {
			_ = conn.Close()
			return nil
		}
	}

	return fmt.Errorf("failed to reach any kafka broker from %v", p.brokers)
}

// Close flushes buffers and closes writers safely.
func (p *KafkaEventPublisher) Close() error {
	var firstErr error
	if err := p.telemetryWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := p.dlqWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
