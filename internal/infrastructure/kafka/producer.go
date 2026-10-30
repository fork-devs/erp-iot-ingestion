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

	msg := kafka.Message{
		Key:   []byte(packet.VehicleID),
		Value: payload,
		Time:  packet.Timestamp,
		Headers: []kafka.Header{
			{Key: "tenant_id", Value: []byte(packet.TenantID)},
			{Key: "device_id", Value: []byte(packet.DeviceID)},
			{Key: "content_type", Value: []byte("application/json")},
		},
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("%w: failed to publish to kafka: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// PublishBatch writes multiple telemetry packets concurrently to Kafka in a single batch operation.
func (p *KafkaEventPublisher) PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error {
	if len(packets) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, len(packets))
	for i, packet := range packets {
		payload, err := json.Marshal(packet)
		if err != nil {
			return fmt.Errorf("%w: failed to serialize batch packet at index %d: %w", domain.ErrInvalidTelemetry, i, err)
		}

		msgs[i] = kafka.Message{
			Key:   []byte(packet.VehicleID),
			Value: payload,
			Time:  packet.Timestamp,
			Headers: []kafka.Header{
				{Key: "tenant_id", Value: []byte(packet.TenantID)},
				{Key: "device_id", Value: []byte(packet.DeviceID)},
				{Key: "content_type", Value: []byte("application/json")},
			},
		}
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("%w: failed to publish batch to kafka: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// PublishDLQ stores corrupt, invalid, or rejected payloads to the dead-letter queue.
func (p *KafkaEventPublisher) PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error {
	msg := kafka.Message{
		Value: rawPayload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "error_reason", Value: []byte(reason)},
			{Key: "timestamp", Value: []byte(strconv.FormatInt(time.Now().Unix(), 10))},
		},
	}

	if err := p.dlqWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("%w: failed to publish to DLQ: %w", domain.ErrPublishFailed, err)
	}
	return nil
}

// Ping checks if at least one Kafka broker is reachable via TCP dial.
func (p *KafkaEventPublisher) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka broker unreachable at %s: %w", p.brokers[0], err)
	}
	_ = conn.Close()
	return nil
}

// Close flushes any pending messages in buffer and closes writer connections cleanly.
func (p *KafkaEventPublisher) Close() error {
	var errTelemetry, errDLQ error
	if p.telemetryWriter != nil {
		errTelemetry = p.telemetryWriter.Close()
	}
	if p.dlqWriter != nil {
		errDLQ = p.dlqWriter.Close()
	}

	if errTelemetry != nil {
		return fmt.Errorf("error closing telemetry writer: %w", errTelemetry)
	}
	if errDLQ != nil {
		return fmt.Errorf("error closing DLQ writer: %w", errDLQ)
	}
	return nil
}
