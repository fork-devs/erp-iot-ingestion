package port

import (
	"context"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
)

// EventPublisherPort defines the outbound boundary for publishing vehicle telemetry events to Kafka.
type EventPublisherPort interface {
	// PublishTelemetry publishes a single telemetry packet to telemetry.raw using vehicle_id as partition key.
	PublishTelemetry(ctx context.Context, packet *model.TelemetryPacket) error

	// PublishBatch publishes multiple telemetry packets efficiently in batch mode.
	PublishBatch(ctx context.Context, packets []*model.TelemetryPacket) error

	// PublishDLQ routes malformed or unprocessable payloads to the dead-letter queue.
	PublishDLQ(ctx context.Context, rawPayload []byte, reason string) error

	// Ping checks connectivity to the messaging broker.
	Ping(ctx context.Context) error

	// Close flushes buffers and closes the publisher connection safely.
	Close() error
}

// DeviceValidatorPort verifies the authenticity of a device API key and resolves the corresponding tenant_id.
type DeviceValidatorPort interface {
	// ValidateKey checks if the device key is authorized and returns its associated tenant ID.
	ValidateKey(ctx context.Context, deviceKey string) (tenantID string, err error)
}
