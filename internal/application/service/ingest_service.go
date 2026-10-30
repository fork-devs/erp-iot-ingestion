package service

import (
	"context"
	"fmt"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
)

// IngestionService implements usecase.IngestionUseCase by validating packets and delegating to Kafka.
type IngestionService struct {
	publisher port.EventPublisherPort
}

// Ensure interface adherence at compile time.
var _ usecase.IngestionUseCase = (*IngestionService)(nil)

// NewIngestionService creates an IngestionService instance.
func NewIngestionService(publisher port.EventPublisherPort) *IngestionService {
	return &IngestionService{
		publisher: publisher,
	}
}

// Ingest constructs a pure domain model, validates boundaries, and publishes to Kafka.
func (s *IngestionService) Ingest(ctx context.Context, cmd usecase.IngestTelemetryCommand) (*model.TelemetryPacket, error) {
	packet, err := model.NewTelemetryPacket(
		"",
		cmd.TenantID,
		cmd.VehicleID,
		cmd.DeviceID,
		cmd.Timestamp,
		cmd.Location,
		cmd.OBD,
		cmd.Metadata,
	)
	if err != nil {
		return nil, err
	}

	if err := s.publisher.PublishTelemetry(ctx, packet); err != nil {
		return nil, fmt.Errorf("failed to enqueue telemetry packet: %w", err)
	}

	return packet, nil
}

// IngestBatch processes and validates an array of readings before publishing as a single batch to Kafka.
func (s *IngestionService) IngestBatch(ctx context.Context, cmd usecase.IngestBatchCommand) ([]*model.TelemetryPacket, error) {
	if len(cmd.Readings) == 0 {
		return nil, fmt.Errorf("%w: batch cannot be empty", domain.ErrInvalidTelemetry)
	}

	packets := make([]*model.TelemetryPacket, 0, len(cmd.Readings))
	for i, r := range cmd.Readings {
		tenant := r.TenantID
		if tenant == "" {
			tenant = cmd.TenantID
		}

		packet, err := model.NewTelemetryPacket(
			"",
			tenant,
			r.VehicleID,
			r.DeviceID,
			r.Timestamp,
			r.Location,
			r.OBD,
			r.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("reading at index %d: %w", i, err)
		}
		packets = append(packets, packet)
	}

	if err := s.publisher.PublishBatch(ctx, packets); err != nil {
		return nil, fmt.Errorf("failed to enqueue telemetry batch: %w", err)
	}

	return packets, nil
}
