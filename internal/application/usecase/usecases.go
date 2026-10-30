package usecase

import (
	"context"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
)

// IngestTelemetryCommand carries input data for a single vehicle telemetry event.
type IngestTelemetryCommand struct {
	TenantID  string
	VehicleID string
	DeviceID  string
	Timestamp time.Time
	Location  model.GPSLocation
	OBD       model.OBDData
	Metadata  map[string]string
}

// IngestBatchCommand carries a collection of telemetry readings reported in batch by a vehicle.
type IngestBatchCommand struct {
	TenantID string
	Readings []IngestTelemetryCommand
}

// IngestionUseCase specifies application operations for ingesting vehicle telemetry.
type IngestionUseCase interface {
	// Ingest validates and asynchronously buffers a single vehicle reading.
	Ingest(ctx context.Context, cmd IngestTelemetryCommand) (*model.TelemetryPacket, error)

	// IngestBatch validates and enqueues a batch of accumulated readings.
	IngestBatch(ctx context.Context, cmd IngestBatchCommand) ([]*model.TelemetryPacket, error)
}
