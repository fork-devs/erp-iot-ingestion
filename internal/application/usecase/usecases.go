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

// IngestionUseCase specifies application operations for ingesting vehicle telemetry.
type IngestionUseCase interface {
	// Ingest validates and asynchronously buffers a single vehicle reading.
	Ingest(ctx context.Context, cmd IngestTelemetryCommand) (*model.TelemetryPacket, error)

	// IngestBatch validates and enqueues a batch of accumulated readings.
	IngestBatch(ctx context.Context, cmd IngestBatchCommand) ([]*model.TelemetryPacket, error)
}

// IngestBatchCommand carries a collection of telemetry readings reported in batch by a vehicle.
type IngestBatchCommand struct {
	TenantID string
	Readings []IngestTelemetryCommand
}

// RegisterDeviceCommand carries input data for registering a new physical OBD2 dongle.
type RegisterDeviceCommand struct {
	TenantID   string
	BranchID   string
	MACAddress string
}

// UpdateDeviceCommand carries input data for updating an existing physical OBD2 dongle.
type UpdateDeviceCommand struct {
	TenantID   string
	DeviceID   string
	MACAddress string
}

// DeviceUseCase specifies operations for physical OBD2 hardware devices.
type DeviceUseCase interface {
	RegisterDevice(ctx context.Context, cmd RegisterDeviceCommand) (*model.OBDDevice, error)
	UpdateDevice(ctx context.Context, cmd UpdateDeviceCommand) (*model.OBDDevice, error)
	DeleteDevice(ctx context.Context, tenantID, deviceID string) error
	GetDevice(ctx context.Context, tenantID, deviceID string) (*model.OBDDevice, error)
	ListDevices(ctx context.Context, tenantID, branchID string, status model.DeviceStatus) ([]*model.OBDDevice, error)
}

// LinkDeviceCommand carries data for binding an OBD2 device to a vehicle.
type LinkDeviceCommand struct {
	TenantID     string
	BranchID     string
	OBD2DeviceID string
	VehicleID    string
}

// UpdateRegistrationStatusCommand carries data for unlinking or updating registration status.
type UpdateRegistrationStatusCommand struct {
	TenantID       string
	RegistrationID string
	Status         model.RegistrationStatus
}

// RegistrationUseCase specifies operations for managing device-to-vehicle bindings.
type RegistrationUseCase interface {
	LinkDevice(ctx context.Context, cmd LinkDeviceCommand) (*model.DeviceRegistration, error)
	UpdateRegistrationStatus(ctx context.Context, cmd UpdateRegistrationStatusCommand) (*model.DeviceRegistration, error)
	ListRegistrations(ctx context.Context, tenantID, branchID string, status model.RegistrationStatus) ([]*model.DeviceRegistration, error)
}

// BatchIngestUseCase specifies operations for ingesting periodic batches of telemetry snapshots.
type BatchIngestUseCase interface {
	IngestBatch(ctx context.Context, batch model.TelemetryBatch) error
}
