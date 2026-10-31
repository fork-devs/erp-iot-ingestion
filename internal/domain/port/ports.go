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

// DeviceRepositoryPort defines persistence and retrieval contracts for physical OBD2 devices and vehicle bindings.
type DeviceRepositoryPort interface {
	// SaveDevice persists a new or updated OBD2 device.
	SaveDevice(ctx context.Context, device *model.OBDDevice) error

	// FindDeviceByID looks up a device by ID within a tenant.
	FindDeviceByID(ctx context.Context, tenantID, deviceID string) (*model.OBDDevice, error)

	// FindDeviceByMAC looks up a device by MAC address within a tenant.
	FindDeviceByMAC(ctx context.Context, tenantID, mac string) (*model.OBDDevice, error)

	// ListDevices lists devices filtered by branch and optional status.
	ListDevices(ctx context.Context, tenantID, branchID string, status model.DeviceStatus) ([]*model.OBDDevice, error)

	// SaveRegistration persists a new device-to-vehicle registration.
	SaveRegistration(ctx context.Context, reg *model.DeviceRegistration) error

	// FindRegistrationByID looks up a registration by ID within a tenant.
	FindRegistrationByID(ctx context.Context, tenantID, regID string) (*model.DeviceRegistration, error)

	// FindActiveRegistrationByDevice checks if an OBD2 device currently has an ACTIVE registration.
	FindActiveRegistrationByDevice(ctx context.Context, tenantID, deviceID string) (*model.DeviceRegistration, error)

	// FindActiveRegistrationByVehicle checks if a vehicle currently has an ACTIVE registration.
	FindActiveRegistrationByVehicle(ctx context.Context, tenantID, vehicleID string) (*model.DeviceRegistration, error)

	// ListRegistrations lists registrations filtered by branch and optional status.
	ListRegistrations(ctx context.Context, tenantID, branchID string, status model.RegistrationStatus) ([]*model.DeviceRegistration, error)

	// UpdateRegistration updates an existing device registration.
	UpdateRegistration(ctx context.Context, reg *model.DeviceRegistration) error
}
