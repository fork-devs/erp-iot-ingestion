package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
)

// BatchApplicationService processes and forwards batches of vehicle telemetry snapshots.
type BatchApplicationService struct {
	repo      port.DeviceRepositoryPort
	publisher port.EventPublisherPort
}

// Compile-time interface check.
var _ usecase.BatchIngestUseCase = (*BatchApplicationService)(nil)

// NewBatchApplicationService creates an instance of BatchApplicationService.
func NewBatchApplicationService(repo port.DeviceRepositoryPort, publisher port.EventPublisherPort) *BatchApplicationService {
	return &BatchApplicationService{
		repo:      repo,
		publisher: publisher,
	}
}

// IngestBatch validates snapshots, resolves active vehicle binding, and enqueues to Kafka topic telemetry.raw.
func (s *BatchApplicationService) IngestBatch(ctx context.Context, batch model.TelemetryBatch) error {
	if err := batch.Validate(); err != nil {
		return err
	}

	vehicleID := batch.VehicleID
	if s.repo != nil {
		reg, err := s.repo.FindActiveRegistrationByDevice(ctx, batch.TenantID, batch.OBD2DeviceID)
		if err != nil {
			return err
		}
		if reg != nil {
			vehicleID = reg.VehicleID
		}
	}

	if vehicleID == "" {
		// Device is not currently bound to any vehicle
		return fmt.Errorf("%w: device %s has no active vehicle registration", domain.ErrDeviceNotAvailable, batch.OBD2DeviceID)
	}

	packets := make([]*model.TelemetryPacket, 0, len(batch.Snapshots))
	for _, snap := range batch.Snapshots {
		var dtcStrings []string
		for _, dtc := range snap.DTCCodes {
			if dtc.DTCCode != "" {
				dtcStrings = append(dtcStrings, dtc.DTCCode)
			}
		}

		obd := model.OBDData{
			EngineRPM:        snap.RPM,
			CoolantTempC:     snap.Temperature,
			SpeedKmH:         snap.SpeedKmH,
			FuelLevelPercent: snap.FuelLevelPercent,
			DTCs:             dtcStrings,
		}

		metadata := map[string]string{
			"odometer_km": strconv.FormatFloat(snap.OdometerKm, 'f', 2, 64),
			"batch_source": "telemetry-batches",
		}

		location := model.GPSLocation{
			Latitude:  0,
			Longitude: 0,
		}

		packet, err := model.NewTelemetryPacket(
			"",
			batch.TenantID,
			vehicleID,
			batch.OBD2DeviceID,
			snap.CreatedAt,
			location,
			obd,
			metadata,
		)
		if err != nil {
			return fmt.Errorf("failed to construct telemetry packet: %w", err)
		}

		packets = append(packets, packet)
	}

	if err := s.publisher.PublishBatch(ctx, packets); err != nil {
		return fmt.Errorf("%w: %w", domain.ErrPublishFailed, err)
	}

	return nil
}
