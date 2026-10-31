package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
)

func TestBatchApplicationService_IngestBatch_Success(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryDeviceRepository()
	pub := &mockPublisher{}
	deviceSvc := service.NewDeviceApplicationService(repo)
	batchSvc := service.NewBatchApplicationService(repo, pub)

	// Setup active device registration
	dev, _ := deviceSvc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
		TenantID:   "tenant-1",
		BranchID:   "branch-1",
		MACAddress: "00:1B:44:11:3A:B7",
	})
	_, _ = deviceSvc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev.ID,
		VehicleID:    "veh-100",
	})

	now := time.Now().UTC()
	batch := model.TelemetryBatch{
		TenantID:     "tenant-1",
		OBD2DeviceID: dev.ID,
		Snapshots: []model.TelemetrySnapshot{
			{
				RPM:              2200,
				Temperature:      88,
				SpeedKmH:         50.0,
				OdometerKm:       10000.0,
				FuelLevelPercent: 70.0,
				CreatedAt:        now,
				DTCCodes: []model.DTCCodeItem{
					{DTCCode: "P0300", Description: "Random Misfire", Severity: "HIGH"},
				},
			},
			{
				RPM:              2300,
				Temperature:      89,
				SpeedKmH:         52.0,
				OdometerKm:       10000.5,
				FuelLevelPercent: 69.9,
				CreatedAt:        now.Add(time.Second * 10),
			},
		},
	}

	err := batchSvc.IngestBatch(ctx, batch)
	if err != nil {
		t.Fatalf("unexpected error during batch ingest: %v", err)
	}

	if len(pub.batches) != 1 {
		t.Fatalf("expected 1 published batch, got %d", len(pub.batches))
	}
	if len(pub.batches[0]) != 2 {
		t.Fatalf("expected 2 packets in published batch, got %d", len(pub.batches[0]))
	}
	if pub.batches[0][0].VehicleID != "veh-100" {
		t.Errorf("expected vehicle ID veh-100 resolved from active registration, got %s", pub.batches[0][0].VehicleID)
	}
	if len(pub.batches[0][0].OBD.DTCs) != 1 || pub.batches[0][0].OBD.DTCs[0] != "P0300" {
		t.Errorf("expected DTC P0300 in packet, got %v", pub.batches[0][0].OBD.DTCs)
	}
}

func TestBatchApplicationService_UnregisteredDeviceFails(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryDeviceRepository()
	pub := &mockPublisher{}
	batchSvc := service.NewBatchApplicationService(repo, pub)

	batch := model.TelemetryBatch{
		TenantID:     "tenant-1",
		OBD2DeviceID: "unregistered-device-id",
		Snapshots: []model.TelemetrySnapshot{
			{
				RPM:              1500,
				Temperature:      80,
				SpeedKmH:         40.0,
				OdometerKm:       500.0,
				FuelLevelPercent: 50.0,
				CreatedAt:        time.Now().UTC(),
			},
		},
	}

	err := batchSvc.IngestBatch(ctx, batch)
	if err == nil || !errors.Is(err, domain.ErrDeviceNotAvailable) {
		t.Errorf("expected ErrDeviceNotAvailable for device without active vehicle registration, got %v", err)
	}
}
