package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
)

func TestDeviceApplicationService_RegisterAndList(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryDeviceRepository()
	svc := service.NewDeviceApplicationService(repo)

	// Register device
	dev, err := svc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
		TenantID:   "tenant-1",
		BranchID:   "branch-1",
		MACAddress: "00:1B:44:11:3A:B7",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dev.Status != model.DeviceStatusAvailable {
		t.Errorf("expected AVAILABLE status, got %s", dev.Status)
	}

	// Get device
	found, err := svc.GetDevice(ctx, "tenant-1", dev.ID)
	if err != nil {
		t.Fatalf("failed to get device: %v", err)
	}
	if found.MACAddress != "00:1B:44:11:3A:B7" {
		t.Errorf("expected MAC 00:1B:44:11:3A:B7, got %s", found.MACAddress)
	}

	// List devices
	list, err := svc.ListDevices(ctx, "tenant-1", "branch-1", "")
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 device, got %d", len(list))
	}
}

func TestDeviceApplicationService_LinkAndUnlinkExclusivity(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryDeviceRepository()
	svc := service.NewDeviceApplicationService(repo)

	dev1, _ := svc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
		TenantID:   "tenant-1",
		BranchID:   "branch-1",
		MACAddress: "00:1B:44:11:3A:01",
	})
	dev2, _ := svc.RegisterDevice(ctx, usecase.RegisterDeviceCommand{
		TenantID:   "tenant-1",
		BranchID:   "branch-1",
		MACAddress: "00:1B:44:11:3A:02",
	})

	// Link dev1 to veh1 -> Success
	reg1, err := svc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev1.ID,
		VehicleID:    "veh-1",
	})
	if err != nil {
		t.Fatalf("unexpected error linking: %v", err)
	}
	if reg1.Status != model.RegistrationStatusActive {
		t.Errorf("expected ACTIVE status, got %s", reg1.Status)
	}

	// Linking already linked dev1 again -> Fails (ErrDeviceNotAvailable or ErrDeviceAlreadyLinked)
	_, err = svc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev1.ID,
		VehicleID:    "veh-2",
	})
	if err == nil {
		t.Errorf("expected error when linking already linked device")
	}

	// Linking dev2 to already linked veh1 -> Fails with ErrVehicleAlreadyLinked
	_, err = svc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev2.ID,
		VehicleID:    "veh-1",
	})
	if err == nil || !errors.Is(err, domain.ErrVehicleAlreadyLinked) {
		t.Errorf("expected ErrVehicleAlreadyLinked, got %v", err)
	}

	// Unlink dev1 -> restores status
	updatedReg, err := svc.UpdateRegistrationStatus(ctx, usecase.UpdateRegistrationStatusCommand{
		TenantID:       "tenant-1",
		RegistrationID: reg1.ID,
		Status:         model.RegistrationStatusInactive,
	})
	if err != nil {
		t.Fatalf("unexpected error unlinking: %v", err)
	}
	if updatedReg.Status != model.RegistrationStatusInactive {
		t.Errorf("expected INACTIVE status, got %s", updatedReg.Status)
	}

	// Now dev1 should be available again
	updatedDev1, _ := svc.GetDevice(ctx, "tenant-1", dev1.ID)
	if updatedDev1.Status != model.DeviceStatusAvailable {
		t.Errorf("expected AVAILABLE status after unlink, got %s", updatedDev1.Status)
	}

	// Now veh1 can be linked to dev2
	reg2, err := svc.LinkDevice(ctx, usecase.LinkDeviceCommand{
		TenantID:     "tenant-1",
		BranchID:     "branch-1",
		OBD2DeviceID: dev2.ID,
		VehicleID:    "veh-1",
	})
	if err != nil {
		t.Fatalf("expected allowed linking veh-1 after previous dev unlinked, got: %v", err)
	}
	if reg2.Status != model.RegistrationStatusActive {
		t.Errorf("expected ACTIVE, got %s", reg2.Status)
	}
}
