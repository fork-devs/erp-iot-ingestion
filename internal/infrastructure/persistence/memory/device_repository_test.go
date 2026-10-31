package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
)

func TestMemoryDeviceRepository_DeviceLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryDeviceRepository()

	dev, err := model.NewOBDDevice("dev-1", "tenant-1", "branch-1", "00:1B:44:11:3A:B7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := repo.SaveDevice(ctx, dev); err != nil {
		t.Fatalf("failed to save device: %v", err)
	}

	// Find by ID
	found, err := repo.FindDeviceByID(ctx, "tenant-1", "dev-1")
	if err != nil {
		t.Fatalf("failed to find device by ID: %v", err)
	}
	if found.MACAddress != "00:1B:44:11:3A:B7" {
		t.Errorf("expected MAC 00:1B:44:11:3A:B7, got %s", found.MACAddress)
	}

	// Multi-tenant isolation: cannot find with wrong tenant
	_, err = repo.FindDeviceByID(ctx, "tenant-2", "dev-1")
	if !errors.Is(err, domain.ErrDeviceNotFound) {
		t.Errorf("expected ErrDeviceNotFound for other tenant, got %v", err)
	}

	// Find by MAC
	foundMAC, err := repo.FindDeviceByMAC(ctx, "tenant-1", "00:1B:44:11:3A:B7")
	if err != nil {
		t.Fatalf("failed to find device by MAC: %v", err)
	}
	if foundMAC.ID != "dev-1" {
		t.Errorf("expected ID dev-1, got %s", foundMAC.ID)
	}

	// Duplicate MAC conflict
	dupDev, _ := model.NewOBDDevice("dev-2", "tenant-1", "branch-1", "00:1B:44:11:3A:B7")
	err = repo.SaveDevice(ctx, dupDev)
	if !errors.Is(err, domain.ErrDeviceConflict) {
		t.Errorf("expected ErrDeviceConflict, got %v", err)
	}

	// Same MAC in another tenant is allowed (multi-tenant isolation)
	otherTenantDev, _ := model.NewOBDDevice("dev-3", "tenant-2", "branch-2", "00:1B:44:11:3A:B7")
	if err := repo.SaveDevice(ctx, otherTenantDev); err != nil {
		t.Fatalf("expected allowed same MAC in different tenant, got %v", err)
	}
}

func TestMemoryDeviceRepository_ListAndFilter(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryDeviceRepository()

	d1, _ := model.NewOBDDevice("d1", "tenant-1", "branch-A", "00:11:22:33:44:01")
	d2, _ := model.NewOBDDevice("d2", "tenant-1", "branch-A", "00:11:22:33:44:02")
	_ = d2.Link()
	d3, _ := model.NewOBDDevice("d3", "tenant-1", "branch-B", "00:11:22:33:44:03")

	_ = repo.SaveDevice(ctx, d1)
	_ = repo.SaveDevice(ctx, d2)
	_ = repo.SaveDevice(ctx, d3)

	// List branch A only
	listA, err := repo.ListDevices(ctx, "tenant-1", "branch-A", "")
	if err != nil || len(listA) != 2 {
		t.Fatalf("expected 2 devices in branch A, got %d (err: %v)", len(listA), err)
	}

	// List branch A available only
	listAvail, err := repo.ListDevices(ctx, "tenant-1", "branch-A", model.DeviceStatusAvailable)
	if err != nil || len(listAvail) != 1 {
		t.Fatalf("expected 1 available device in branch A, got %d", len(listAvail))
	}
	if listAvail[0].ID != "d1" {
		t.Errorf("expected d1, got %s", listAvail[0].ID)
	}
}

func TestMemoryDeviceRepository_RegistrationLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryDeviceRepository()

	reg, err := model.NewDeviceRegistration("reg-1", "tenant-1", "branch-1", "dev-1", "veh-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := repo.SaveRegistration(ctx, reg); err != nil {
		t.Fatalf("failed to save registration: %v", err)
	}

	// Active checks
	activeDev, err := repo.FindActiveRegistrationByDevice(ctx, "tenant-1", "dev-1")
	if err != nil || activeDev == nil || activeDev.ID != "reg-1" {
		t.Errorf("expected active registration for dev-1, got %v", activeDev)
	}

	activeVeh, err := repo.FindActiveRegistrationByVehicle(ctx, "tenant-1", "veh-1")
	if err != nil || activeVeh == nil || activeVeh.ID != "reg-1" {
		t.Errorf("expected active registration for veh-1, got %v", activeVeh)
	}

	// Deactivate
	reg.Deactivate()
	if err := repo.UpdateRegistration(ctx, reg); err != nil {
		t.Fatalf("failed to update registration: %v", err)
	}

	activeDevAfter, err := repo.FindActiveRegistrationByDevice(ctx, "tenant-1", "dev-1")
	if err != nil || activeDevAfter != nil {
		t.Errorf("expected nil active registration after deactivation, got %v", activeDevAfter)
	}
}

func TestMemoryDeviceRepository_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryDeviceRepository()

	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				devID := fmt.Sprintf("dev-%d-%d", workerID, j)
				mac := fmt.Sprintf("00:1B:44:11:%02X:%02X", workerID, j)
				d, _ := model.NewOBDDevice(devID, "tenant-1", "branch-1", mac)
				_ = repo.SaveDevice(ctx, d)
				_, _ = repo.FindDeviceByID(ctx, "tenant-1", devID)
				_, _ = repo.ListDevices(ctx, "tenant-1", "branch-1", "")
			}
		}()
	}

	wg.Wait()
}
