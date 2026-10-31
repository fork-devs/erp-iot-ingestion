package memory

import (
	"context"
	"strings"
	"sync"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
)

// MemoryDeviceRepository implements port.DeviceRepositoryPort with thread-safe in-memory maps.
type MemoryDeviceRepository struct {
	mu            sync.RWMutex
	devices       map[string]*model.OBDDevice        // key: tenantID:deviceID
	devicesByMAC  map[string]*model.OBDDevice        // key: tenantID:MAC
	registrations map[string]*model.DeviceRegistration // key: tenantID:regID
}

// Compile-time check for interface compliance.
var _ port.DeviceRepositoryPort = (*MemoryDeviceRepository)(nil)

// NewMemoryDeviceRepository creates an initialized in-memory device repository.
func NewMemoryDeviceRepository() *MemoryDeviceRepository {
	return &MemoryDeviceRepository{
		devices:       make(map[string]*model.OBDDevice),
		devicesByMAC:  make(map[string]*model.OBDDevice),
		registrations: make(map[string]*model.DeviceRegistration),
	}
}

func (r *MemoryDeviceRepository) SaveDevice(ctx context.Context, device *model.OBDDevice) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	macKey := device.TenantID + ":" + strings.ToUpper(device.MACAddress)
	if existing, ok := r.devicesByMAC[macKey]; ok && existing.ID != device.ID {
		return domain.ErrDeviceConflict
	}

	devCopy := *device
	idKey := device.TenantID + ":" + device.ID
	r.devices[idKey] = &devCopy
	r.devicesByMAC[macKey] = &devCopy
	return nil
}

func (r *MemoryDeviceRepository) FindDeviceByID(ctx context.Context, tenantID, deviceID string) (*model.OBDDevice, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := tenantID + ":" + deviceID
	dev, ok := r.devices[key]
	if !ok {
		return nil, domain.ErrDeviceNotFound
	}
	devCopy := *dev
	return &devCopy, nil
}

func (r *MemoryDeviceRepository) FindDeviceByMAC(ctx context.Context, tenantID, mac string) (*model.OBDDevice, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := tenantID + ":" + strings.ToUpper(mac)
	dev, ok := r.devicesByMAC[key]
	if !ok {
		return nil, domain.ErrDeviceNotFound
	}
	devCopy := *dev
	return &devCopy, nil
}

func (r *MemoryDeviceRepository) ListDevices(ctx context.Context, tenantID, branchID string, status model.DeviceStatus) ([]*model.OBDDevice, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*model.OBDDevice
	for _, dev := range r.devices {
		if dev.TenantID != tenantID {
			continue
		}
		if branchID != "" && dev.BranchID != branchID {
			continue
		}
		if status != "" && dev.Status != status {
			continue
		}
		devCopy := *dev
		result = append(result, &devCopy)
	}
	return result, nil
}

func (r *MemoryDeviceRepository) SaveRegistration(ctx context.Context, reg *model.DeviceRegistration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	regCopy := *reg
	key := reg.TenantID + ":" + reg.ID
	r.registrations[key] = &regCopy
	return nil
}

func (r *MemoryDeviceRepository) FindRegistrationByID(ctx context.Context, tenantID, regID string) (*model.DeviceRegistration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := tenantID + ":" + regID
	reg, ok := r.registrations[key]
	if !ok {
		return nil, domain.ErrRegistrationNotFound
	}
	regCopy := *reg
	return &regCopy, nil
}

func (r *MemoryDeviceRepository) FindActiveRegistrationByDevice(ctx context.Context, tenantID, deviceID string) (*model.DeviceRegistration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, reg := range r.registrations {
		if reg.TenantID == tenantID && reg.OBD2DeviceID == deviceID && reg.Status == model.RegistrationStatusActive {
			regCopy := *reg
			return &regCopy, nil
		}
	}
	return nil, nil
}

func (r *MemoryDeviceRepository) FindActiveRegistrationByVehicle(ctx context.Context, tenantID, vehicleID string) (*model.DeviceRegistration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, reg := range r.registrations {
		if reg.TenantID == tenantID && reg.VehicleID == vehicleID && reg.Status == model.RegistrationStatusActive {
			regCopy := *reg
			return &regCopy, nil
		}
	}
	return nil, nil
}

func (r *MemoryDeviceRepository) ListRegistrations(ctx context.Context, tenantID, branchID string, status model.RegistrationStatus) ([]*model.DeviceRegistration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*model.DeviceRegistration
	for _, reg := range r.registrations {
		if reg.TenantID != tenantID {
			continue
		}
		if branchID != "" && reg.BranchID != branchID {
			continue
		}
		if status != "" && reg.Status != status {
			continue
		}
		regCopy := *reg
		result = append(result, &regCopy)
	}
	return result, nil
}

func (r *MemoryDeviceRepository) UpdateRegistration(ctx context.Context, reg *model.DeviceRegistration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := reg.TenantID + ":" + reg.ID
	if _, ok := r.registrations[key]; !ok {
		return domain.ErrRegistrationNotFound
	}
	regCopy := *reg
	r.registrations[key] = &regCopy
	return nil
}
