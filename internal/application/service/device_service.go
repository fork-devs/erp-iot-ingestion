package service

import (
	"context"
	"fmt"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/usecase"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/port"
)

// DeviceApplicationService implements DeviceUseCase and RegistrationUseCase.
type DeviceApplicationService struct {
	repo port.DeviceRepositoryPort
}

// Compile-time interface checks.
var _ usecase.DeviceUseCase = (*DeviceApplicationService)(nil)
var _ usecase.RegistrationUseCase = (*DeviceApplicationService)(nil)

// NewDeviceApplicationService creates a new DeviceApplicationService.
func NewDeviceApplicationService(repo port.DeviceRepositoryPort) *DeviceApplicationService {
	return &DeviceApplicationService{
		repo: repo,
	}
}

// RegisterDevice registers a new physical OBD2 device with initial AVAILABLE status.
func (s *DeviceApplicationService) RegisterDevice(ctx context.Context, cmd usecase.RegisterDeviceCommand) (*model.OBDDevice, error) {
	device, err := model.NewOBDDevice("", cmd.TenantID, cmd.BranchID, cmd.MACAddress)
	if err != nil {
		return nil, err
	}

	if err := s.repo.SaveDevice(ctx, device); err != nil {
		return nil, err
	}

	return device, nil
}

// GetDevice retrieves an OBD2 device by its identifier within the tenant.
func (s *DeviceApplicationService) GetDevice(ctx context.Context, tenantID, deviceID string) (*model.OBDDevice, error) {
	return s.repo.FindDeviceByID(ctx, tenantID, deviceID)
}

// ListDevices returns devices matching the tenant, branch, and optional status filter.
func (s *DeviceApplicationService) ListDevices(ctx context.Context, tenantID, branchID string, status model.DeviceStatus) ([]*model.OBDDevice, error) {
	return s.repo.ListDevices(ctx, tenantID, branchID, status)
}

// LinkDevice binds an available OBD2 device to a vehicle, enforcing mutual exclusivity rules.
func (s *DeviceApplicationService) LinkDevice(ctx context.Context, cmd usecase.LinkDeviceCommand) (*model.DeviceRegistration, error) {
	device, err := s.repo.FindDeviceByID(ctx, cmd.TenantID, cmd.OBD2DeviceID)
	if err != nil {
		return nil, fmt.Errorf("%w: device %s", domain.ErrDeviceNotFound, cmd.OBD2DeviceID)
	}

	if device.Status != model.DeviceStatusAvailable {
		return nil, fmt.Errorf("%w: device is in status %s", domain.ErrDeviceNotAvailable, device.Status)
	}

	// Verify device does not already have an active registration
	activeDev, err := s.repo.FindActiveRegistrationByDevice(ctx, cmd.TenantID, cmd.OBD2DeviceID)
	if err != nil {
		return nil, err
	}
	if activeDev != nil {
		return nil, fmt.Errorf("%w: device %s already linked", domain.ErrDeviceAlreadyLinked, cmd.OBD2DeviceID)
	}

	// Verify vehicle does not already have an active registration
	activeVeh, err := s.repo.FindActiveRegistrationByVehicle(ctx, cmd.TenantID, cmd.VehicleID)
	if err != nil {
		return nil, err
	}
	if activeVeh != nil {
		return nil, fmt.Errorf("%w: vehicle %s already linked to device %s", domain.ErrVehicleAlreadyLinked, cmd.VehicleID, activeVeh.OBD2DeviceID)
	}

	// Link device and persist
	if err := device.Link(); err != nil {
		return nil, err
	}
	if err := s.repo.SaveDevice(ctx, device); err != nil {
		return nil, err
	}

	registration, err := model.NewDeviceRegistration("", cmd.TenantID, cmd.BranchID, cmd.OBD2DeviceID, cmd.VehicleID)
	if err != nil {
		return nil, err
	}

	if err := s.repo.SaveRegistration(ctx, registration); err != nil {
		return nil, err
	}

	return registration, nil
}

// UpdateRegistrationStatus updates status or unlinks an OBD2 device from a vehicle.
func (s *DeviceApplicationService) UpdateRegistrationStatus(ctx context.Context, cmd usecase.UpdateRegistrationStatusCommand) (*model.DeviceRegistration, error) {
	reg, err := s.repo.FindRegistrationByID(ctx, cmd.TenantID, cmd.RegistrationID)
	if err != nil {
		return nil, fmt.Errorf("%w: registration %s", domain.ErrRegistrationNotFound, cmd.RegistrationID)
	}

	if cmd.Status == model.RegistrationStatusInactive && reg.Status == model.RegistrationStatusActive {
		reg.Deactivate()
		if err := s.repo.UpdateRegistration(ctx, reg); err != nil {
			return nil, err
		}

		// Restore device status to AVAILABLE
		device, err := s.repo.FindDeviceByID(ctx, cmd.TenantID, reg.OBD2DeviceID)
		if err == nil && device != nil {
			device.Unlink()
			_ = s.repo.SaveDevice(ctx, device)
		}
	}

	return reg, nil
}

// ListRegistrations lists bindings for a branch with optional status filter.
func (s *DeviceApplicationService) ListRegistrations(ctx context.Context, tenantID, branchID string, status model.RegistrationStatus) ([]*model.DeviceRegistration, error) {
	return s.repo.ListRegistrations(ctx, tenantID, branchID, status)
}
