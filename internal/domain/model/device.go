package model

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/google/uuid"
)

var macRegex = regexp.MustCompile(`^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$`)

// DeviceStatus represents the operational status of an OBD2 dongle.
type DeviceStatus string

const (
	DeviceStatusAvailable      DeviceStatus = "AVAILABLE"
	DeviceStatusLinked         DeviceStatus = "LINKED"
	DeviceStatusDecommissioned DeviceStatus = "DECOMMISSIONED"
)

// RegistrationStatus represents the active link between an OBD2 dongle and a vehicle.
type RegistrationStatus string

const (
	RegistrationStatusActive   RegistrationStatus = "ACTIVE"
	RegistrationStatusInactive RegistrationStatus = "INACTIVE"
)

// ValidateMAC validates whether a string matches a standard IEEE 802 MAC address format.
func ValidateMAC(mac string) error {
	trimmed := strings.TrimSpace(mac)
	if trimmed == "" {
		return fmt.Errorf("%w: MAC address cannot be empty", domain.ErrInvalidMACAddress)
	}
	if !macRegex.MatchString(trimmed) {
		return fmt.Errorf("%w: '%s' is not a valid MAC format", domain.ErrInvalidMACAddress, mac)
	}
	return nil
}

// OBDDevice represents a physical OBD2 telematics scanner dongle in a workshop branch.
type OBDDevice struct {
	ID         string       `json:"id"`
	TenantID   string       `json:"tenantId,omitempty"`
	BranchID   string       `json:"branchId"`
	MACAddress string       `json:"macAddress"`
	Status     DeviceStatus `json:"status"`
	CreatedAt  time.Time    `json:"createdAt"`
	UpdatedAt  time.Time    `json:"updatedAt"`
}

// NewOBDDevice constructs and validates an OBDDevice instance.
func NewOBDDevice(id, tenantID, branchID, macAddress string) (*OBDDevice, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenantId is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(branchID) == "" {
		return nil, fmt.Errorf("%w: branchId is required", domain.ErrInvalidTelemetry)
	}
	if err := ValidateMAC(macAddress); err != nil {
		return nil, err
	}

	if id == "" {
		id = uuid.NewString()
	}

	now := time.Now().UTC()
	return &OBDDevice{
		ID:         id,
		TenantID:   tenantID,
		BranchID:   branchID,
		MACAddress: strings.ToUpper(strings.TrimSpace(macAddress)),
		Status:     DeviceStatusAvailable,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

// Link marks the device as LINKED to a vehicle.
func (d *OBDDevice) Link() error {
	if d.Status != DeviceStatusAvailable {
		return fmt.Errorf("%w: cannot link device in status %s", domain.ErrDeviceNotAvailable, d.Status)
	}
	d.Status = DeviceStatusLinked
	d.UpdatedAt = time.Now().UTC()
	return nil
}

// Unlink restores the device status back to AVAILABLE.
func (d *OBDDevice) Unlink() {
	d.Status = DeviceStatusAvailable
	d.UpdatedAt = time.Now().UTC()
}

// DeviceRegistration represents the binding of an OBD2 device to a specific vehicle.
type DeviceRegistration struct {
	ID           string             `json:"id"`
	TenantID     string             `json:"tenantId,omitempty"`
	BranchID     string             `json:"branchId"`
	OBD2DeviceID string             `json:"obd2DeviceId"`
	VehicleID    string             `json:"vehicleId"`
	Status       RegistrationStatus `json:"status"`
	CreatedAt    time.Time          `json:"createdAt"`
	UpdatedAt    time.Time          `json:"updatedAt"`
}

// NewDeviceRegistration creates a new active device-to-vehicle binding.
func NewDeviceRegistration(id, tenantID, branchID, obd2DeviceID, vehicleID string) (*DeviceRegistration, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenantId is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(branchID) == "" {
		return nil, fmt.Errorf("%w: branchId is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(obd2DeviceID) == "" {
		return nil, fmt.Errorf("%w: obd2DeviceId is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(vehicleID) == "" {
		return nil, fmt.Errorf("%w: vehicleId is required", domain.ErrInvalidTelemetry)
	}

	if id == "" {
		id = uuid.NewString()
	}

	now := time.Now().UTC()
	return &DeviceRegistration{
		ID:           id,
		TenantID:     tenantID,
		BranchID:     branchID,
		OBD2DeviceID: obd2DeviceID,
		VehicleID:    vehicleID,
		Status:       RegistrationStatusActive,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

// Deactivate transitions an active registration to INACTIVE.
func (r *DeviceRegistration) Deactivate() {
	r.Status = RegistrationStatusInactive
	r.UpdatedAt = time.Now().UTC()
}
