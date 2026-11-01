package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/google/uuid"
)

// GPSLocation represents geographic coordinates captured by a telematics unit.
type GPSLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude,omitempty"`
	Heading   float64 `json:"heading,omitempty"`
}

// Validate checks latitude and longitude within legitimate physical Earth boundaries.
func (g GPSLocation) Validate() error {
	if g.Latitude < -90 || g.Latitude > 90 {
		return fmt.Errorf("%w: latitude %f out of range [-90, 90]", domain.ErrInvalidTelemetry, g.Latitude)
	}
	if g.Longitude < -180 || g.Longitude > 180 {
		return fmt.Errorf("%w: longitude %f out of range [-180, 180]", domain.ErrInvalidTelemetry, g.Longitude)
	}
	return nil
}

// OBDData represents engine parameters and diagnostic codes from an OBD-II interface.
type OBDData struct {
	EngineRPM         int      `json:"engine_rpm"`
	SpeedKmH          float64  `json:"speed_kmh"`
	CoolantTempC      int      `json:"coolant_temp_c,omitempty"`
	FuelLevelPercent  float64  `json:"fuel_level_percent,omitempty"`
	ThrottlePosition  float64  `json:"throttle_position,omitempty"`
	BatteryVoltage    float64  `json:"battery_voltage,omitempty"`
	DTCs              []string `json:"dtcs,omitempty"` // Diagnostic Trouble Codes (e.g., P0300)
}

// Validate ensures physical sensor bounds for automotive values.
func (o OBDData) Validate() error {
	if o.EngineRPM < 0 {
		return fmt.Errorf("%w: engine RPM cannot be negative, got %d", domain.ErrInvalidTelemetry, o.EngineRPM)
	}
	if o.SpeedKmH < 0 {
		return fmt.Errorf("%w: speed cannot be negative, got %f", domain.ErrInvalidTelemetry, o.SpeedKmH)
	}
	if o.FuelLevelPercent < 0 || o.FuelLevelPercent > 100 {
		if o.FuelLevelPercent != 0 {
			return fmt.Errorf("%w: fuel level must be between 0 and 100, got %f", domain.ErrInvalidTelemetry, o.FuelLevelPercent)
		}
	}
	if o.ThrottlePosition < 0 || o.ThrottlePosition > 100 {
		if o.ThrottlePosition != 0 {
			return fmt.Errorf("%w: throttle position must be between 0 and 100, got %f", domain.ErrInvalidTelemetry, o.ThrottlePosition)
		}
	}
	return nil
}

// TelemetryPacket is the domain aggregate representing a validated reading event from a vehicle.
type TelemetryPacket struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	VehicleID string            `json:"vehicle_id"`
	DeviceID  string            `json:"device_id"`
	Timestamp time.Time         `json:"timestamp"`
	Location  GPSLocation       `json:"location"`
	OBD       OBDData           `json:"obd"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// NewTelemetryPacket constructs and validates a new TelemetryPacket.
func NewTelemetryPacket(
	id string,
	tenantID string,
	vehicleID string,
	deviceID string,
	timestamp time.Time,
	location GPSLocation,
	obd OBDData,
	metadata map[string]string,
) (*TelemetryPacket, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(vehicleID) == "" {
		return nil, fmt.Errorf("%w: vehicle_id is required", domain.ErrInvalidTelemetry)
	}
	if strings.TrimSpace(deviceID) == "" {
		return nil, fmt.Errorf("%w: device_id is required", domain.ErrInvalidTelemetry)
	}
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	if err := location.Validate(); err != nil {
		return nil, err
	}
	if err := obd.Validate(); err != nil {
		return nil, err
	}

	if id == "" {
		id = uuid.NewString()
	}

	return &TelemetryPacket{
		ID:        id,
		TenantID:  strings.TrimSpace(tenantID),
		VehicleID: strings.TrimSpace(vehicleID),
		DeviceID:  strings.TrimSpace(deviceID),
		Timestamp: timestamp.UTC(),
		Location:  location,
		OBD:       obd,
		Metadata:  metadata,
	}, nil
}
