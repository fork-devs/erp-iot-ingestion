package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
)

// DTCCodeItem represents an OBD-II diagnostic trouble code item with severity.
type DTCCodeItem struct {
	DTCCode     string `json:"dtcCode"`
	Description string `json:"description"`
	Severity    string `json:"severity"` // LOW, MEDIUM, HIGH, CRITICAL
}

// TelemetrySnapshot represents a single periodic reading captured by an OBD2 device.
type TelemetrySnapshot struct {
	RPM              int           `json:"rpm"`
	Temperature      int           `json:"temperature"`
	SpeedKmH         float64       `json:"speedKmh"`
	OdometerKm       float64       `json:"odometerKm"`
	FuelLevelPercent float64       `json:"fuelLevelPercent"`
	CreatedAt        time.Time     `json:"createdAt"`
	DTCCodes         []DTCCodeItem `json:"dtcCodes,omitempty"`
}

// Validate ensures physical validity of sensor boundaries in a snapshot.
func (s TelemetrySnapshot) Validate() error {
	if s.RPM < 0 || s.RPM > 12000 {
		return fmt.Errorf("%w: RPM must be between 0 and 12000, got %d", domain.ErrInvalidTelemetry, s.RPM)
	}
	if s.Temperature < -40 || s.Temperature > 150 {
		return fmt.Errorf("%w: temperature must be between -40 and 150 C, got %d", domain.ErrInvalidTelemetry, s.Temperature)
	}
	if s.SpeedKmH < 0 || s.SpeedKmH > 400 {
		return fmt.Errorf("%w: speed must be between 0 and 400 km/h, got %f", domain.ErrInvalidTelemetry, s.SpeedKmH)
	}
	if s.OdometerKm < 0 {
		return fmt.Errorf("%w: odometer cannot be negative, got %f", domain.ErrInvalidTelemetry, s.OdometerKm)
	}
	if s.FuelLevelPercent < 0 || s.FuelLevelPercent > 100 {
		return fmt.Errorf("%w: fuel level must be between 0 and 100 percent, got %f", domain.ErrInvalidTelemetry, s.FuelLevelPercent)
	}
	if s.CreatedAt.IsZero() {
		return fmt.Errorf("%w: createdAt timestamp is required", domain.ErrInvalidTelemetry)
	}
	return nil
}

// TelemetryBatch represents a payload containing a series of periodic readings from an OBD2 device.
type TelemetryBatch struct {
	TenantID     string              `json:"tenantId,omitempty"`
	OBD2DeviceID string              `json:"obd2DeviceId"`
	VehicleID    string              `json:"vehicleId,omitempty"`
	Snapshots    []TelemetrySnapshot `json:"snapshots"`
}

// Validate verifies batch constraints, sensor boundaries, and non-decreasing odometer monotonicity.
func (b TelemetryBatch) Validate() error {
	if strings.TrimSpace(b.OBD2DeviceID) == "" {
		return fmt.Errorf("%w: obd2DeviceId is required", domain.ErrInvalidTelemetry)
	}
	if len(b.Snapshots) == 0 {
		return fmt.Errorf("%w: snapshots array cannot be empty", domain.ErrInvalidTelemetry)
	}
	if len(b.Snapshots) > 100 {
		return fmt.Errorf("%w: snapshots array exceeds maximum limit of 100, got %d", domain.ErrInvalidTelemetry, len(b.Snapshots))
	}

	var lastOdometer float64 = -1
	for i, s := range b.Snapshots {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("snapshot[%d]: %w", i, err)
		}
		if lastOdometer >= 0 && s.OdometerKm < lastOdometer {
			return fmt.Errorf("%w: snapshot[%d] odometer %f is lower than previous %f", domain.ErrInvalidOdometer, i, s.OdometerKm, lastOdometer)
		}
		lastOdometer = s.OdometerKm
	}

	return nil
}
