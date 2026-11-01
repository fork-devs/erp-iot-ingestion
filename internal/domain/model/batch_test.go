package model

import (
	"errors"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
)

func TestTelemetryBatch_Validate(t *testing.T) {
	now := time.Now().UTC()

	t.Run("valid telemetry batch with non-decreasing odometer", func(t *testing.T) {
		batch := TelemetryBatch{
			TenantID:     "tenant-1",
			OBD2DeviceID: "dev-1",
			Snapshots: []TelemetrySnapshot{
				{
					RPM:              2200,
					Temperature:      90,
					SpeedKmH:         55.5,
					OdometerKm:       25000.0,
					FuelLevelPercent: 65.0,
					CreatedAt:        now,
					DTCCodes: []DTCCodeItem{
						{DTCCode: "P0420", Description: "Catalyst System Efficiency", Severity: "MEDIUM"},
					},
				},
				{
					RPM:              2400,
					Temperature:      92,
					SpeedKmH:         60.0,
					OdometerKm:       25001.2,
					FuelLevelPercent: 64.8,
					CreatedAt:        now.Add(1 * time.Minute),
				},
			},
		}

		if err := batch.Validate(); err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
	})

	t.Run("empty snapshots array fails", func(t *testing.T) {
		batch := TelemetryBatch{
			OBD2DeviceID: "dev-1",
			Snapshots:    []TelemetrySnapshot{},
		}
		if err := batch.Validate(); err == nil || !errors.Is(err, domain.ErrInvalidTelemetry) {
			t.Errorf("expected ErrInvalidTelemetry for empty snapshots, got %v", err)
		}
	})

	t.Run("missing obd2DeviceId fails", func(t *testing.T) {
		batch := TelemetryBatch{
			Snapshots: []TelemetrySnapshot{
				{RPM: 1000, Temperature: 80, SpeedKmH: 20, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now},
			},
		}
		if err := batch.Validate(); err == nil || !errors.Is(err, domain.ErrInvalidTelemetry) {
			t.Errorf("expected ErrInvalidTelemetry for missing obd2DeviceId, got %v", err)
		}
	})

	t.Run("decreasing odometer fails", func(t *testing.T) {
		batch := TelemetryBatch{
			OBD2DeviceID: "dev-1",
			Snapshots: []TelemetrySnapshot{
				{RPM: 1500, Temperature: 85, SpeedKmH: 30, OdometerKm: 50000.0, FuelLevelPercent: 50, CreatedAt: now},
				{RPM: 1500, Temperature: 85, SpeedKmH: 30, OdometerKm: 49999.0, FuelLevelPercent: 50, CreatedAt: now.Add(time.Minute)},
			},
		}
		if err := batch.Validate(); err == nil || !errors.Is(err, domain.ErrInvalidOdometer) {
			t.Errorf("expected ErrInvalidOdometer for decreasing odometer, got %v", err)
		}
	})

	t.Run("invalid sensor boundaries", func(t *testing.T) {
		tests := []struct {
			name     string
			snapshot TelemetrySnapshot
		}{
			{"negative RPM", TelemetrySnapshot{RPM: -10, Temperature: 80, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now}},
			{"excessive RPM", TelemetrySnapshot{RPM: 15000, Temperature: 80, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now}},
			{"extreme cold temp", TelemetrySnapshot{RPM: 1000, Temperature: -50, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now}},
			{"extreme hot temp", TelemetrySnapshot{RPM: 1000, Temperature: 200, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now}},
			{"negative speed", TelemetrySnapshot{RPM: 1000, Temperature: 80, SpeedKmH: -5, OdometerKm: 100, FuelLevelPercent: 50, CreatedAt: now}},
			{"negative fuel level", TelemetrySnapshot{RPM: 1000, Temperature: 80, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: -1, CreatedAt: now}},
			{"fuel level over 100", TelemetrySnapshot{RPM: 1000, Temperature: 80, SpeedKmH: 50, OdometerKm: 100, FuelLevelPercent: 105, CreatedAt: now}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				batch := TelemetryBatch{
					OBD2DeviceID: "dev-1",
					Snapshots:    []TelemetrySnapshot{tt.snapshot},
				}
				if err := batch.Validate(); err == nil {
					t.Errorf("expected error for %s", tt.name)
				}
			})
		}
	})
}
