package model_test

import (
	"errors"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
	"github.com/fork-devs/erp-iot-ingestion/internal/domain/model"
)

func TestNewTelemetryPacket_TableDriven(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name        string
		tenantID    string
		vehicleID   string
		deviceID    string
		location    model.GPSLocation
		obd         model.OBDData
		expectErr   bool
		expectedErr error
	}{
		{
			name:      "valid telemetry packet",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.046374,
				Longitude: -77.042793,
				Altitude:  150.5,
				Heading:   90.0,
			},
			obd: model.OBDData{
				EngineRPM:        2500,
				SpeedKmH:         65.5,
				CoolantTempC:     88,
				FuelLevelPercent: 75.0,
				ThrottlePosition: 22.0,
				BatteryVoltage:   12.6,
				DTCs:             []string{"P0300"},
			},
			expectErr: false,
		},
		{
			name:      "missing tenant_id",
			tenantID:  "",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -77.0,
			},
			obd:         model.OBDData{EngineRPM: 1000, SpeedKmH: 20},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "missing vehicle_id",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -77.0,
			},
			obd:         model.OBDData{EngineRPM: 1000, SpeedKmH: 20},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "missing device_id",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "   ",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -77.0,
			},
			obd:         model.OBDData{EngineRPM: 1000, SpeedKmH: 20},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "invalid latitude above 90",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  95.0,
				Longitude: -77.0,
			},
			obd:         model.OBDData{EngineRPM: 1000, SpeedKmH: 20},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "invalid longitude below -180",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -185.0,
			},
			obd:         model.OBDData{EngineRPM: 1000, SpeedKmH: 20},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "negative speed",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -77.0,
			},
			obd: model.OBDData{
				EngineRPM: 1000,
				SpeedKmH:  -5.0,
			},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
		{
			name:      "negative engine RPM",
			tenantID:  "00000000-0000-0000-0000-000000000001",
			vehicleID: "V-12345",
			deviceID:  "DEV-001",
			location: model.GPSLocation{
				Latitude:  -12.0,
				Longitude: -77.0,
			},
			obd: model.OBDData{
				EngineRPM: -100,
				SpeedKmH:  0,
			},
			expectErr:   true,
			expectedErr: domain.ErrInvalidTelemetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet, err := model.NewTelemetryPacket(
				"",
				tt.tenantID,
				tt.vehicleID,
				tt.deviceID,
				now,
				tt.location,
				tt.obd,
				nil,
			)

			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.expectedErr != nil && !errors.Is(err, tt.expectedErr) {
					t.Errorf("expected error %v, got %v", tt.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if packet.ID == "" {
					t.Errorf("expected generated UUID, got empty")
				}
				if packet.VehicleID != tt.vehicleID {
					t.Errorf("expected VehicleID %s, got %s", tt.vehicleID, packet.VehicleID)
				}
			}
		})
	}
}
