package model

import (
	"errors"
	"testing"

	"github.com/fork-devs/erp-iot-ingestion/internal/domain"
)

func TestValidateMAC_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		mac     string
		wantErr bool
	}{
		{
			name:    "valid standard MAC with colons",
			mac:     "00:1B:44:11:3A:B7",
			wantErr: false,
		},
		{
			name:    "valid standard MAC with hyphens",
			mac:     "00-1B-44-11-3A-B7",
			wantErr: false,
		},
		{
			name:    "valid lowercase MAC",
			mac:     "aa:bb:cc:dd:ee:ff",
			wantErr: false,
		},
		{
			name:    "empty string fails",
			mac:     "",
			wantErr: true,
		},
		{
			name:    "invalid characters fails",
			mac:     "00:1B:44:11:3A:ZZ",
			wantErr: true,
		},
		{
			name:    "too few octets fails",
			mac:     "00:1B:44:11:3A",
			wantErr: true,
		},
		{
			name:    "too many octets fails",
			mac:     "00:1B:44:11:3A:B7:C8",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMAC(tt.mac)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMAC(%q) error = %v, wantErr %v", tt.mac, err, tt.wantErr)
			}
		})
	}
}

func TestNewOBDDevice_Validation(t *testing.T) {
	t.Run("valid device creation", func(t *testing.T) {
		dev, err := NewOBDDevice("", "tenant-1", "branch-1", "00:1B:44:11:3A:B7")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dev.ID == "" {
			t.Errorf("expected auto-generated ID")
		}
		if dev.Status != DeviceStatusAvailable {
			t.Errorf("expected status %s, got %s", DeviceStatusAvailable, dev.Status)
		}
		if dev.MACAddress != "00:1B:44:11:3A:B7" {
			t.Errorf("expected uppercase MAC, got %s", dev.MACAddress)
		}
	})

	t.Run("missing tenant fails", func(t *testing.T) {
		_, err := NewOBDDevice("", "", "branch-1", "00:1B:44:11:3A:B7")
		if err == nil || !errors.Is(err, domain.ErrInvalidTelemetry) {
			t.Errorf("expected ErrInvalidTelemetry, got %v", err)
		}
	})

	t.Run("missing branch fails", func(t *testing.T) {
		_, err := NewOBDDevice("", "tenant-1", "", "00:1B:44:11:3A:B7")
		if err == nil || !errors.Is(err, domain.ErrInvalidTelemetry) {
			t.Errorf("expected ErrInvalidTelemetry, got %v", err)
		}
	})

	t.Run("invalid MAC fails", func(t *testing.T) {
		_, err := NewOBDDevice("", "tenant-1", "branch-1", "invalid-mac")
		if err == nil || !errors.Is(err, domain.ErrInvalidMACAddress) {
			t.Errorf("expected ErrInvalidMACAddress, got %v", err)
		}
	})

	t.Run("link and unlink lifecycle", func(t *testing.T) {
		dev, err := NewOBDDevice("dev-1", "tenant-1", "branch-1", "00:1B:44:11:3A:B7")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := dev.Link(); err != nil {
			t.Fatalf("failed to link device: %v", err)
		}
		if dev.Status != DeviceStatusLinked {
			t.Errorf("expected status LINKED, got %s", dev.Status)
		}

		// Linking already linked device fails
		if err := dev.Link(); err == nil || !errors.Is(err, domain.ErrDeviceNotAvailable) {
			t.Errorf("expected ErrDeviceNotAvailable when linking already linked device, got %v", err)
		}

		dev.Unlink()
		if dev.Status != DeviceStatusAvailable {
			t.Errorf("expected status AVAILABLE after unlink, got %s", dev.Status)
		}
	})
}

func TestNewDeviceRegistration_Validation(t *testing.T) {
	t.Run("valid registration", func(t *testing.T) {
		reg, err := NewDeviceRegistration("", "tenant-1", "branch-1", "dev-1", "veh-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reg.Status != RegistrationStatusActive {
			t.Errorf("expected status ACTIVE, got %s", reg.Status)
		}

		reg.Deactivate()
		if reg.Status != RegistrationStatusInactive {
			t.Errorf("expected status INACTIVE after deactivation, got %s", reg.Status)
		}
	})

	t.Run("missing parameters fail", func(t *testing.T) {
		_, err := NewDeviceRegistration("", "", "b", "d", "v")
		if err == nil {
			t.Errorf("expected error for empty tenant")
		}
		_, err = NewDeviceRegistration("", "t", "", "d", "v")
		if err == nil {
			t.Errorf("expected error for empty branch")
		}
		_, err = NewDeviceRegistration("", "t", "b", "", "v")
		if err == nil {
			t.Errorf("expected error for empty device")
		}
		_, err = NewDeviceRegistration("", "t", "b", "d", "")
		if err == nil {
			t.Errorf("expected error for empty vehicle")
		}
	})
}
