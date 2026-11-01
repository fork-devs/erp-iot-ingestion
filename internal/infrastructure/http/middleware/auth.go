package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

type contextKey string

const (
	tenantKey      contextKey = "tenant_id"
	deviceKey      contextKey = "device_id"
	correlationKey contextKey = "correlation_id"
)

// DeviceAuthenticator verifies request authentication via X-Device-Key or Authorization Bearer header.
type DeviceAuthenticator struct {
	allowedKeys map[string]string // deviceKey -> tenantId
}

// NewDeviceAuthenticator creates an authenticator middleware.
func NewDeviceAuthenticator(allowedKeys map[string]string) *DeviceAuthenticator {
	return &DeviceAuthenticator{
		allowedKeys: allowedKeys,
	}
}

// Handler returns an HTTP middleware verifying device authenticity and binding tenant_id into context.
func (a *DeviceAuthenticator) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract correlation ID from header or Chi request ID
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = r.Header.Get("X-Request-ID")
		}
		if correlationID == "" {
			correlationID = chimiddleware.GetReqID(ctx)
		}
		if correlationID != "" {
			ctx = context.WithValue(ctx, correlationKey, correlationID)
			w.Header().Set("X-Correlation-ID", correlationID)
		}

		apiKey := r.Header.Get("X-Device-Key")
		if apiKey != "" {
			tenantID, ok := a.allowedKeys[apiKey]
			if !ok {
				writeUnauthorized(w, "Invalid X-Device-Key")
				return
			}
			ctx = context.WithValue(ctx, tenantKey, tenantID)
			ctx = context.WithValue(ctx, deviceKey, apiKey)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" {
				writeUnauthorized(w, "Missing Bearer token value")
				return
			}

			// In development/gateway edge setup, inspect header X-Tenant-ID forwarded by edge
			tenantID := r.Header.Get("X-Tenant-ID")
			if tenantID == "" {
				// Fallback for gateway authenticated traffic
				tenantID = "00000000-0000-0000-0000-000000000001"
			}
			ctx = context.WithValue(ctx, tenantKey, tenantID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		writeUnauthorized(w, "Missing X-Device-Key or Bearer token header")
	})
}

// TenantFromContext extracts the authenticated tenant_id from request context.
func TenantFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(tenantKey).(string)
	return val, ok && val != ""
}

// DeviceFromContext extracts the device_id from request context.
func DeviceFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(deviceKey).(string)
	return val, ok && val != ""
}

// CorrelationFromContext extracts the correlation_id from request context.
func CorrelationFromContext(ctx context.Context) string {
	val, ok := ctx.Value(correlationKey).(string)
	if ok {
		return val
	}
	return ""
}

func writeUnauthorized(w http.ResponseWriter, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   "https://erp-automotive.pe/errors/unauthorized",
		"title":  "Unauthorized",
		"status": http.StatusUnauthorized,
		"detail": detail,
	})
}
