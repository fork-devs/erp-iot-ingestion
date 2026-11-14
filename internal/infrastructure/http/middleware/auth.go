package middleware

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type contextKey string

const (
	tenantKey      contextKey = "tenant_id"
	deviceKey      contextKey = "device_id"
	correlationKey contextKey = "correlation_id"
)

// AuthOption configures DeviceAuthenticator behavior.
type AuthOption func(*DeviceAuthenticator)

// WithJWKSURL configures the JWKS URL used to verify Bearer RS256 JWTs.
func WithJWKSURL(jwksURL string) AuthOption {
	return func(a *DeviceAuthenticator) {
		a.jwksURL = strings.TrimSpace(jwksURL)
	}
}

// WithStaticPublicKeys injects static RSA public keys by kid (primarily for unit tests).
func WithStaticPublicKeys(keys map[string]*rsa.PublicKey) AuthOption {
	return func(a *DeviceAuthenticator) {
		a.mu.Lock()
		defer a.mu.Unlock()
		for k, v := range keys {
			a.cachedKeys[k] = v
		}
		a.cacheExpiry = time.Now().Add(24 * time.Hour)
	}
}

// DeviceAuthenticator verifies request authentication via X-Device-Key or Authorization Bearer JWT.
type DeviceAuthenticator struct {
	allowedKeys map[string]string // deviceKey -> tenantId
	jwksURL     string
	httpClient  *http.Client
	mu          sync.RWMutex
	cachedKeys  map[string]*rsa.PublicKey
	cacheExpiry time.Time
}

// NewDeviceAuthenticator creates an authenticator middleware.
func NewDeviceAuthenticator(allowedKeys map[string]string, opts ...AuthOption) *DeviceAuthenticator {
	a := &DeviceAuthenticator{
		allowedKeys: allowedKeys,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		cachedKeys:  make(map[string]*rsa.PublicKey),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

// Handler returns an HTTP middleware verifying device authenticity or JWT and binding tenant_id into context.
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
			parsedTenant, err := parseNonNilUUID(tenantID)
			if err != nil {
				writeUnauthorized(w, "Invalid tenant configuration for device key")
				return
			}
			ctx = context.WithValue(ctx, tenantKey, parsedTenant.String())
			ctx = context.WithValue(ctx, deviceKey, apiKey)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
			if token == "" {
				writeUnauthorized(w, "Missing Bearer token value")
				return
			}

			tokenTenantID, err := a.verifyRS256JWT(r.Context(), token)
			if err != nil {
				writeUnauthorized(w, "Invalid or expired Bearer token")
				return
			}

			// Cross-check optional X-Tenant-ID header against verified JWT tenant_id
			if headerTenant := strings.TrimSpace(r.Header.Get("X-Tenant-ID")); headerTenant != "" {
				parsedHeader, err := parseNonNilUUID(headerTenant)
				if err != nil || parsedHeader.String() != tokenTenantID {
					writeForbidden(w, "X-Tenant-ID header does not match authenticated token tenant")
					return
				}
			}

			ctx = context.WithValue(ctx, tenantKey, tokenTenantID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		writeUnauthorized(w, "Missing X-Device-Key or Bearer token header")
	})
}

func (a *DeviceAuthenticator) verifyRS256JWT(ctx context.Context, rawToken string) (string, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return "", errors.New("malformed jwt")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", err
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", err
	}
	if header.Alg != "RS256" {
		return "", fmt.Errorf("unsupported alg: %s", header.Alg)
	}

	pubKey, err := a.getPublicKey(ctx, header.Kid)
	if err != nil {
		return "", err
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", err
	}

	signingInput := parts[0] + "." + parts[1]
	digest := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, digest[:], sigBytes); err != nil {
		return "", err
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", err
	}

	expVal, ok := claims["exp"].(float64)
	if !ok || time.Now().Unix() >= int64(expVal) {
		return "", errors.New("token expired or missing exp")
	}
	subVal, ok := claims["sub"].(string)
	if !ok || strings.TrimSpace(subVal) == "" {
		return "", errors.New("missing sub claim")
	}
	if typeVal, hasType := claims["type"]; hasType {
		if typeStr, ok := typeVal.(string); !ok || typeStr != "access" {
			return "", errors.New("invalid token type")
		}
	}

	rawTenant, ok := claims["tenant_id"].(string)
	if !ok {
		return "", errors.New("missing tenant_id claim")
	}
	parsedTenant, err := parseNonNilUUID(rawTenant)
	if err != nil {
		return "", err
	}
	return parsedTenant.String(), nil
}

func (a *DeviceAuthenticator) getPublicKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	a.mu.RLock()
	if time.Now().Before(a.cacheExpiry) {
		if key, ok := a.cachedKeys[kid]; ok {
			a.mu.RUnlock()
			return key, nil
		}
		if kid == "" && len(a.cachedKeys) == 1 {
			for _, k := range a.cachedKeys {
				a.mu.RUnlock()
				return k, nil
			}
		}
	}
	a.mu.RUnlock()

	if a.jwksURL == "" {
		return nil, errors.New("jwks url not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.jwksURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks endpoint returned status %d", resp.StatusCode)
	}

	var jwks struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, err
	}

	newKeys := make(map[string]*rsa.PublicKey)
	for _, jwk := range jwks.Keys {
		if jwk.Kty != "RSA" || jwk.N == "" || jwk.E == "" {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
		if err != nil {
			continue
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
		if err != nil {
			continue
		}
		n := new(big.Int).SetBytes(nBytes)
		e := int(new(big.Int).SetBytes(eBytes).Int64())
		if e <= 0 {
			continue
		}
		newKeys[jwk.Kid] = &rsa.PublicKey{N: n, E: e}
	}

	a.mu.Lock()
	a.cachedKeys = newKeys
	a.cacheExpiry = time.Now().Add(5 * time.Minute)
	a.mu.Unlock()

	if key, ok := newKeys[kid]; ok {
		return key, nil
	}
	if kid == "" && len(newKeys) == 1 {
		for _, k := range newKeys {
			return k, nil
		}
	}
	return nil, fmt.Errorf("signing key not found for kid %q", kid)
}

func parseNonNilUUID(raw string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return uuid.Nil, err
	}
	if parsed == uuid.Nil {
		return uuid.Nil, errors.New("nil uuid is not permitted")
	}
	return parsed, nil
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

func writeForbidden(w http.ResponseWriter, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   "https://erp-automotive.pe/errors/forbidden",
		"title":  "Forbidden",
		"status": http.StatusForbidden,
		"detail": detail,
	})
}
