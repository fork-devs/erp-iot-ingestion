package bdd_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// RestTestContext encapsulates HTTP requests and assertions for Go BDD acceptance tests.
type RestTestContext struct {
	BaseURL      string
	Client       *http.Client
	Headers      map[string]string
	LastStatus   int
	LastBody     []byte
}

func NewRestTestContext(baseURL string) *RestTestContext {
	return &RestTestContext{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Client:  &http.Client{},
		Headers: map[string]string{"Content-Type": "application/json"},
	}
}

func (c *RestTestContext) Send(t *testing.T, method, path string, payload any) {
	t.Helper()
	var bodyReader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("failed to marshal payload: %v", err)
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.BaseURL+path, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()
	c.LastStatus = resp.StatusCode
	c.LastBody, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
}

func TestFeatureFilesExistAndAreValidGherkin(t *testing.T) {
	matches, err := filepath.Glob("features/*.feature")
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("expected at least one .feature file in tests/bdd/features")
	}
	for _, file := range matches {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("failed reading %s: %v", file, err)
		}
		content := string(raw)
		for _, kw := range []string{"Feature:", "Scenario:", "Given ", "When ", "Then "} {
			if !strings.Contains(content, kw) {
				t.Errorf("%s missing Gherkin keyword %q", file, kw)
			}
		}
	}
}
