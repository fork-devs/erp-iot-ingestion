package handler

import (
	"encoding/json"
	"net/http"
)

// ProblemDetail represents an RFC 9457 error payload.
type ProblemDetail struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}

// WriteProblem writes an RFC 9457 ProblemDetail JSON response.
func WriteProblem(w http.ResponseWriter, status int, title, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ProblemDetail{
		Type:     "https://erp-automotive.pe/errors/" + title,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

// WriteJSON serializes arbitrary data to JSON with specified status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
