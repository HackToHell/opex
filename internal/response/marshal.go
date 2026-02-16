package response

import (
	"encoding/json"
	"net/http"
	"strings"
)

// MIME type constants for content negotiation.
const (
	// HeaderAcceptJSON is the MIME type for JSON responses.
	HeaderAcceptJSON = "application/json"
	// HeaderAcceptProtobuf is the MIME type for Protobuf responses.
	HeaderAcceptProtobuf = "application/protobuf"
)

// MarshalingFormat determines the response format from the Accept header.
func MarshalingFormat(r *http.Request) string {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, HeaderAcceptProtobuf) {
		return HeaderAcceptProtobuf
	}
	return HeaderAcceptJSON
}

// WriteJSON writes a JSON response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes a JSON error response.
func WriteError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
