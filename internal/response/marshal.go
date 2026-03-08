// Package response provides Tempo-compatible response types and marshaling.
package response

import (
	"encoding/json"
	"net/http"
	"strings"
)

// HeaderAcceptJSON is the MIME type for JSON responses.
const HeaderAcceptJSON = "application/json"

// HeaderAcceptProtobuf is the MIME type for Protobuf responses.
const HeaderAcceptProtobuf = "application/protobuf"

// MarshalingFormat determines the preferred response format from the Accept header.
func MarshalingFormat(r *http.Request) string {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, HeaderAcceptProtobuf) {
		return HeaderAcceptProtobuf
	}
	return HeaderAcceptJSON
}

// WriteJSON writes v as a JSON-encoded HTTP response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteTrace writes a trace response in JSON or protobuf depending on Accept.
func WriteTrace(w http.ResponseWriter, r *http.Request, status int, trace *Trace) error {
	if MarshalingFormat(r) != HeaderAcceptProtobuf {
		WriteJSON(w, status, trace)
		return nil
	}
	if trace == nil {
		trace = &Trace{}
	}
	data, err := MarshalTraceProto(trace)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", HeaderAcceptProtobuf)
	w.WriteHeader(status)
	_, _ = w.Write(data)
	return nil
}

// WriteTraceByIDResponse writes the V2 trace-by-ID envelope in JSON or protobuf.
func WriteTraceByIDResponse(w http.ResponseWriter, r *http.Request, status int, resp *TraceByIDResponse) error {
	if MarshalingFormat(r) != HeaderAcceptProtobuf {
		WriteJSON(w, status, resp)
		return nil
	}
	trace := &Trace{}
	if resp != nil && resp.Trace != nil {
		trace = resp.Trace
	}
	data, err := MarshalTraceByIDResponseProto(trace)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", HeaderAcceptProtobuf)
	w.WriteHeader(status)
	_, _ = w.Write(data)
	return nil
}

// WriteError writes a JSON error response with the given status code and message.
func WriteError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
