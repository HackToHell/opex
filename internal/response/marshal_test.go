package response

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMarshalingFormat(t *testing.T) {
	tests := []struct {
		name   string
		accept string
		want   string
	}{
		{
			name:   "no accept header",
			accept: "",
			want:   HeaderAcceptJSON,
		},
		{
			name:   "json accept",
			accept: "application/json",
			want:   HeaderAcceptJSON,
		},
		{
			name:   "protobuf accept",
			accept: "application/protobuf",
			want:   HeaderAcceptProtobuf,
		},
		{
			name:   "mixed with protobuf",
			accept: "application/json, application/protobuf",
			want:   HeaderAcceptProtobuf,
		},
		{
			name:   "text/html fallback to json",
			accept: "text/html",
			want:   HeaderAcceptJSON,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			got := MarshalingFormat(req)
			if got != tc.want {
				t.Errorf("MarshalingFormat() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]string{"hello": "world"}

	WriteJSON(w, http.StatusOK, data)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", resp.Header.Get("Content-Type"))
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["hello"] != "world" {
		t.Errorf("expected hello=world, got %q", result["hello"])
	}
}

func TestWriteJSON_CustomStatus(t *testing.T) {
	w := httptest.NewRecorder()
	WriteJSON(w, http.StatusCreated, map[string]int{"count": 42})

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d", resp.StatusCode)
	}
}

func TestWriteTrace_Protobuf(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", HeaderAcceptProtobuf)
	w := httptest.NewRecorder()

	trace := &Trace{}
	want, err := MarshalTraceProto(trace)
	if err != nil {
		t.Fatalf("MarshalTraceProto() error: %v", err)
	}
	if err := WriteTrace(w, req, http.StatusOK, trace); err != nil {
		t.Fatalf("WriteTrace() error: %v", err)
	}

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != HeaderAcceptProtobuf {
		t.Errorf("expected Content-Type %q, got %q", HeaderAcceptProtobuf, resp.Header.Get("Content-Type"))
	}
	if !bytes.Equal(w.Body.Bytes(), want) {
		t.Error("WriteTrace() did not write expected protobuf payload")
	}
}

func TestWriteTraceByIDResponse_Protobuf(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", HeaderAcceptProtobuf)
	w := httptest.NewRecorder()

	respBody := &TraceByIDResponse{Trace: &Trace{}, Status: "complete"}
	want, err := MarshalTraceByIDResponseProto(respBody.Trace)
	if err != nil {
		t.Fatalf("MarshalTraceByIDResponseProto() error: %v", err)
	}
	if err := WriteTraceByIDResponse(w, req, http.StatusOK, respBody); err != nil {
		t.Fatalf("WriteTraceByIDResponse() error: %v", err)
	}

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != HeaderAcceptProtobuf {
		t.Errorf("expected Content-Type %q, got %q", HeaderAcceptProtobuf, resp.Header.Get("Content-Type"))
	}
	if !bytes.Equal(w.Body.Bytes(), want) {
		t.Error("WriteTraceByIDResponse() did not write expected protobuf payload")
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	WriteError(w, http.StatusBadRequest, "invalid query")

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", resp.Header.Get("Content-Type"))
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if result["error"] != "invalid query" {
		t.Errorf("expected error='invalid query', got %q", result["error"])
	}
}

func TestWriteError_InternalServerError(t *testing.T) {
	w := httptest.NewRecorder()
	WriteError(w, http.StatusInternalServerError, "something went wrong")

	resp := w.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}
