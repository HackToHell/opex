package tracequery

import (
	"testing"
)

func TestNormalizeTraceID(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"AABBCCDD", "aabbccdd"},
		{"aa-bb-cc-dd", "aabbccdd"},
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"AA-BB-CC-DD-EE-FF-00-11-22-33-44-55-66-77-88-99", "aabbccddeeff00112233445566778899"},
	}
	for _, tc := range tests {
		got := NormalizeTraceID(tc.input)
		if got != tc.want {
			t.Errorf("NormalizeTraceID(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestIsValidHexTraceID(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},  // 32 hex
		{"aaaaaaaaaaaaaaaa", true},                  // 16 hex
		{"abc", false},                              // too short
		{"gggggggggggggggg", false},                 // invalid hex
		{"aabbccddeeff00112233445566778899", true},  // 32 hex
		{"", false},                                 // empty
		{"aabbccddeeff001122334455667788990", false}, // 33 chars
	}
	for _, tc := range tests {
		got := IsValidHexTraceID(tc.input)
		if got != tc.valid {
			t.Errorf("IsValidHexTraceID(%q) = %v, want %v", tc.input, got, tc.valid)
		}
	}
}

func TestIsInputError(t *testing.T) {
	ie := newInputError(nil)
	if !IsInputError(ie) {
		t.Error("expected IsInputError to return true for InputError")
	}

	if IsInputError(nil) {
		t.Error("expected IsInputError to return false for nil")
	}

	regular := &InputError{Err: nil}
	if !IsInputError(regular) {
		t.Error("expected IsInputError to return true for *InputError")
	}
}

func TestEscapeSQL(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"hello", "hello"},
		{"it's", "it\\'s"},
		{`back\slash`, `back\\slash`},
		{"'; DROP TABLE--", "\\'; DROP TABLE--"},
	}
	for _, tc := range tests {
		got := escapeSQL(tc.input)
		if got != tc.want {
			t.Errorf("escapeSQL(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
