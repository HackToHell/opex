package clickhouse

import (
	"testing"
	"time"
)

func TestCircuitBreaker_StartsInClosedState(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second)
	if cb.State() != CircuitClosed {
		t.Errorf("initial state = %v, want CircuitClosed", cb.State())
	}
	if !cb.Allow() {
		t.Error("Allow() = false in closed state, want true")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second)

	// Record failures up to threshold
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Errorf("state after 1 failure = %v, want CircuitClosed", cb.State())
	}

	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Errorf("state after 2 failures = %v, want CircuitClosed", cb.State())
	}

	cb.RecordFailure() // threshold reached
	if cb.State() != CircuitOpen {
		t.Errorf("state after 3 failures = %v, want CircuitOpen", cb.State())
	}

	if cb.Allow() {
		t.Error("Allow() = true in open state (before timeout), want false")
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // reset

	cb.RecordFailure()
	cb.RecordFailure()
	// 2 failures after reset — still below threshold of 3
	if cb.State() != CircuitClosed {
		t.Errorf("state = %v, want CircuitClosed (success should have reset count)", cb.State())
	}
}

func TestCircuitBreaker_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(2, 100*time.Millisecond)

	// Use controllable time
	now := time.Now()
	cb.now = func() time.Time { return now }

	cb.RecordFailure()
	cb.RecordFailure() // opens circuit
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}

	// Advance time past the timeout
	now = now.Add(150 * time.Millisecond)

	if cb.State() != CircuitHalfOpen {
		t.Errorf("state after timeout = %v, want CircuitHalfOpen", cb.State())
	}

	if !cb.Allow() {
		t.Error("Allow() = false in half-open state, want true (probe request)")
	}
}

func TestCircuitBreaker_HalfOpenToClosedOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(2, 100*time.Millisecond)

	now := time.Now()
	cb.now = func() time.Time { return now }

	cb.RecordFailure()
	cb.RecordFailure()

	// Advance past timeout to enter half-open
	now = now.Add(150 * time.Millisecond)
	_ = cb.State() // triggers transition to half-open

	// Probe succeeds
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("state after success in half-open = %v, want CircuitClosed", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToOpenOnFailure(t *testing.T) {
	cb := NewCircuitBreaker(2, 100*time.Millisecond)

	now := time.Now()
	cb.now = func() time.Time { return now }

	cb.RecordFailure()
	cb.RecordFailure()

	// Advance past timeout to enter half-open
	now = now.Add(150 * time.Millisecond)
	_ = cb.Allow() // triggers transition to half-open

	// Probe fails
	cb.RecordFailure()
	cb.RecordFailure() // back over threshold
	if cb.State() != CircuitOpen {
		t.Errorf("state after failure in half-open = %v, want CircuitOpen", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}

	cb.Reset()
	if cb.State() != CircuitClosed {
		t.Errorf("state after Reset = %v, want CircuitClosed", cb.State())
	}
	if !cb.Allow() {
		t.Error("Allow() = false after Reset, want true")
	}
}

func TestCircuitBreaker_DoesNotOpenBelowThreshold(t *testing.T) {
	cb := NewCircuitBreaker(5, 10*time.Second)

	for i := 0; i < 4; i++ {
		cb.RecordFailure()
	}

	if cb.State() != CircuitClosed {
		t.Errorf("state after 4 failures (threshold=5) = %v, want CircuitClosed", cb.State())
	}
	if !cb.Allow() {
		t.Error("Allow() = false below threshold, want true")
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state    CircuitState
		expected string
	}{
		{CircuitClosed, "closed"},
		{CircuitHalfOpen, "half-open"},
		{CircuitOpen, "open"},
		{CircuitState(99), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			if tc.state.String() != tc.expected {
				t.Errorf("CircuitState(%d).String() = %q, want %q", tc.state, tc.state.String(), tc.expected)
			}
		})
	}
}
