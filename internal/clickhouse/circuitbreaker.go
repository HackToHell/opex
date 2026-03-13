package clickhouse

import (
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Circuit breaker states
// ---------------------------------------------------------------------------

// CircuitState represents the current state of the circuit breaker.
type CircuitState int

const (
	// CircuitClosed is the normal operating state — all requests pass through.
	CircuitClosed CircuitState = iota
	// CircuitHalfOpen allows a single probe request to test recovery.
	CircuitHalfOpen
	// CircuitOpen rejects all requests immediately to protect a failing backend.
	CircuitOpen
)

// String returns a human-readable label for the circuit state.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitHalfOpen:
		return "half-open"
	case CircuitOpen:
		return "open"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// Circuit breaker implementation
// ---------------------------------------------------------------------------

// CircuitBreaker implements a simple three-state circuit breaker pattern.
// It tracks consecutive failures and transitions between Closed, Open, and
// Half-Open states to protect against cascading failures from an unhealthy
// ClickHouse backend.
type CircuitBreaker struct {
	mu                  sync.Mutex
	state               CircuitState
	consecutiveFailures int
	threshold           int           // failures before opening
	timeout             time.Duration // time in open state before half-open
	lastFailureTime     time.Time
	now                 func() time.Time // for testing
}

// NewCircuitBreaker creates a new circuit breaker.
// threshold is the number of consecutive failures before the circuit opens.
// timeout is how long to wait in the open state before allowing a probe.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     CircuitClosed,
		threshold: threshold,
		timeout:   timeout,
		now:       time.Now,
	}
}

// Allow checks whether a request should be allowed through.
// Returns true if the request can proceed, false if the circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		// In half-open, allow one probe request at a time.
		// The caller must report success/failure via RecordSuccess/RecordFailure.
		return true
	case CircuitOpen:
		// Check if enough time has passed to try a probe
		if cb.now().Sub(cb.lastFailureTime) > cb.timeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess records a successful operation, resetting the failure count
// and transitioning from Half-Open to Closed if applicable.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
	}
}

// RecordFailure records a failed operation. If consecutive failures reach
// the threshold, the circuit transitions to Open.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures++
	cb.lastFailureTime = cb.now()

	if cb.consecutiveFailures >= cb.threshold {
		cb.state = CircuitOpen
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check for auto-transition from open to half-open
	if cb.state == CircuitOpen && cb.now().Sub(cb.lastFailureTime) > cb.timeout {
		cb.state = CircuitHalfOpen
	}

	return cb.state
}

// Reset forces the circuit back to the closed state. Useful after a
// successful reconnection.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = CircuitClosed
	cb.consecutiveFailures = 0
}
