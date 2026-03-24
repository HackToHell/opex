package server

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
)

func TestAuthMiddleware_ValidToken(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{AuthToken: "test-secret"},
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response.WriteJSON(w, http.StatusOK, map[string]string{"ok": "true"})
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{AuthToken: "test-secret"},
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_InvalidPrefix(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{AuthToken: "test-secret"},
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_WrongToken(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{AuthToken: "test-secret"},
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_EmptyBearerToken(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{AuthToken: "test-secret"},
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	req.Header.Set("Authorization", "Bearer ")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}
