package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/andymarkow/go-gcs-datapuller/internal/server/router"
)

// Server represents a HTTP server.
type Server struct {
	log    *slog.Logger
	server *http.Server
}

func NewServer(opts ...Option) *Server {
	mux := router.NewRouter()

	srv := &Server{
		log: slog.Default(),
		server: &http.Server{
			Addr:              ":8080",
			Handler:           mux,
			ReadTimeout:       30 * time.Second,
			ReadHeaderTimeout: 30 * time.Second,
			WriteTimeout:      30 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(srv)
	}

	return srv
}

type Option func(s *Server)

func WithLogger(log *slog.Logger) Option {
	return func(s *Server) {
		s.log = log
	}
}

func WithServerAddr(addr string) Option {
	return func(s *Server) {
		s.server.Addr = addr
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(s *Server) {
		s.server.ReadTimeout = timeout
	}
}

func WithReadHeaderTimeout(timeout time.Duration) Option {
	return func(s *Server) {
		s.server.ReadHeaderTimeout = timeout
	}
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *Server) {
		s.server.WriteTimeout = timeout
	}
}

func (s *Server) Start() error {
	s.log.Info("Starting HTTP server", slog.String("addr", s.server.Addr))

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http.ListenAndServe: %w", err)
	}

	return nil
}
