package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/andymarkow/go-gcs-datapuller/internal/config"
	"github.com/andymarkow/go-gcs-datapuller/internal/datapuller"
	"github.com/andymarkow/go-gcs-datapuller/internal/logger"
	"github.com/andymarkow/go-gcs-datapuller/internal/server"
	"github.com/andymarkow/go-gcs-datapuller/internal/storage/gcsstorage"
)

// App represents the application.
type App struct {
	log    *slog.Logger
	server *server.Server
	puller *datapuller.DataPuller
}

// NewApp creates a new App instance.
func NewApp() (*App, error) {
	cfg, err := config.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("config.NewConfig: %w", err)
	}

	logLevel, err := logger.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("logger.ParseLogLevel: %w", err)
	}

	l := logger.NewLogger(
		logger.WithLevel(logLevel),
		logger.WithFormat(logger.LogFormat(cfg.LogFormat)),
	)

	store, err := gcsstorage.NewStorage(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("gcsstorage.NewStorage: %w", err)
	}

	puller, err := datapuller.NewDataPuller(
		store,
		cfg.GCSBucketName,
		datapuller.WithLogger(l),
		datapuller.WithBucketPrefix(cfg.GCSBucketPrefix),
		datapuller.WithDestDir(cfg.DestDir),
		datapuller.WithReadInterval(cfg.ReadInterval),
		datapuller.WithReadTimeout(cfg.ReadTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("datapuller.NewDataPuller: %w", err)
	}

	srv := server.NewServer(
		server.WithLogger(l),
		server.WithServerAddr(cfg.ServerAddr),
	)

	return &App{
		log:    l,
		server: srv,
		puller: puller,
	}, nil
}

// Start starts the application.
func (a *App) Start() error {
	errChan := make(chan error, 1)

	go func() {
		if err := a.server.Start(); err != nil {
			errChan <- fmt.Errorf("server.Start: %w", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go a.puller.Run(ctx, done)

	// Graceful shutdown by OS signals.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
		a.log.Info("Termination signal received")

	case err := <-errChan:
		a.log.Error("Server error", slog.Any("error", err))
	}

	cancel()

	// Wait for datapuller to finish.
	a.log.Info("Waiting for data puller to finish tasks")
	<-done

	return nil
}

// Shutdown shuts down the application.
func (a *App) Shutdown() error {
	if err := a.puller.Close(); err != nil {
		return fmt.Errorf("datapuller.Shutdown: %w", err)
	}

	return nil
}
