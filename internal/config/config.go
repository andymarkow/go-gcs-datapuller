package config

import (
	"flag"
	"fmt"
	"time"

	"github.com/caarlos0/env"
)

type Config struct {
	LogLevel        string        `env:"LOG_LEVEL"`
	LogFormat       string        `env:"LOG_FORMAT"`
	GCSBucketName   string        `env:"GCS_BUCKET_NAME"`
	GCSBucketPrefix string        `env:"GCS_BUCKET_PREFIX"`
	DestDir         string        `env:"DEST_DIR"`
	Parallelism     int           `env:"PARALLELISM"`
	ReadInterval    time.Duration `env:"READ_INTERVAL"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT"`
}

func NewConfig() (Config, error) {
	cfg := Config{}

	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log output level [env:LOG_LEVEL]")
	flag.StringVar(&cfg.LogFormat, "log-format", "json", "log output format [env:LOG_FORMAT]")
	flag.StringVar(&cfg.GCSBucketName, "bucket-name", "", "GCS bucket name [env:GCS_BUCKET_NAME]")
	flag.StringVar(&cfg.GCSBucketPrefix, "bucket-prefix", "", "GCS bucket prefix [env:GCS_BUCKET_PREFIX]")
	flag.StringVar(&cfg.DestDir, "dest-dir", ".", "destination directory to store data [env:DEST_DIR]")
	flag.IntVar(&cfg.Parallelism, "parallelism", 1, "number of parallel workers [env:PARALLELISM]")
	flag.DurationVar(&cfg.ReadInterval, "read-interval", 60*time.Second, "read interval as duration [env:READ_INTERVAL]")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", 60*time.Second, "read timeout as duration [env:READ_TIMEOUT]")
	flag.Parse()

	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("env.Parse: %w", err)
	}

	if cfg.GCSBucketName == "" {
		return cfg, fmt.Errorf("flag '--bucket-name' or environment variable 'GCS_BUCKET_NAME' is required")
	}

	return cfg, nil
}
