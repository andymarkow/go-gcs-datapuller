package datapuller

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"

	"github.com/andymarkow/go-gcs-datapuller/internal/storage/gcsstorage"
)

type DataPuller struct {
	log          *slog.Logger
	storage      *gcsstorage.Storage
	bucketName   string
	bucketPrefix string
	destDir      string
	parallelism  int
	readInterval time.Duration
	readTimeout  time.Duration
}

func NewDataPuller(store *gcsstorage.Storage, bucketName string, opts ...Option) (*DataPuller, error) {
	dp := &DataPuller{
		log:          slog.Default(),
		storage:      store,
		bucketName:   bucketName,
		destDir:      ".",
		parallelism:  1,
		readInterval: 60 * time.Second,
		readTimeout:  60 * time.Second,
	}

	for _, opt := range opts {
		opt(dp)
	}

	return dp, nil
}

type Option func(d *DataPuller)

func WithLogger(log *slog.Logger) Option {
	return func(d *DataPuller) {
		d.log = log
	}
}

func WithBucketPrefix(prefix string) Option {
	return func(d *DataPuller) {
		d.bucketPrefix = prefix
	}
}

func WithDestDir(destDir string) Option {
	return func(d *DataPuller) {
		d.destDir = destDir
	}
}

func WithParallelism(parallelism int) Option {
	return func(d *DataPuller) {
		d.parallelism = parallelism
	}
}

func WithReadInterval(interval time.Duration) Option {
	return func(d *DataPuller) {
		d.readInterval = interval
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(d *DataPuller) {
		d.readTimeout = timeout
	}
}

func (d *DataPuller) Run(ctx context.Context, done chan<- struct{}) {
	d.log.Info("Starting data puller")

	ctxCancel, cancel := context.WithCancel(ctx)

	objsChan := d.runProducer(ctxCancel)

	wg := &sync.WaitGroup{}

	// Spawn workers.
	for w := 1; w <= d.parallelism; w++ {
		wg.Add(1)

		go d.runWorker(ctxCancel, wg, objsChan)
	}

	d.log.Info("Data puller started")

	// Wait for context done to initiate stop.
	<-ctx.Done()

	// Cancel context to stop producer and workers.
	cancel()

	// Wait for workers to finish.
	wg.Wait()

	d.log.Info("Data puller stopped")

	// Signal that the data puller is done.
	done <- struct{}{}
}

func (d *DataPuller) Close() error {
	if err := d.storage.Close(); err != nil {
		return fmt.Errorf("storage.Close: %w", err)
	}

	return nil
}

func (d *DataPuller) runProducer(ctx context.Context) <-chan gcsstorage.StorageObject {
	d.log.Info("Data producer started")

	ch := make(chan gcsstorage.StorageObject)

	ticker := time.NewTicker(d.readInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()

				close(ch)

				d.log.Info("Data producer stopped")

				return

			case <-ticker.C:
				ctxCancel, cancel := context.WithTimeout(ctx, d.readTimeout)
				objs, err := d.pullObjectsList(ctxCancel)

				cancel()

				if err != nil {
					d.log.Error("pullObjectsList", slog.Any("error", err))

					continue
				}

				for _, obj := range objs {
					ch <- obj
				}
			}
		}
	}()

	return ch
}

func (d *DataPuller) pullObjectsList(ctx context.Context) ([]gcsstorage.StorageObject, error) {
	query := &storage.Query{
		Prefix:     d.bucketPrefix,
		Versions:   false,
		Projection: storage.ProjectionNoACL,
	}

	attrSelection := []string{"Name", "Bucket", "CRC32C"}

	if err := query.SetAttrSelection(attrSelection); err != nil {
		return nil, fmt.Errorf("query.SetAttrSelection: %w", err)
	}

	objs, err := d.storage.ListObjects(ctx, d.bucketName, query)
	if err != nil {
		return nil, fmt.Errorf("storage.ListObjects: %w", err)
	}

	return objs, nil
}

func (d *DataPuller) runWorker(ctx context.Context, wg *sync.WaitGroup, objs <-chan gcsstorage.StorageObject) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case obj := <-objs:
			// Trim destination directory OS-specific path separator if any.
			destDirPath := strings.TrimRight(d.destDir, string(os.PathSeparator))

			// Concatenate full file path.
			filePath := filepath.Join(destDirPath, string(os.PathSeparator), obj.Name())

			fileExists, err := isFileExists(filePath)
			if err != nil {
				d.log.Error("isFileExists", slog.Any("error", err))

				continue
			}

			if fileExists {
				d.log.Debug("File already exists", slog.String("file", obj.Name()))

				d.log.Debug("Calculating file hashsum", slog.String("file", obj.Name()))

				isEqual, err := compareFileHashsum(filePath, obj.CRC32C())
				if err != nil {
					d.log.Error("failed to compare file hashsum with the object", slog.Any("error", err))

					continue
				}

				if isEqual {
					d.log.Debug("File hashsums match. Skipping download", slog.String("file", obj.Name()), slog.Uint64("crc32c", uint64(obj.CRC32C())))

					continue
				}
			} else {
				d.log.Debug("File does not exist", slog.String("file", obj.Name()))

				// Create destination directory if not exists.
				if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
					d.log.Error("os.MkdirAll", slog.Any("error", err))
				}
			}

			f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				d.log.Error("os.OpenFile", slog.Any("error", err))

				continue
			}

			d.log.Debug("Proceeding with file download", slog.String("file", obj.Name()))

			ctxCancel, cancel := context.WithTimeout(ctx, d.readTimeout)

			if err := d.storage.ReadObject(ctxCancel, f, obj); err != nil {
				d.log.Error("ReadObject", slog.Any("error", err))

				cancel()

				continue
			}

			cancel()

			f.Close()
		}
	}
}

// isFileExists checks if a file exists at the given path.
//
// It returns true and nil if the file exists, false and nil if the file does not exist,
// and false and error if an error occurs during file existence check.
//
// The function performs a file existence check using os.Stat and errors.Is.
// If the file exists, it returns true and nil. If the file does not exist,
// it returns false and nil. If an error occurs during file existence check,
// it returns false and the error.
func isFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, fmt.Errorf("os.Stat: %w", err)
}

// getCRC32hashsum calculates the CRC32 hashsum of a given io.Reader.
//
// The function returns the hashsum as a uint32 and an error if any.
//
// The function uses the Castagnoli CRC32 polynomial.
func getCRC32hashsum(rd io.Reader) (uint32, error) {
	// Create a CRC32 hash table.
	table := crc32.MakeTable(crc32.Castagnoli)

	// Create a CRC32 hash using the table.
	hash := crc32.New(table)

	// Write bytes to the hash.
	if _, err := io.Copy(hash, rd); err != nil {
		return 0, fmt.Errorf("io.Copy: %w", err)
	}

	return hash.Sum32(), nil
}

// compareFileHashsum compares the hashsum of a given file with the given crc32c value.
//
// The function opens a file at the given filePath, calculates its hashsum and compares it
// with the given crc32c value. If the hashsums match, the function returns true, nil.
// If the hashsums don't match, or an error occurs during file opening or hashsum calculation,
// the function returns false, error.
func compareFileHashsum(filePath string, crc32c uint32) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("os.Open: %w", err)
	}
	defer f.Close()

	fileHashSum, err := getCRC32hashsum(f)
	if err != nil {
		return false, fmt.Errorf("failed to calculate file hashsum: %w", err)
	}

	return fileHashSum == crc32c, nil
}
