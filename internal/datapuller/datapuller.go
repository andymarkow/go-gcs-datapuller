package datapuller

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/andymarkow/go-gcs-datapuller/internal/storage/gcsstorage"
)

type DataPuller struct {
	log          *slog.Logger
	storage      *gcsstorage.GCSStorage
	bucketName   string
	bucketPrefix string
	destDir      string
	parallelism  int
	readInterval time.Duration
	readTimeout  time.Duration
}

func NewDataPuller(bucketName string, opts ...Option) (*DataPuller, error) {
	store, err := gcsstorage.NewGCSStorage(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("gcsstorage.NewGCSStorage: %w", err)
	}

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
	//TODO: implement.

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case obj := <-objs:
			d.log.Info("Pulling object", slog.String("name", obj.Name()), slog.Uint64("crc32c", uint64(obj.CRC32C())))
		}
	}
}

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
