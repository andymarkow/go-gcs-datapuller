package gcsstorage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSStorage represents the Google Cloud Storage client.
type GCSStorage struct {
	client *storage.Client
}

// NewGCSStorage creates a new Google Cloud Storage instance.
func NewGCSStorage(ctx context.Context, creds []byte) (*GCSStorage, error) {
	var storageOpts []option.ClientOption

	if len(creds) > 0 {
		storageOpts = append(storageOpts, option.WithCredentialsJSON(creds))
	}

	client, err := storage.NewClient(ctx, storageOpts...)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}

	return &GCSStorage{
		client: client,
	}, nil
}

// Close closes the Google Cloud Storage client.
func (s *GCSStorage) Close() error {
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("client.Close: %w", err)
	}

	return nil
}

// ListObjects returns all objects in the storage bucket by query.
func (s *GCSStorage) ListObjects(ctx context.Context, bucketName string, query *storage.Query) ([]StorageObject, error) {
	objects := make([]StorageObject, 0)

	it := s.client.Bucket(bucketName).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("client.Bucket(%q).Objects(): %w", bucketName, err)
		}

		obj, err := NewStorageObject(attrs.Name, attrs.Bucket, attrs.CRC32C)
		if err != nil {
			return nil, fmt.Errorf("NewStorageObject: %w", err)
		}

		objects = append(objects, *obj)
	}

	return objects, nil
}

func (s *GCSStorage) ReadObject(ctx context.Context, obj StorageObject) (io.ReadCloser, error) {
	rd, err := s.client.Bucket(obj.Bucket()).Object(obj.Name()).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("client.Bucket(%q).Object(%q).NewReader: %w", obj.Bucket(), obj.Name(), err)
	}

	return rd, nil
}
