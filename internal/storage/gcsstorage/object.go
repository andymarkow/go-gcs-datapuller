package gcsstorage

import (
	"fmt"
)

type StorageObject struct {
	name   string
	bucket string
	crc32c uint32
}

func NewStorageObject(name, bucket string, crc32c uint32) (*StorageObject, error) {
	if err := validateName(name); err != nil {
		return nil, fmt.Errorf("invalid name: %w", err)
	}

	if err := validateCRC32C(crc32c); err != nil {
		return nil, fmt.Errorf("invalid crc32c: %w", err)
	}

	if err := validateBucket(bucket); err != nil {
		return nil, fmt.Errorf("invalid bucket: %w", err)
	}

	return &StorageObject{
		name:   name,
		bucket: bucket,
		crc32c: crc32c,
	}, nil
}

func (s *StorageObject) Name() string {
	return s.name
}

func (s *StorageObject) Bucket() string {
	return s.bucket
}

func (s *StorageObject) CRC32C() uint32 {
	return s.crc32c
}

func validateName(name string) error {
	if name == "" {
		return ErrObjectNameEmpty
	}

	return nil
}

func validateBucket(bucket string) error {
	if bucket == "" {
		return ErrObjectBucketNameEmpty
	}

	return nil
}

func validateCRC32C(crc32c uint32) error {
	if crc32c == 0 {
		return ErrObjectCRC32CInvalid
	}

	return nil
}
