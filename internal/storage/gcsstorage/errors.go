package gcsstorage

import "fmt"

var (
	ErrObjectNameEmpty       = fmt.Errorf("object name is empty")
	ErrObjectCRC32CInvalid   = fmt.Errorf("object crc32c is invalid")
	ErrObjectBucketNameEmpty = fmt.Errorf("object bucket name is empty")
)
