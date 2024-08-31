package datapuller

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCRC32Hashsum(t *testing.T) {
	data := "qwerty"

	rd := strings.NewReader(data)

	wantHash := uint32(3283772498)

	t.Run("base", func(t *testing.T) {
		hash, err := getCRC32hashsum(rd)
		require.NoError(t, err)

		require.Equal(t, wantHash, hash)
	})
}

func TestCompareFileHashsum(t *testing.T) {
	// Create a test file with known hashsum.
	testFile, err := os.CreateTemp("", "file.test")
	require.NoError(t, err)

	defer os.Remove(testFile.Name())

	_, err = testFile.WriteString("Hello, World!")
	require.NoError(t, err)

	err = testFile.Close()
	require.NoError(t, err)

	f, err := os.Open(testFile.Name())
	require.NoError(t, err)
	defer f.Close()

	hashsum, err := getCRC32hashsum(f)
	require.NoError(t, err)

	tests := []struct {
		name      string
		filePath  string
		crc32c    uint32
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "file exists and hashsum matches",
			filePath:  testFile.Name(),
			crc32c:    hashsum,
			wantMatch: true,
			wantErr:   false,
		},
		{
			name:      "file exists but hashsum does not match",
			filePath:  testFile.Name(),
			crc32c:    hashsum + 1,
			wantMatch: false,
			wantErr:   false,
		},
		{
			name:      "file does not exist",
			filePath:  "non-existent-file",
			crc32c:    hashsum,
			wantMatch: false,
			wantErr:   true,
		},
		{
			name:      "file cannot be opened due to permission error",
			filePath:  "/root/non-existent-file",
			crc32c:    hashsum,
			wantMatch: false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := compareFileHashsum(tt.filePath, tt.crc32c)
			if (err != nil) != tt.wantErr {
				t.Errorf("compareFileHashsum() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if match != tt.wantMatch {
				t.Errorf("compareFileHashsum() match = %v, want %v", match, tt.wantMatch)
			}
		})
	}
}
