package datapuller

import (
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
