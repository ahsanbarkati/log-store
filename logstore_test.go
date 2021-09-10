package logstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	ls, err := OpenLogStore(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 1, len(ls.storeFiles))
}
