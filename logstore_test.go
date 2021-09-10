package logstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFileOpen(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)

	require.Equal(t, uint64(0), lf.nextIdx)
	require.Equal(t, uint64(dataStartOffset), lf.dataOffset)
}

func TestLogFileAppend(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)

	data := []byte("hello")
	lf.append(data, 190)
	e := lf.getEntry(0)

	require.Equal(t, uint64(190), e.Index())
	require.Equal(t, uint64(dataStartOffset), e.DataOffset())
	require.Equal(t, uint64(len(data)), e.DataSize())

	require.Equal(t, uint64(1), lf.nextIdx)
	require.Equal(t, uint64(dataStartOffset+len(data)), lf.dataOffset)
}

func TestOpen(t *testing.T) {
	ls, err := OpenLogStore(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 1, len(ls.storeFiles))

	ls.Append([]byte("hello"))
}
