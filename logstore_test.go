package logstore

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFileOpen(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)
	lf.init()
	require.Equal(t, uint64(0), lf.nextIdx)
	require.Equal(t, uint64(dataStartOffset), lf.dataOffset)
}

func TestLogFileAppend(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)
	lf.init()

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

}

func TestAppendAndReplay(t *testing.T) {
	ls, err := OpenLogStore(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 1, len(ls.storeFiles))

	n := int(100)
	for i := 0; i < n; i++ {
		idx, err := ls.Append([]byte(fmt.Sprintf("hello-%d", i)))
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx)
	}

	ind := 0
	ls.Replay(0, func(buf []byte) {
		require.Equal(t, []byte(fmt.Sprintf("hello-%d", ind)), buf)
		ind++
	})
	require.Equal(t, n, ind)
}

func TestConcurrentAppend(t *testing.T) {
	ls, err := OpenLogStore(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 1, len(ls.storeFiles))

	var wg sync.WaitGroup
	numBlobs := 100
	numGo := 16

	for i := 0; i < numGo; i++ {
		wg.Add(1)
		go func(thread int, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < numBlobs; i++ {
				_, err := ls.Append([]byte(fmt.Sprintf("hello-%d-%d", thread, i)))
				require.NoError(t, err)
			}

		}(i, &wg)
	}
	wg.Wait()

	mp := make(map[int]int)
	ls.Replay(0, func(buf []byte) {
		splits := strings.Split(string(buf), "-")
		th, err := strconv.Atoi(splits[1])
		require.NoError(t, err)
		mp[th]++
	})

	for i := 0; i < numGo; i++ {
		require.Equal(t, numBlobs, mp[i])
	}
}
