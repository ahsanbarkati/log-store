package logstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func getBlob(i int) []byte { return []byte(fmt.Sprintf("hello-%d", i)) }

func TestLogFileOpen(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)
	lf.initLogFile()

	require.Equal(t, uint64(0), lf.nextIdx)
	require.Equal(t, uint64(dataStartOffset), lf.nextDataOffset)
}

func TestLogFileAppend(t *testing.T) {
	lf, err := OpenLogFile(t.TempDir(), 1)
	require.NoError(t, err)
	lf.initLogFile()

	data := []byte("hello")
	lf.append(data, 190)

	// Check that the entry is created as expected
	e := lf.getEntry(0)
	require.Equal(t, uint64(190), e.Index())
	require.Equal(t, uint64(dataStartOffset), e.DataOffset())
	require.Equal(t, uint64(len(data)), e.DataSize())

	// Check that nextIdx and dataOffset is correctly set.
	require.Equal(t, uint64(1), lf.nextIdx)
	require.Equal(t, uint64(dataStartOffset+len(data)), lf.nextDataOffset)
}

func TestAppendAndReplay(t *testing.T) {
	ls, err := OpenLogStore(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, 1, len(ls.storeFiles))

	n := int(100)
	for i := 0; i < n; i++ {
		idx, err := ls.Append(getBlob(i))
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx)
	}

	ind := 0
	ls.Replay(0, func(buf []byte) {
		require.Equal(t, getBlob(ind), buf)
		ind++
	})
	require.Equal(t, n, ind)
}

func TestRotate(t *testing.T) {
	dir := t.TempDir()
	ls, err := OpenLogStore(dir)
	require.NoError(t, err)

	// Adding entry more than maxEntries should rotate the log file.
	for i := 0; i < maxEntries+1; i++ {
		ls.Append(getBlob(i))
	}
	require.Equal(t, 2, len(ls.storeFiles))

	// Walk the directory and verify that we have two log files.
	var fileList []string
	filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if !fi.IsDir() && strings.HasSuffix(path, logSuffix) {
			_, fname := filepath.Split(path)
			fileList = append(fileList, fname)
		}
		return nil
	})
	require.Equal(t, []string{"00000.store", "00001.store"}, fileList)
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()
	ls, err := OpenLogStore(dir)
	require.NoError(t, err)

	// Adding entry more than maxEntries should rotate the log file. After this there should be
	// two files.
	for i := 0; i < maxEntries+1; i++ {
		ls.Append(getBlob(i))
	}
	require.Equal(t, 2, len(ls.storeFiles))

	// Truncating maxEntries-1 entries should delete the file 00000.store.
	ls.Truncate(maxEntries - 1)
	require.Equal(t, 1, len(ls.storeFiles))

	filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if !fi.IsDir() && strings.HasSuffix(path, logSuffix) {
			_, fname := filepath.Split(path)
			require.Equal(t, "00001.store", fname)
		}
		return nil
	})
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
