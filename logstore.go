package logstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const (
	logSuffix  = ".store"
	entrySize  = 24
	maxEntries = 10

	logFileSize = 1 << 30

	dataStartOffset = entrySize * maxEntries
)

type lstore struct {
	sync.RWMutex
	storeFiles []*logFile
	cur        *logFile // Current log file that is being used.
	nextIdx    uint64   // Index at which next write is done.
	dir        string   // Directory of the log store.
}

// Append is used to append data blob to the log store. It returns the index at which the blob was
// written and error, if any. This function is thread-safe.
func (ls *lstore) Append(b []byte) (uint64, error) {
	ls.Lock()
	defer ls.Unlock()

	// create a new log file if the current log file is full.
	if ls.cur.nextIdx >= maxEntries {
		if err := ls.rotate(); err != nil {
			return 0, errors.Wrap(err, "failed to rotate the log file")
		}
	}
	lf := ls.cur
	lf.append(b, ls.nextIdx)
	ls.nextIdx++
	return ls.nextIdx - 1, nil
}

// GetPosition returns the index at which next entry will be writen. It is thread-safe.
func (ls *lstore) GetPosition() uint64 {
	return atomic.LoadUint64(&ls.nextIdx)
}

// Truncate clears the disk space by deleting entries before the given index. It is safe to call
// this API concurrently.
func (ls *lstore) Truncate(idx uint64) error {
	ls.Lock()
	defer ls.Unlock()

	// Find the log file which has first entry > idx. It is safe to delete all the files before it.
	fid := sort.Search(len(ls.storeFiles), func(i int) bool {
		lf := ls.storeFiles[i]
		return lf.getEntry(0).Index() > idx
	})

	var count uint64
	for _, lf := range ls.storeFiles {
		if lf.fid < fid {
			if err := lf.delete(); err != nil {
				return err
			}
			count++
			continue
		}
		break
	}
	ls.storeFiles = ls.storeFiles[count:]
	return nil
}

// Replay reads the log-store from idx to until the end and calls the callback function for each
// data entry.
func (ls *lstore) Replay(idx uint64, callback func(b []byte)) {
	ls.RLock()
	defer ls.RUnlock()

	for _, lf := range ls.storeFiles {
		lf.replay(idx, callback)
	}
}

// Sync is used to force sycn the memory-mapped log-store to the disk.
func (ls *lstore) Sync() error {
	ls.RLock()
	defer ls.RUnlock()

	return unix.Msync(ls.cur.data, unix.MS_SYNC)
}

// Close safely closes the log store by syncing it to the disk.
func (ls *lstore) Close() error {
	if err := unix.Msync(ls.cur.data, unix.MS_SYNC); err != nil {
		return errors.Wrap(err, "failed to sync to disk")
	}
	if err := unix.Munmap(ls.cur.data); err != nil {
		return errors.Wrap(err, "failed to Munmap")
	}
	err := ls.cur.fd.Close()
	return errors.Wrap(err, "failed to close the file")
}

// rotate closes the current log file, creates a new log file and sets it as the current file.
func (ls *lstore) rotate() error {
	if err := unix.Msync(ls.cur.data, unix.MS_SYNC); err != nil {
		return errors.Wrap(err, "failed to sync to disk")
	}
	lf, err := OpenLogFile(ls.dir, len(ls.storeFiles))
	if err != nil {
		return errors.Wrap(err, "failed to open new log file")
	}
	lf.init()
	ls.storeFiles = append(ls.storeFiles, lf)
	ls.cur = lf
	return nil
}

// OpenLogStore opens the log-store at the given directory. If there is already some store files
// present, then it initializes its state with those files, otherwise it creates a new store file.
func OpenLogStore(dir string) (*lstore, error) {
	var fileList []string
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() && strings.HasSuffix(path, logSuffix) {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed walk the given directory")
	}

	var files []*logFile
	for _, path := range fileList {
		_, fname := filepath.Split(path)
		fname = strings.TrimSuffix(fname, logSuffix)

		fid, err := strconv.ParseInt(fname, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse file name: %q", fname)
		}

		f, err := OpenLogFile(dir, int(fid))
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].fid < files[j].fid
	})

	ls := &lstore{
		storeFiles: files,
		dir:        dir,
	}
	if cnt := len(files); cnt > 0 {
		ls.cur = files[cnt-1]
		return ls, nil
	}

	// No store files were found, create one.
	lf, err := OpenLogFile(dir, 0)
	lf.init()
	ls.cur = lf
	ls.storeFiles = append(ls.storeFiles, lf)
	return ls, nil
}

type entry []byte

func (e entry) Index() uint64      { return binary.BigEndian.Uint64(e[:8]) }
func (e entry) DataOffset() uint64 { return binary.BigEndian.Uint64(e[8:16]) }
func (e entry) DataSize() uint64   { return binary.BigEndian.Uint64(e[16:24]) }

func setEntry(buf []byte, idx, dataOffset, dataSize uint64) {
	assert(len(buf) == entrySize)
	binary.BigEndian.PutUint64(buf, idx)
	binary.BigEndian.PutUint64(buf[8:], dataOffset)
	binary.BigEndian.PutUint64(buf[16:], dataSize)
}

type logFile struct {
	data []byte
	fd   *os.File
	fid  int

	nextIdx    uint64
	dataOffset uint64
}

func OpenLogFile(path string, fid int) (*logFile, error) {
	filename := filepath.Join(path, fmt.Sprintf("%05d%s", fid, logSuffix))
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open: %q", filename)
	}
	if err := fd.Truncate(int64(logFileSize)); err != nil {
		return nil, errors.Wrapf(err, "failed to truncate file")
	}

	mtype := unix.PROT_READ | unix.PROT_WRITE
	buf, err := unix.Mmap(int(fd.Fd()), 0, logFileSize, mtype, unix.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "failed to mmap the log file")
	}

	lf := &logFile{
		data: buf,
		fid:  fid,
		fd:   fd,
	}
	return lf, nil
}

func (lf *logFile) init() {
	lf.nextIdx = lf.nextIndex()
	lf.dataOffset = dataStartOffset

	if lf.nextIdx > 0 {
		last := lf.getEntry(lf.nextIdx - 1)
		lf.dataOffset = last.DataOffset() + last.DataSize()
	}
}

func (lf *logFile) append(data []byte, idx uint64) {
	entryOffset := lf.nextIdx * entrySize
	buf := lf.data[entryOffset : entryOffset+entrySize]
	setEntry(buf, idx, lf.dataOffset, uint64(len(data)))
	assert(len(data) == copy(lf.data[lf.dataOffset:], data))

	lf.nextIdx++
	lf.dataOffset += uint64(len(data))
}

func (lf *logFile) firstIndex() uint64 {
	return lf.getEntry(0).Index()
}

func (lf *logFile) nextIndex() uint64 {
	idx := sort.Search(maxEntries, func(i int) bool {
		e := lf.getEntry(uint64(i))
		return e.Index() == 0
	})
	return uint64(idx)
}

func (lf *logFile) getEntry(idx uint64) entry {
	entryOffset := idx * entrySize
	return entry(lf.data[entryOffset : entryOffset+entrySize])
}

func (lf *logFile) getData(idx uint64) []byte {
	e := lf.getEntry(idx)
	off := e.DataOffset()
	sz := e.DataSize()
	return lf.data[off : off+sz]
}

func (lf *logFile) replay(idx uint64, f func(b []byte)) {
	for i := idx; i < lf.nextIdx; i++ {
		f(lf.getData(i))
	}
}

func (lf *logFile) delete() error {
	if err := unix.Munmap(lf.data); err != nil {
		return errors.Wrap(err, "delete failed to unmap")
	}
	lf.data = nil
	if err := lf.fd.Truncate(0); err != nil {
		return errors.Errorf("failed to truncate file: %s, error: %v\n", lf.fd.Name(), err)
	}
	if err := lf.fd.Close(); err != nil {
		return errors.Errorf("failed to close file: %s, error: %v\n", lf.fd.Name(), err)
	}
	return os.Remove(lf.fd.Name())
}

func assert(condition bool) {
	if !condition {
		panic("Assert failed")
	}
}
