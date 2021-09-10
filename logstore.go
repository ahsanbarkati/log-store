package logstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const (
	LOG_SUFFIX = ".store"
)

type lstore struct {
	storeFiles []*logFile
	cur        *logFile

	nextIdx int
	dir     string
}

func OpenLogStore(dir string) (*lstore, error) {
	var fileList []string
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() && strings.HasSuffix(path, LOG_SUFFIX) {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log store")
	}

	var files []*logFile
	for _, path := range fileList {
		_, fname := filepath.Split(path)
		fname = strings.TrimSuffix(fname, LOG_SUFFIX)

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

	f, err := OpenLogFile(dir, 1)
	ls.cur = f
	return ls, nil
}

func (ls *lstore) Append(b []byte) error {
	return nil
}

func (ls *lstore) GetPosition() error {
	return nil
}

func (ls *lstore) Truncate(idx uint64) error {
	return nil
}

func (ls *lstore) Replay(idx uint64, callback func([]byte) error) error {
	return nil
}

type entry []byte

func (e entry) Index() uint64      { return binary.BigEndian.Uint64(e[:8]) }
func (e entry) DataOffset() uint64 { return binary.BigEndian.Uint64(e[8:]) }

func createEntry(dataOffset, idx uint64) *entry {
	return &entry{}
}

type logFile struct {
	data []byte
	fd   *os.File
	fid  int
}

func OpenLogFile(path string, fid int) (*logFile, error) {
	filename := filepath.Join(path, fmt.Sprintf("%05d%s", fid, LOG_SUFFIX))

	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open: %q", filename)
	}

	mtype := unix.PROT_READ | unix.PROT_WRITE
	size := 1 << 20

	buf, err := syscall.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "failed to mmap the log file")
	}

	return &logFile{
		data: buf,
		fd:   fd,
		fid:  fid,
	}, nil
}
