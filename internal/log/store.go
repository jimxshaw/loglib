package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// Sizes and index entries of records are
	// persisted using this type of encoding.
	enc = binary.BigEndian
)

const (
	// Number of bytes used to store
	// the record's length.
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// Getting the file's current size is important just
	// in case the store is being re-created from a file that
	// has existing data. E.g. after this service re-started.
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(file.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}
