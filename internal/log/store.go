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
	// Type embedding:
	// https://go101.org/article/type-embedding.html
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

func (s *store) Append(p []byte) (numOfBytes uint64, position uint64, err error) {
	// Ensure only 1 goroutine can access a variable at
	// a time to avoid conflicts. This is called mutual exclusion
	// and the data structure name for that is mutex.
	s.mu.Lock()

	// Unlock can be called anywhere in the method as deferred
	// functions are executed after the expression list
	// of the return statement is evaluated.
	defer s.mu.Unlock()

	position = s.size

	// Write the length of the record so that when the record is
	// read the number of bytes to read will be known.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write to the buffered writer, instead of directly to the file,
	// will reduce the number of system calls and improve performance.
	written, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	written += lenWidth
	s.size += uint64(written)

	// Number of bytes written, position where the store
	// holds the record in its file.
	// The segment will use this position when it creates
	// an associated index entry for this record.
	return uint64(written), position, nil
}

func (s *store) Read(position uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the writer buffer first, in case we try to read
	// a record that the buffer hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Find out how many bytes we have to read to get the whole record.
	// Then we retrieve and return that record.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(position+lenWidth)); err != nil {
		return nil, err
	}

	// Returns the record stored at the given position.
	return b, nil
}

// ReadAt reads the length of the byte slice into the byte slice
// starting at the offset in the store's file. It implements
// io.ReadAt interface on the store struct.
func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, offset)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
