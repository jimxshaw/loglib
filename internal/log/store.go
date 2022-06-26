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
