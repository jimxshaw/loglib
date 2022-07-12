package log

import (
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// Widths define the number of bytes
	// that make up each index entry.
	// Entry width is used to go directly to
	// the position of an entry given its offset,
	// since the position in the file is offset * entryWidth.
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}
