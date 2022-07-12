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
	// https://en.wikipedia.org/wiki/Memory-mapped_file
	mmap gommap.MMap
	// Size of the index and where to write the
	// next entry appended to the index.
	size uint64
}

// newIndex creates an index for the given file.
// Create the index and save the current file size in
// order to keep track of the amount of data in the index
// file as more index entries are added. Grow the file to
// the max index size before memory-mapping the file and
// return the created index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	index := &index{
		file: f,
	}

	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	index.size = uint64(file.Size())
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if index.mmap, err = gommap.Map(
		index.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return index, nil
}

// Close ensures the memory-mapped file has synced its data to
// the persisted file and has flushed its contents to stable 
// storage. Then truncates the persisted file to the amount
// of data that's actually in it and closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
