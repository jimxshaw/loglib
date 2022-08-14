package log

import (
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"
)

// Segment wraps the store and index types and
// coordinates operations across them.
// For example, when the log appends a record to
// the active segment, the segment writes the data
// to its store and adds a new entry to the index.
// For reads, the segment needs to look up the entry
// from the index and then fetch the data from the store.
type segment struct {
	store *store
	index *index
	// These offsets are needed to know what offset to append
	// new records under and to calculate the relative offsets
	// for the index entries.
	baseOffset uint64
	nextOffset uint64
	// Config is in the segment in order to compare the store
	// file and the index sizes to the configured limits, which
	// lets us know when the segment is maxed out.
	config Config
}

// The log calls this when it needs to add a new segment, such as when the
// current active segment hits its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	// Create the file if it doesn't exist yet.
	// The append flag makes the operating system append to the
	// file when writing.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Create the file if it doesn't exist yet.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Set the segment's next offset to prepare for the
	// next appended record.
	if off, _, err := s.index.Read(-1); err != nil {
		// If the index is empty then the next record appended to
		// the segment would be the first record and its offset
		// would be the segment's base offset.
		s.nextOffset = baseOffset
	} else {
		// If the index has at least one entry then that means
		// the offset of the next record written should take
		// the offset at the end of the segment by adding 1
		// to the base offset and relative offset.
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record to the segment and returns the newly appended record's offset.
// The log returns the offset to the API response.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	current := s.nextOffset
	record.Offset = current

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// The segment appends the data to the store and then adds an index entry.
	_, position, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// index offsets are relative to the base offset.
		// Subtract the segment's next offset from its base offset, which are
		// both absolute offsets, to get the entry's relative offset in the segment.
		uint32(s.nextOffset-uint64(s.baseOffset)),
		position,
	); err != nil {
		return 0, nil
	}

	// Increment the next offset to prep for a future append call.
	s.nextOffset++

	return current, nil
}

// Read returns the record for the given offset.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	// Translate the absolute index into a relative offset and get
	// the associated index entry.
	_, position, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// The segment goes to the record's position in the store
	// and read the proper amount of data.
	p, err := s.store.Read(position)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

// The log uses this to know it needs to create a new segment.
func (s *segment) IsMaxed() bool {
	// Writing a small number of long logs will hit the segment bytes limit.
	// Writing many short logs will hit the index bytes limit.
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// This closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}
