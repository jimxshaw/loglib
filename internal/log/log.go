package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/jimxshaw/loglib/api/v1"
)

// The log consists of a list of segments and
// a pointer to the active segment to append
// writes to. The directory stores segments.
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

// Set config defaults if the caller didn't specify,
// create a log instance and set up the instance.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// When a log starts, set itself up for for segments already
// exist on disk or if the leg is new and has no segments
// then bootstraping the initial segment.
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Fetch the list of segments on disk, parse and sort the
	// base offsets in order from oldest to newest.
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffSet contains duplicate for index and store so
		// skip the duplicate.
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

// Appends a record to the log. Append the record to the
// active segment. Make a new active segment if the segment
// is at its max size (per the max size configuration).
func (l *Log) Append(record *api.Record) (uint64, error) {
	// RWMutex is used to grant access to reads when there
	// isn't a write holding the lock.
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(offset + 1)
	}

	return offset, err
}

// Reads the record stored at the given offset.
func (l *Log) Read(offset uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		// Since the segments are in order from oldest to newest and the
		// segment's base offset is the smallest offset in the segment,
		// we iterate over the segments until we find the first segment
		// whose base offset is less than or equal to the offset we seek.
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	// Once we know the segment that contains the record, we get the index
	// entry from the segment's index and we read the data out of the
	// segment's store file and return the data.
	return s.Read(offset)
}

// Iterate over the segments and closes them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Closes the log and remove its data.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Removes the log and creates a new log to replace it.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset := l.segments[len(l.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}
	return offset - 1, nil
}

// Removes all segments whose highest offset is lower than
// lowest. We don't have infinite disk space so we call
// truncate periodically to remove old segments whose data
// has hopefully been procssed by then and don't need anymore.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}
