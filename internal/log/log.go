package log

import (
	"io/ioutil"
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
