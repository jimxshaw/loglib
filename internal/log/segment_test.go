package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/jimxshaw/loglib/api/v1"
	"github.com/stretchr/testify/require"
)

// Test append a record to a segment, read back the same
// record and eventually hit the configured max size for
// both the store and index. Calling newSegment() twice
// with the same base offset and directory also checks that
// the function loads a segment's state from the persisted
// index and log files.
func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello go")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, offset)

		got, err := s.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	// Maxed index.
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	//Maxed store.
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
