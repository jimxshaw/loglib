package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Create an index file large enough to contain our test
// entries via the truncate method call. We have to grow
// the file before we use it because it's memory-mapped to
// a slice of bytes. We'd get an out-of-bounds error if we
// didn't increase the file size before we wrote to it.
func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	index, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = index.Read(-1)
	require.NoError(t, err)
	require.Equal(t, f.Name(), index.Name())

	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
	}

	// Iterate over each entry adn write it to the index.
	// Check that the same entry can be read back via the
	// Read method.
	for _, want := range entries {
		err = index.Write(want.Offset, want.Position)
		require.NoError(t, err)

		_, position, err := index.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Position, position)
	}

	// Index and scanner should error when reading
	// past existing entries.
	_, _, err = index.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = index.Close()

	// Index should build its state from the existing file,
	// for when the service restarts with existing data.
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	index, err = newIndex(f, c)
	require.NoError(t, err)
	offset, position, err := index.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, entries[1].Position, position)
}
