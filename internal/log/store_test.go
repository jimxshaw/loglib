package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// Create a new store and test reading from it
	// again to verify that the service will recover
	// its state after a restart.
	s, err = newStore(f)
	require.NoError(t, err)

	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		numOfBytes, position, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, position+numOfBytes, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var position uint64

	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(position)
		require.NoError(t, err)
		require.Equal(t, write, read)
		position += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		numOfBytesRead, err := s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, numOfBytesRead)
		offset += int64(numOfBytesRead)

		size := enc.Uint64(b)
		b = make([]byte, size)
		numOfBytesRead, err = s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), numOfBytesRead)
		offset += int64(numOfBytesRead)
	}
}
