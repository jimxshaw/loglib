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

func testStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	// https://stackoverflow.com/questions/18415904/what-does-mode-t-0644-mean
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fileInfo.Size(), nil
}
