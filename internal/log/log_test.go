package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/jimxshaw/loglib/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32

			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, o *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	offset, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	offset, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)

	n, err := NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	offset, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	offset, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)
}