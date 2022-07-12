package log

// Config provides configuration of a log,
// such as the max size of a segment's
// store and index.
type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
