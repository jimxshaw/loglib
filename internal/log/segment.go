package log

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
