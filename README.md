# Log Lib

Go example log service with gRPC.

### Big Picture of Logs
A log is a sequence of records that is append-only. Records are appended to the end of the log. A log is usually read from top to bottom, oldest to newest. Any data can be logged. 

When a record is appended to a log, it's assigned a unique and sequential offset number that serves as its ID. A log always order the records by time and also index each record by time created and offset.

Logs are split into segments because records can't be appended to the same file forever due to lack of unlimited disk space. As disk space grows, old segments with data that have already been processed or archived are deleted. Cleaning up old segments can be done in a background process while the log service still continues to produce to the newest (active) segment and consume from other segments with little to no conflict as long as the goroutines can access the same data.

The active segment is the only segment to which the log service actively writes. Once the active segment is filled then a new segment is created and that becomes the active segment.

A segment has a store file and an index file. Record data is stored in the store file and records are continually appended to it. Records are indexed in the index file. The index file increases read speed because it matches the record offset to its position in the store file. 

Reading a record, with its offset known, takes two steps:
1) Get the record's entry in the index file, which states the position of the record in the store file.
2) Read the record at that position in the store file.

An index file is can be quite small (compared to the store file that has the actual data) as it only requires two fields, the offset and the record's stored position. An index file is small enough that it can be added to a memory-map file and have operations on the file as fast as in-memory data operations.

Historically, logs are filled with text for humans to read but over time more and more logs are binary-encoded messages meant for other applications to read.

### Project Definitions 
- Record: the data stored in the log.
- Store: the file that stores the records.
- Index: the file that stores the index entries.
- Segment: the abstraction that connects together the index and the store.
- Log: the abstraction that connects all segments.