# Log Lib

Go example log service with gRPC.

### Project Definitions 
- Record: the data stored in the log.
- Store: the file that stores the records.
- Index: the file that stores the index entries.
- Segment: the abstract layer that connects together the index and the store.
- Log: the abstract layer that connects all segments.