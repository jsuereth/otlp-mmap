# OLTP MMAP Protocol

The OTLP mmap protocol works by opening and mmaping a set of files, preferring ring-buffers for export.

## Export metadata

A file called: `export.meta` in the export location.  It has the following format:

| Field            | Size | Index | Type | Description                                         |
|------------------|------|-------|------|-----------------------------------------------------|
| version          | 8    | 0     | int64 | version number of the OTLP export                 |
| ring_buffer_size | 8    | 1     | int64 | size, in bytes, of chunks of ring buffers         |
| num_chunks       | 8    | 2     | int64 | number of chunks in the ring buffer               |
| unused           | 16   |       |        | zeroed out bits                                   |
| checksum         | 8    | 5     | int64 | CRC checksum of previous fields.                  |
| writer_position  | 8    | 6     | int64 | position of the current_chunk about to be written |
| reader_position  | 8    | 7     | int64 | position of the current chunk about to be read    |



Total header size = 64 bytes

TODO - should we export resource / scope separately?