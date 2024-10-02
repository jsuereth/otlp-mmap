# OLTP MMAP Protocol

The OTLP mmap protocol works by opening and mmaping a set of files, preferring ring-buffers for export.

## Concept

OTLP MMAP protocol uses File I/O to communicate between the primary system and an exporter.  Unlike normal File I/O, files are bound to shared memory using mmap, and written using memory concurrency primtives.  On process death, a collector is able to still extract in-memory state and, unless the machine itself is killed, state will be persisted on disk for later retrieval.

## High Level Details

OTLP-mmap constructs a directory of files for export:

- `{output directory}`
  - `resource.otlp`: A Dictionary containing resource definitions
  - `scope.otlp`: A Dictionary containing scope definitions
  - `spans.otlp`: A Circular Buffer containing exported spans
  - `metrics.otlp`: A Timeseries file containing exported metric data points
  - `logs.otlp`: A Circular Buffer containing exported logs

### resource

TODO

### scope

TODO

### spans

TODO - ring buffer file, with scope/resource referenced by ID.

### metrics

TODO

### logs

TODO - ring buffer file, with scope/resource referenced by ID.

## Low Level Details

OTLP-mmap is built on lower-level primitives for file I/O operations, specifically:

- Circular buffers: A mechanism to push/pull messages using a mmapped file.
- Dictionary: A mechanism to look up messages via ID reference
- Timeseries file: A mechanism to store aggregated (or partially aggregated) data points and
  collect them at regular intervals.

### Circular Buffer files

Files which implement (fixed size) circular buffers contain a 64-byte header as follows:

| Field            | Size | Index | Type | Description                                        |
|------------------|------|-------|------|----------------------------------------------------|
| version          | 8    | 0     | int64 | version number of the OTLP export                 |
| num_chunks       | 8    | 1     | int64 | number of chunks in the ring buffer               |
| chunk_size       | 8    | 2     | int64 | size, in bytes, of chunks of ring buffers         |
| unused           | 24   |       |       | zeroed out bits                                   |
| writer_position  | 8    | 7     | int64 | position of the current_chunk about to be written |
| reader_position  | 8    | 6     | int64 | position of the current chunk about to be read    |

Remaining bytes are the actual fixed-size contents, e.g.

```
| header (64 bytes) | msg idx 0 | msg idx 1 | ... | msg idx {num_chunks-1} |
```

At any time, if `version` has changed, a "reader" of this file needs to reset its state and start fresh.

### Dictionary Files

TODO - How do we write dictionaries, like Resource / instrumentation scope.

### Timeseries files

TODO - columnar storage for metrics?  Attempt to store aggregate memory DIRECTLY on mmap file?