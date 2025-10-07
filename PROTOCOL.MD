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
  - `metrics.otlp`: A dictionary containing metric names.
  - `metrics_series.otlp`: A Dictionary containing attributes for timeseries that are recorded.
  - `metrics_points.otlp`: A circular Buffer containing data points
  - `logs.otlp`: A Circular Buffer containing exported logs

### resource

Resources are written to a Dictionary file as length-encoded protobufs. They use the 
[Resource](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/resource/v1/resource.proto#L28) protobuf definition in OTLP.

The file name is always `resource.otlp` and is shared between signals.

TODO(jsuereth): These should support `schema_url`.

### scope

InstrumentationScopeInfo are written to a Dictionary file as length-encoded protobufs. They use the
[InstrumentationScope](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto#L71) protobuf definition in OTLP.

The file name is always `scope.otlp` and is shared between signals.

TODO(jsuereth): These should support `schema_url`.

### spans

Spans are written to a Ringbuffer file as length-encoded protobufs.  They use a custom protobuf definition:

```protobuf
message SpanRef {
  // The index reference of the resource in the resource dictionary file.
  required int64 resource_ref = 1;
  // The index reference of the resource in the resource dictionary file.
  required int64 scope_ref = 2;
  // The actual span of interest.
  required opentelemetry.proto.trace.v1.Span span = 3;
}
```

The file name is always `spans.otlp`.

## metrics

A dictionary of metric definitions.

```protobuf
message Metric {
  required string name = 1;
  required string description = 2;
  required string unit = 3;
  required InstrumentType instrument = 4;
}
```

### metrics time series

Time series are written as a dictionary of metric name and attribute.

```protobuf
message TimeSeries {
  required int64 metric_ref = 1;
  repeated KeyValue attributes = 2;
}
```

### metrics data points

A ringbuffer of points.

```protobuf
message DataPoint {
  required int64 timeseries_ref = 1;
  oneof point {
    ...
  }
}
```

### logs

Logs are written to a Ringbuffer file as length-encoded protobufs.  They use a custom protobuf definition:

```protobuf
message LogRef {
  // The index reference of the resource in the resource dictionary file.
  required int64 resource_ref = 1;
  // The index reference of the resource in the resource dictionary file.
  required int64 scope_ref = 2;
  // The actual span of interest.
  required opentelemetry.proto.logs.v1.LogRecord log = 3;
}
```

The file name is always `logs.otlp`.

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
| reader_position  | 8    | 6     | int64 | position of the current chunk about to be read    |
| writer_position  | 8    | 7     | int64 | position of the current_chunk about to be written |

Remaining bytes are the actual fixed-size contents, e.g.

```
| header (64 bytes) | msg idx 0 | msg idx 1 | ... | msg idx {num_chunks-1} |
```

At any time, if `version` has changed, a "reader" of this file needs to reset its state and start fresh.

### Fixed Size Dictionary Files

Files which implement a dictionary have the following 64-byte header:

| Field            | Size | Index | Type | Description                                        |
|------------------|------|-------|------|----------------------------------------------------|
| version          | 8    | 0     | int64 | version number of the OTLP export                 |
| num_entries      | 8    | 1     | int64 | the number of entry chunks written                |
| chunk_size       | 8    | 2     | int64 | The size of each entry (all same size)            |
| unused           | 40   |       |       | zeroed out bits                                   |

Dictionary entries are fixed size and written sequentially:

```
| header (64 bytes) | entry 1 | entry 2 | ... | entry N-1 |
```


### Variable Sized Dictionary Files

Files which implement a variable sized dictionary have the following 64-byte header:

| Field            | Size | Index | Type | Description                                        |
|------------------|------|-------|------|----------------------------------------------------|
| version          | 8    | 0     | int64 | version number of the OTLP export                 |
| unused           | 48   |       |       | zeroed out bits                                   |
| writer_position  | 8    | 7     | int64 | byte offset of the next unused entry chunk.       |

  
Dictionary entries are fixed size and written sequentially:

```
| header (64 bytes) | entry 1 | entry 2 | ... | entry N-1 |
```

TODO(jsuereth) - Think about a sparse index.

Note: When referencing a Variable-Sized dictionary, the offset (in bytes) from the start of the file
is used as the index into the dictionary.

### Timeseries files

TODO - columnar storage for metrics?  Attempt to store aggregate memory DIRECTLY on mmap file?