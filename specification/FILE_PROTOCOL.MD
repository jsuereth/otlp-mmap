# SDK MMap File Protocol

Expanding on the OTLP-mmap protocol, we can look at a mechanism to put all SDK
output into a single, consumable file. Unlike normal log files, this would rotate
via ring-buffers and be designed for co-reading/writing where both sides can mmap
the file.  Additionally, much SDK processing would move to the "collector" component rather than being in-process.

## Overview

The SDK MMap File protocol allows the SDK to write all telemetry data directly into
a file for better resiliency in the case of system crashes.

Generally, the MMAP File Protocol SHOULD do the following:

- events in the SDK *should* be written to preallocated memory using memory-mapped file primitives.
- A dictionary is used to record things which do not change often or can be re-used across
  many events (e.g. attribute sets).

The file is laid out as follows:

```
| Header | RingBuffers | Dictionary |
```

The header describes where each section can be found.  RingBuffers, while having configurable
size at startup, cannot be modified (in size) after the header is written. These are found
after the header and before the dictionary.  The dictionary makes up the tail of the file and
can grow based on what attributes are encountered in the SDK.

Note: there is currently no way to "clean" the dictionary, outside of opening a new file.


### Header

The header will consist of the following 64-byte block. Fields that are updated by the SDK after initialization (e.g., offsets and timestamps) MUST be accessed using atomic operations to ensure safety across multiple processes.

| Field            | Size | Offset | Type | Description                                        |
|------------------|------|--------|------|----------------------------------------------------|
| version          | 8    | 0      | int64 | Version number of the OTLP export.                 |
| events           | 8    | 8      | int64 | Offset to event ring buffer.                       |
| spans            | 8    | 16     | int64 | Offset to span ring buffer.                         |
| measurements     | 8    | 24     | int64 | Offset to measurement ring buffer.                 |
| dictionary       | 8    | 32     | int64 | Offset to dictionary.                             |
| time_unix_nano   | 8    | 40     | uint64| Time when SDK started.                             |
| unused           | 16   | 48     |       | Reserved for future use. Must be zeroed.           |


### Ringbuffers

There will be a series of ringbuffers in the file, with the following
order:

- **Events**: raw events (from Logger or active Span).
- **Spans**: events representing span lifecycle.
- **Measurements**: events represnting a measurement (metric).

Ringbuffers are an array laid out as follows:

```
| header | availability array | buffer 1 | buffer 2 | ... |
```

The header is 32-bytes and consists of the following. The `reader_index` and `writer_index` fields MUST be accessed using atomic operations.

| Field             | Size | Offset | Type | Description                                        |
|-------------------|------|--------|------|----------------------------------------------------|
| num_buffers       | 8    | 0      | int64 | Number of buffers in the ring buffer.              |
| buffer_size       | 8    | 8      | int64 | Size, in bytes, of a buffer in the ring buffers.   |
| reader_index      | 8    | 16     | int64 | Index of the number of events the reader has read. |
| writer_index      | 8    | 24     | int64 | Index of the number of events claimed by writers.  |

Note: Upon initialization of a new file, both `reader_index` and `writer_index` MUST be set to `-1`. The entire availability array MUST be filled with a sentinel value of `-1`.

The header is immediately followed by a an `int32` array representing buffer availability. The length of
the availability array is equal to number of buffers in the ring buffer.

The availability array is then followed by the actual buffers themselves.

Ringbuffers are designed to be lock-less multi-producer single-consumer (MPSC) queues. To support
multiple writers concurrently, we have mechanism that allows producers to claim unique slots atomically
and then publish those slots out of order.

This means that we follow a "claim-then-commit" approach to any particular buffer in the ring.

All index values are monotonically increasing sequences (vs. current index in the ringbuffer). We
use this property to understand, via the availability array, whether any particular buffer is ready
to be read.

Note: Ring buffers *must* have size equal to a power-of-two, to optimise latency-sensitive math used in hot-paths.

#### Producer Algorithm

First a producer attempts to claim the next `writer_index` (atomically). 
This sequence number serves as the guaranteed unique index for the buffer.
This should have a capacity check.  An example algorithm:

```rust
fn tryObtainNextWrite() -> Option<i64> {
  let current = header.writer_index.load(Ordering::Relaxed)
  let next = current + 1
  let reader = header.reader_index.load(Ordering::Acquire)
  let hasCapacity = {
    let previousIndexWithConflict = current + 1 - header.num_buffers
    previousIndexWithConflict < reader
  }
  if hasCapacity && header.writer_index.compare_exchange(
    current, next, Ordering::SeqCst, Ordering::Relaxed
  ) {
    Some(next)
  } else {
    None
  }
}
```

This producer now owns this section of the ring-buffer.

Once a producer has finished writing data to its claimed slot, it atomically updates the availability
buffer entry at the corresponding index using a release store operation, which releases the buffer
from the producer.

This can be done as follows:

```rust
// Assume we have an `indexMask` of `num_buffers-1`
// Assume we have a `shift` of `log2(num_buffers)`
fn ringIndex(idx: i64) -> usize {
  // Note this is the equivalent of
  // (idx % num_buffers) as usize
  (idx & indexMask) as usize
}
fn availabilityFlagForIndex(idx: i64) -> i32 {
  // Note: this should be a logical (zero-fill) right shift.
  (idx >>> shift) as i32
}
fn setAvailable(idx: i64) {
  availability_array[ringIndex(idx)] = availabilityFlagForIndex(idx)
}
```

#### Consumer algorithm

The single consumer checks consecutive availability starting from its current `reader_index`. A buffer
is considered available only if the flag stored in the availability buffer matches the expected availability
 for that sequence number. 
 
The consumer increments its `reader_index` only after it reads the next sequence, ensuring producers do not overwrite data it is reading.

An example check for availability:

```rust
fn tryObtainNextRead() -> Option<i64> {
  let reader = header.reader_index.load(Ordering::Acquire)
  let writer = header.writer_index.load(Ordering::Acquire)
  let next = reader + 1
  if isReadable(next) {
    Some(next)
  } else {
    None
  }
}

fn isReadable(idx: i64) -> bool {
  let ridx = ringIndex(idx)
  let flag = availabilityFlagForIndex(idx)
  availability_array[ringIndex(idx)] == flag
}
```
Note: When done reading, ensure the current read index is incremented
to match the value returned from `tryObtainNextRead`.

### Dictionary

The dictionary is a large, variable sized, collection of data types.

The type of any particular entry can only be determined via a ring buffer association.  For example, the `Measurement` ringbuffer will include a reference to a "resource" in the dictionary, which may also reference an "attribute set".


The dictionary begins with a 64-byte header. The `end` and `num_entries` fields MUST be accessed using atomic operations.

| Field            | Size | Offset | Type  | Description                                        |
|------------------|------|--------|-------|----------------------------------------------------|
| end              | 8    | 0      | int64 | Offset of the end of the last written entry.       |
| num_entries      | 8    | 8      | int64 | The number of entries written.                     |
| unused           | 48   | 16     |       | Reserved for future use. Must be zeroed.           |

Note: Dictionary offset `0` is reserved for empty/null values. All valid entries will have an offset greater than zero.

When interacting with the dictionary, all references are *offsets from the start of the file*.

Dictionary entries are serialized Protocol Buffer messages (they MUST begin with a serialized size as a varint).

## Specific Types

### Ringbuffer - Events

The event ringbuffer is composed of events that match the `Event` message in
`mmap.proto`.

### Ringbuffer - Spans

The span event ring buffer contains `SpanEvent` messages in `mmap.proto`.

### RingBuffer - Metrics

The core metric ring buffer contains `Measurement` messages from `mmap.proto`.

### Dictionary - Instrumentation Scope

Anywhere a `scope_ref` or `instrumentation_scope_ref` is defined, you
should expect a dictionary entry at this location which is a
serialized `InstrumentationScope` message from `mmap.proto`.

### Dictionary - Resource

Anywhere a `resource_ref` is defined, you should expect a dictionary
entry at this location which is a serialized `Resource` message from
`mmap.proto`.

### Dictionary - Strings

Anywhere a `key_ref` is defined, you should expect a dictionary entry at this location which is a length-delimited, serialized UTF-8 `String`.

This is also used for `name_ref`, `version_ref`, and `event_name_ref`.

Note: The `ValueRef` field in `AnyValueRef` (and `AnyValue`) allows for dictionary encoding of strings and other complex types. However, this is currently **unused** in all implementations to avoid unpredictable growth of the dictionary. Future versions may employ heuristics to selectively intern **low-cardinality** string values (where many occurrences share few unique values) to optimize file size without dictionary bloat.

### Dictionary - MetricDefinitions

Anywhere a `metric_ref` is defined, you should expect a dictionary
entry at this location which is a `MetricRef` message from `mmap.proto`.
