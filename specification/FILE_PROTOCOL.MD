# SDK MMap File Protocol

Expanding on the OTLP-mmap protocol, we can look at a mechanism to put all SDK
output into a single, consumable file. Unlike normal log files, this would rotate
via ring-buffers and be designed for co-reading/writing where both sides can mmap
the file.  Additionally, much SDK processing would move to the "collector"
component vs. being in-process.

## Overview

The SDK MMap File protocol allows the SDK to write all telemetry data directly into
a file for better resiliency in the case of system crashing.


Generally, the MMAP File Protocol SHOULD do the following:

- events in the SDK *should* be written to preallocated memory via the use of mmapped file
  primitives.
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

The header will consist of the following 64-byte block.

| Field            | Size | Index | Type | Description                                        |
|------------------|------|-------|------|----------------------------------------------------|
| version          | 8    | 0     | int64 | version number of the OTLP export                 |
| events           | 8    | 1     | int64 | offset to event ringbuffer.                       |
| spans            | 8    | 2     | int64 | offest to span ringbufer.                         |
| measurements     | 8    | 2     | int64 | offset to measurement ringbuffer.                 |
| measurements     | 8    | 3     | int64 | offset to dictionary.                             |
| unused           | 16   |       |       | zeroed out bits                                   |


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

The header is 32-bytes and consists of the following:

| Field             | Size | Index | Type | Description                                        |
|-------------------|------|-------|------|----------------------------------------------------|
| num_buffers       | 8    | 0     | int64 | number of buffers in the ring buffer              |
| buffer_size       | 8    | 1     | int64 | size, in bytes, of a buffer in the ring buffers   |
| reader_index      | 8    | 2     | int64 | index of the number of events the reader has read |
| writer_index      | 8    | 3     | int64 | index of the number of events claimed by writers  |

The header is immediately followed by a int32 array representing buffer availability. The length of
the availability array is equal to number of buffers in the ring buffer.

The availibility array is then followed by the actual buffers themselves.

Ringbuffers are designed to be lock-less multi-producer single-consumer (MPSC) queues. To support
multiple writers concurrently, we have mechanism that allows producers to claim unique slots atomically
and then publish those slots out of order.

This means that we follow a "claim-then-commit" approach to any particular buffer in the ring.

All index values are monotonically increasing sequences (vs. current index in the ringbuffer). We
use this property to understand, via the availability array, whether any particular buffer is ready
to be read.

Note: Ringbuffers *must* be have size equal to a power-of-two, to optimise latency-sensitive math used in hot-paths.

#### Producer Algorithm

First a producer attempts to claim the next `writer_position` (atomically). 
This sequence number serves as the guaranteed unique index for the buffer.
This should have a capacity check.  An example algorithm:

```rust
fn tryObtainNextWrite() -> Option<i64> {
  let current = header.write_position.load(Ordering::Relaxed)
  let next = current + 1
  let reader = header.read_position.load(Ordering::Acquire)
  let hasCapacity = {
    let previousIndexWithConflict = current + 1 - header.num_buffers
    previousIndexWithConflict < reader
  }
  if hasCapacity && header.write_position.compare_exchange(
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
buffer entry at the corresponding index using a release store operation. This releases the buffer
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
  (idx >>> shift) as i32
}
fn setAvailable(idx: i64) {
  availability_array[ringIndex(idx)] = availabilityFlagForIndex(idx)
}
```

#### Consumer algorithm

The single consumer checks consecutive availability starting from its current reader_position. A buffer
is considered available only if the flag stored in the availability buffer matches the expected availability
 for that sequence number. 
 
 The consumer increments its reader_position only after it reads the next sequence, ensuring producers do not overwrite data it is reading.

An example check for availability:

```rust
fn tryObtainNextRead() -> Option<i64> {
  let reader = header.read_position.load(Ordering::Acquire)
  let writer = header.write_position.load(Ordering::Acquire)
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
  availability_array[idx] == flag
}
```

Note: When done reading, make sure to increment the current read index
to match the value returned from `tryObtainNextRead`.

### Dictionary

The dictiionary is a large, variable sized, collection of data types.

The type of any particularly entry can only be determined via a ringbuffer association.  For example, the `Measurement` ringbuffer will include a reference to a "resource" in the dictionary, which may also reference an "attribute set".


The dictionary begins with a 64-byte header:

| Field            | Size | Index | Type  | Description                                        |
|------------------|------|-------|-------|----------------------------------------------------|
| end              | 8    | 1     | int64 | index after the last entry was written.                |
| num_entries      | 8    | 2     | int64 | the number of entries written.                     |
| unused           | 48   |       |       | zeroed out bits                                    |

When interacting with the dictionary, all references are *offsets from the start of the file*.

Dictionary entries are serialized protocol buffer messages (so they MUST begin with serialized size, as a varint).

## Specific Types

### Ringbuffer - Events

TODO

### Ringbuffer - Spans

The span metric ringbuffer contains "span" events.  These events contain:

- scope_ref: Dictionary offset to an InstrumentationScope
- trace id: TODO
- span id: TODO
- one of the following:
  - Start Span Event
    - span name
    - timestamp
    - parent_span_id
    - span flags
    - span kind
    - attribute_ref: Dictionary offset to an Attribute Set.
  - Change Span Name Event
    - span name
  - Update Attributes Event
    - attribute_ref: Dictionary offset to additional attributes.
  - End Span Event
    - timestamp
  - Add Span Link Event
    - other trace id
    - other span id
    - attributes_ref

### RingBuffer - Metrics

The core metric ringbuffer contains "measurement" events. These events contain:

- scope_ref: Dictionary offset to InstrumentationScope definition.
- metric_ref: Dictionary offset to metric definition.
- span_context: Current span context
- timestamp: Current timestamp of the measurement.
- timeseries_ref: Dictionary offset to Attribute set.
- value: The value being recorded (integer or double)
- TODO - "additional attributes"?


### Dictionary - InstrumentationScope

The core instrumentation scope entry contains the following:

- resource_ref: Dictionary offset to a resource.
- name: name of the instrumentation scope
- version: the version of the instrumentation scope
- schema_url: the schema url of the instrumentation scope.
- attributes_ref: Dictionary offset to an attribute set.

### Dictionary - Resource

TODO

### Payload - Attribute Set

The attribute set entry contains a set of key-value attributes.

An attribute set is a repeated field of containing one of the following:

- A KeyValue pair
- A dictionary offset to another attribute set.

### Payload - KeyValue pair

A key value pair is the following:

- A dictionary offset to the key (a string).
- The value (serialized in proto)