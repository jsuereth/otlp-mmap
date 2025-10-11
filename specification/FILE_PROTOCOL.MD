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

Ringbuffers are an array of fixed-size buffers that begin with the following 32-byte header:

| Field             | Size | Index | Type | Description                                        |
|-------------------|------|-------|------|----------------------------------------------------|
| num_buffers       | 8    | 0     | int64 | number of buffers in the ring buffer              |
| buffer_size       | 8    | 1     | int64 | size, in bytes, of a buffer in the ring buffers   |
| reader_position   | 8    | 2     | int64 | position of the current chunk about to be read    |
| writer_position   | 8    | 3     | int64 | TODO                                              |

The header is immediately followed by a int32 array representing buffer availability.

The size of the availability mask

Ringbuffers are designed to be lock-less multi-producer single-consumer (MPSC) queues. To support
multiple writers concurrently, we have mechanism that allows producers to claim unique slots atomically
and then publish those slots out of order.

Note: `writer_position` refers to the largest sequence claimed by *any* producer.

The current available capacity for writing in a ring buffer can be computed as:

```java
long capacity =
  (reader_position < writer_position) ? 
  reader_position + num_buffers - writer_position :
  reader_position - writer_position;
```

## Producer algorithm

Producers use an atomic "get and add" operation on `writer_position` to atomically claim the next n
available buffers. This sequence number serves as the guaranteed unique index for that specific message
or batch of messages.

This producer now owns this section of the ring-buffer.

Once a producer has finished writing data to its claimed slot, it atomically updates the availability
buffer entry at the corresponding index using a release store operation. This releases the buffer
from the producer.

## Consumer algorithm

The single consumer checks consecutive availability starting from its current reader_position. A buffer
is considered available only if the flag stored in the availability buffer matches the expected availability
 for that sequence number. The consumer increments its reader_position only when it finds the next sequence
 is available, ensuring strict ordering even if producers published out of sequence.





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

Dictionary entries are serialized protocol buffer messages (so they MUST begin with serialized size)

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

### Dictionary - Attribute Set

The attribute set entry contains a set of key-value attributes.

An attribute set is a repeated field of containing one of the following:

- A KeyValue pair
- A dictionary offset to another attribute set.