# Frequently Asked Questions

Below are some answers to questions asked about this
project.

## Will this support Python?

Yes. One of the next prototypes is to build a Python SDK
that supports the MMap protocol.

## Why isn't this just a shared native library?

A shared native library does NOT solve the key problem we
want to address of ensuring observability data is able to
be collected even during a process crash. In general, the
goal for any MMAP-SDK is to acheive the following:

- Signals can leverage pre-allocated memory, allowing
  an SDK to report signals even during an OOM event.
- MMAP-SDKs should avoid as much local buffering as
  possible, prefering to directly leverage the MMAP
  buffers, ensuring data is recoverable on a crash.

A shared native library *could* be used to implement an
MMAP-SDK, but would still require an MMAP-Collector. This
may be investigated in the future, but for prototyping
purposes, isn't as high a priority as investigating the
performance implications of large shared-memory regions
between processes.

## Why Protocol Buffers?

Protocol buffers were chosen for the following:

- Good support across languages
- Easy to do length-delimited writes (i.e. length)
  of variable sized content preceedes the content.
- Good compression of common data via variable-integers.

This choice needs to be balanced against the other goals /
priorities of OTLP-MMAP, e.g. ensuring that signals written
*during* a process crash can still make it out. Protocol
Buffers, if not careful, can cause memory allocations or computations that may prevent the core objective. This will
be evalauted during benchmarking.

## Why not STEF?

For context, See [Sequential Tabular Exchange Format](https://github.com/splunk/stef).

STEF is a bi-directional, stateful protocol. Both the
client and the server can participate in decisions, e.g.
resetting dictionaries and deciding when compression
is complete. It is optimised for point-to-point
communication and makes that highly efficient. STEF
is ill-suited to the goals of this project for a few
reasons:

- A dictionary reset from the mmap-collector may cause
  a mass of reallocations in an mmap-sdk, which would be
  problematic *during* an OOM error.
- OTLP-MMAP is *not* designed for bi-directional
  communication. Currently, the ringbuffers are
  output only, with the only communication from an
  `mmap-collector` to an `mmap-sdk` being the setting
  of a single integer via an *atomic* operation. STEF
  would require a full bidirectional protocol.

We believe STEF may be an ideal format for an
`mmap-collector` to report data into an `otel-collcetor`,
but is ill-suited to the goals of OTLP-MMAP.

Some of the insights in STEF into structuring binary data
*may* be applicable and could wind up in OTLP-MMAP.
However, as benchmarking is showing, we want to further
limit / reduct the synchronization needed in OTLP-MMAP
ringbuffers, not increase the dependency on this
capability.