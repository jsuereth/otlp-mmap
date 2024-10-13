# OTLP Memory Mapped File Protocol

This is an experiment in using Memory Mapped Files as a (local) transport mechanism between a system being observed, and an out-of-band export of that observability data.

## Why mmap?

Using memory mapped files for export has drawbacks, but a few important upsides:

- Shared mmap file region can be used communicate across processes via simple memory concurrency primitives.
- Process death of the system being observed still allows the observability consumer to collect data. Think of this like a "black box" on an airplane.


A few principles:

- *Limited Persistence:* We do not (truly) care about persistence. This could leverage shared memory.  However, persistence
- *Concurrent Access:* We must assume at least 1 producer and at most 1 consumer of o11y data. All access to files should leverage memory safety primitves, and encourage direct page sharing between processes.
- *Fixed sized entries:* We start with fixed-size assumptions and can adapt/scale based on performance benchmarks.


## Example

To run the current example, do the following:

1. In one terminal, go to the `java/otlp-mmap` directory.  Type `sbt run`
2. In another terminal go to the `rust` directory, Type `cargo run`.

You should see a Java (scala) program generating Spans and firing them into the `export` directory.  The Rust
program will be reading these spans and printing information about them.
   

## Details

See [Protocol](PROTOCOL.MD) for details on the file contents and layout.