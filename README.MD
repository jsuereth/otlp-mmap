# OTLP Memory Mapped File Protocol

This is an experiment in using Memory Mapped Files as a (local) transport mechanism between a system being observed, and an out-of-band export of that observability data.

## Why mmap?

Using memory mapped files for export has drawbacks, but a few important upsides:

- Shared mmap file region can be used communicate across processes via simple memory concurrency primitives.
- Process death of the system being observed still allows the observability consumer to collect data. Think of this like a "black box" on an airplane.


A few principles:

- *Limited Persistence:* We do not (truly) care about persistence. This could leverage shared memory. However, persistence can be a benefit in the event the collection process dies and need to restart.
- *Concurrent Access:* We must assume at least 1 producer and at most 1 consumer of o11y data. All access to files should leverage memory safety primitves, and encourage direct page sharing between processes.
- *Fixed sized entries:* We start with fixed-size assumptions and can adapt/scale based on performance benchmarks.

## Docker-Compose Demo

1. Build otlp-mmap-collector image

```
cd rust
docker build . -t otel/otlp-mmap-collector:latest
```

2. Build java-demo-app image

```
cd java
cd otlp-mmap
docker build . -t otel/mmap-demo:latest
```

3. Run the desired demo

```
docker compose -f scenarios/{scenario}.yml up
```

The demo will send two batches of 100 spans from a java process through the otlp-mmap-collector to a normal collector.

## Running manually

To run the example outside of docker, do the following:

1. In one terminal, start a debug opentelemetry collector.

```
docker run   -p 127.0.0.1:4317:4317   -p 127.0.0.1:55679:55679   otel/opentelemetry-collector-contrib:0.111.0
```

2. In one terminal, go to the `java/otlp-mmap` directory.  Type `sbt run`
3. In another terminal go to the `rust` directory, Type `cargo run`.

You should see a Java (scala) program generating Spans and firing them into the `export` directory.  The Rust
program will be reading these spans and sending them via regular OTLP to the collector.

## Details

See [Protocol](PROTOCOL.MD) for details on the file contents and layout.

## Prototyping TODOs

- [ ] Throughput tests
- [ ] Benchmarks
    - Memory usage of traditonal otlp exporter vs. MMap-Writer + MMap-collector combined
    - Memory overhead of primary process
- File format experiements
  - [X] variable sized entry dictionary
  - [X] Metric file format
  - [ ] Evaluate Parquet
  - [ ] Evaluate STEF
- More Language Writers
  - [ ] Go
  - [ ] Python
- Deeper SDK hooks
  - [X] Directly keeping metric aggregations in mmap
  - [X] Directly writing span start/stop/event to ringbuffer
- Comparison w/ eBPF techniques