# OTLP Memory Mapped File Protocol

This is an experiment in using Memory Mapped Files as a (local) transport mechanism between a system being observed, and an out-of-band export of that observability data.

## Why mmap?

Using memory mapped files for export has drawbacks, but a few important upsides:

- Shared mmap file region can be used communicate across processes via simple memory concurrency primitives.
- Process death of the system being observed still allows the observability consumer to collect data. Think of this like a "black box" on an airplane.

See also:

- [Protocol Specification](specification/FILE_PROTOCOL.MD)
- [Benchmarks](benchmarks/README.MD)
- [mmap-collector implementation](rust)
- [Try it out](#try-it-yourself)
- [Frequently Asked Questions](FAQS.md)

## Principles

The design of otlp-mmap is guided by the following:

- *Limited Persistence:* We do not (truly) care about persistence. This could leverage shared memory. However, persistence can be a benefit in the event the collection process dies and need to restart.
- *Concurrent Access:* We must assume at least 1 producer and at most 1 consumer of o11y data. All access to files should leverage memory safety primitves, and encourage direct page sharing between processes.
- *Fixed sized buffers:* We start with fixed-size assumptions and can adapt/scale based on performance benchmarks.
  This is to avoid forcing an ever growing file and requiring file-rotations and detecting file-truncation, as
  is done in most log-based observability collection today.
- *SDK makes all the decisions*: We still require the SDK to instantiate the mmapped-file and determine its size and
  characteristics. While an `mmap-collector` component may have performance related configuration, it should be fully
  reactive to the size configuration from the SDK. Any OTEL file-based configuration support should find a way to flow
  from an `mmap-sdk` through the mmap file into the `mmap-collector`
- *Shared description*: The OTLP-mmap file is *not* a self-describing format that could encode any possible data.
  Instead, the definition of data it passes MUST be known ahead of time.

## Results

See our [Benchmarks](benchmarks/README.MD) for the current status.

Today, the following is true:

- (For Java) using `mmap-sdk` + `mmap-collector` results in less Memory usage, higher CPU usage and little impact
  to throughput against an SDK configured with reasonable batching.
- `mmap-sdk` + `mmap-collector` have dramatically increased performance of "synchronous network export" - which would
  be the direct alternative way in OpenTelemetry today for getting data out of process quickly. This means
  *for batch jobs*, this may be a MUCH more efficient mechanism of getting data out.

## Try it yourself

You can run any of the docker compose demos found in the `scenarios` directory.

```
docker compose -f scenarios/{scenario}.yml up
```

Note: These all require a running on a disk where MMAP pages will be *local* to the machine running them.

### MMAP SDK Demo

The `mmap-sdk.docker-compose.yml` demo provides a simple example that will:

- Spin up an OpenTelemetry collector with traditional OTLP ingestion.
- Spin up a Java process that fires N (~200) spans out via the MMAP SDK.
- Spin up the MMAP collector to process these spans and fire them at an OpenTelemetry Collector via OTLP.

This demo demonstrates the applicability of using MMAP files across containers and leveraging atomic memory
operations for communication between processes in these containers.

### MMAP SDk vs. Traditional SDK Comparison

The `mmap-sdk-vs-pure-sdk.docker-compose.yml` demo provides great insight into the performance
characteristics of the MMAP SDK on larger servers. This demo will:

- Set up two java HTTP servers, one with traditional SDK and another with MMAP SDK.
- Initiate the same k6 load test on the HTTP servers.
- Record all metrics/spans/events from these servers in an LGTM container.
- Record cadvisor metrics from these servers into an LGTM container.

You can view collected metrics at http://localhost:3000/ via Grafana.

This is an ideal test for checking *pure overhead* of using MMAP vs. a traditional SDK. This is because the Java HTTP server does *very little* so most deviations in latency, CPU or Memory usage is purely from the overhead of instrumentation and OpenTelemetry.
This will *not* give accurate numbers on a real-world HTTP server for overhead, but instead can be used to find bottlenecks,
assess macro-performance issues (e.g. cpu contention) and otherwise tune the MMAP SDK.

### Building images locally

You can also build the images locally as follows:

1. Build mmap-collector image

```
cd mmap_collector
docker build . -t ghcr.io/jsuereth/mmap-collector:main
```

2. Build java-demo-app image

```
cd java
cd otlp-mmap
docker build . -t ghcr.io/jsuereth/mmap-demo:main
```

### Running manually

To run the example outside of docker, do the following:

1. In one terminal, start a debug opentelemetry collector.

```
docker run   -p 127.0.0.1:4317:4317   -p 127.0.0.1:55679:55679   otel/opentelemetry-collector-contrib:0.111.0
```

2. Set the ENV variable, e.g. `export SDK_MMAP_EXPORTER_FILE=/path/to/mmap.otlp`
3. Run the `java/otlp-mmap` server: `sbt run`
4. With the same ENV variable, inside `mmap-collector` directory, Type `cargo run`.

You should see a Java (scala) program generating Spans and firing them into the `export` directory.  The Rust
program will be reading these spans and sending them via regular OTLP to the collector.

## Details

See [Protocol](PROTOCOL.MD) for details on the file contents and layout.

## Prototyping TODOs

- [X] Throughput tests
  - Basic k6 test for a server
  - Comparison on CPU/Mem usage vs. Latency
  - [ ] Max throughput tests
- [ ] Benchmarks
    - Traditonal (batch) otlp exporter vs. MMap-Writer + MMap-collector combined
      - CPU usage
      - Memory overhead of primary process
      - Garbage Collection pressure
    - Figure out if we have "quick wins" in synchronous event export path in Java MMAP SDK.
- File format experiements
  - [X] variable sized entry dictionary
  - [X] Metric file format
  - [ ] Evaluate Parquet
  - [X] Evaluate STEF
- More Language Writers
  - [ ] Go
  - [ ] Python
- Deeper SDK hooks
  - [X] Directly keeping metric aggregations in mmap
  - [X] Directly writing span start/stop/event to ringbuffer
  - [ ] Use instrument hints in metric aggregations in mmap.
- Resiliency
  - [ ] Detect File resets
  - [ ] MMAP Collector retry-batch
  - [ ] Restart MMap collector when needed
- Comparison w/ eBPF techniques