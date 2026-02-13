# Python Example

This implements a Python OpenTelemetry SDK by passing important
methods into rust.

## Architecture

We depend on the `otlp-mmap-pybindings` rust crate (located in `rust/crates/otlp-mmap-pybindings`), and pass all important OpenTelemetry SDK methods into this crate.

## Development

This project uses a Rust extension for performance, integrated with Python using `maturin`. Development and testing are performed within a Docker environment to ensure consistency.

### Prerequisites

*   Docker installed and running.

### Build and Test Workflow

The Dockerfiles are configured to run tests during the build process. If the build succeeds, the tests have passed.

1.  **Run All Tests (Development Image):**
    This image builds the internal Rust bindings and runs all Python tests for the SDK and Example Server.

    ```bash
    docker build -t python-mmap-tests -f python/Dockerfile .
    ```

2.  **Build and Test the Example Server (Production-ready Image):**
    This multi-stage build performs testing in the first stage and produces a clean, minimal image in the second stage.

    ```bash
    docker build -t python-otlp-mmap-server -f python/otlp-mmap-example-server/Dockerfile .
    ```

## Example HTTP Server

A Flask-based example server is available in `otlp-mmap-example-server`. It can be run using Docker and configured to use either the standard OTLP exporter or the OTLP MMAP SDK.

### 1. Build the Example Server Image

Navigate to the project root and run:

```bash
docker build -f python/otlp-mmap-example-server/Dockerfile -t python-otlp-mmap-server .
```

### 2. Run with OTLP MMAP SDK

To run the server writing to a memory-mapped file:

```bash
docker run --rm -it \
  -e SDK_MMAP_EXPORTER_FILE="/tmp/mmap_data" \
  -e HTTP_ENDPOINT_PORT=5000 \
  -p 5000:5000 \
  python-otlp-mmap-server
```

The server will write telemetry data to `/tmp/mmap_data` inside the container. You can mount a volume to inspect this file on your host.

### 3. Run with Standard OTLP Exporter

To run the server sending data to an OTLP endpoint (e.g., a local collector):

```bash
docker run --rm -it \
  -e OTEL_EXPORTER_OTLP_ENDPOINT="http://host.docker.internal:4317" \
  -e HTTP_ENDPOINT_PORT=5000 \
  -p 5000:5000 \
  python-otlp-mmap-server
```
