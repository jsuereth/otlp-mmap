# --- Build Stage ---
FROM python:3.11-alpine AS builder

# Install build tools
RUN apk add --no-cache rust cargo build-base python3-dev libffi-dev openssl-dev
RUN pip install maturin hatchling patchelf pytest requests flask opentelemetry-api opentelemetry-sdk opentelemetry-instrumentation-flask opentelemetry-exporter-otlp

WORKDIR /build

# 1. Build Rust Bindings FIRST in its own layer
COPY rust/Cargo.toml rust/Cargo.lock /build/rust/
COPY rust/crates /build/rust/crates/
WORKDIR /build/rust/crates/otlp-mmap-pybindings
RUN maturin build --release --out /wheels

# 2. Build Python SDK
# Install the built internal wheel so SDK can be tested if needed
RUN pip install /wheels/otlp_mmap_pybindings*.whl

WORKDIR /build/python
COPY python/otlp-mmap-sdk /build/python/otlp-mmap-sdk/
RUN pip wheel --no-deps --wheel-dir /wheels ./otlp-mmap-sdk

# 3. Build Example Server
COPY python/otlp-mmap-example-server /build/python/otlp-mmap-example-server/
RUN pip wheel --no-deps --wheel-dir /wheels ./otlp-mmap-example-server

# --- Testing Stage ---
WORKDIR /build/tests
COPY python/tests /build/tests/
RUN pip install /wheels/otlp_mmap_sdk*.whl /wheels/otlp_mmap_example_server*.whl
RUN python -m pytest

# --- Runner Stage ---
FROM python:3.11-alpine
WORKDIR /app

# Copy wheels from builder
COPY --from=builder /wheels /wheels

# Install all wheels
# This will install internal, sdk, server and pull in runtime dependencies from PyPI
RUN pip install /wheels/*.whl && rm -rf /wheels

# Default Env Vars
ENV HTTP_ENDPOINT_PORT=5000
EXPOSE 5000

# Entrypoint - run the example server module
CMD ["python", "-m", "otlp_mmap_example_server.app"]
