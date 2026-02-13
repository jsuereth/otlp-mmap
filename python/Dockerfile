# --- Test/Development Stage ---
FROM python:3.11-alpine

# Install Rust toolchain and required system dependencies for compilation
RUN apk add --no-cache rust cargo build-base python3-dev libffi-dev openssl-dev

# Install build tools and dependencies
RUN pip install --no-cache-dir maturin patchelf pytest pytest-benchmark requests flask opentelemetry-api opentelemetry-sdk hatchling

WORKDIR /app

# 1. Build Rust Bindings FIRST
COPY rust/Cargo.toml rust/Cargo.lock /app/rust/
COPY rust/crates /app/rust/crates/
WORKDIR /app/rust/crates/otlp-mmap-pybindings
RUN maturin build --release --out /wheels
RUN pip install /wheels/otlp_mmap_pybindings*.whl

# 2. Setup Python environment
WORKDIR /app/python
COPY python/otlp-mmap-sdk /app/python/otlp-mmap-sdk/
COPY python/otlp-mmap-example-server /app/python/otlp-mmap-example-server/
COPY python/tests /app/python/tests/
COPY python/pyproject.toml python/README.md /app/python/

# Install SDK and Example Server
RUN pip install -e ./otlp-mmap-sdk -e ./otlp-mmap-example-server

# Default to running tests
CMD ["python", "-m", "pytest"]
