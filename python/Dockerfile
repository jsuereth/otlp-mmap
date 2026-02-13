# --- Test/Development Stage ---
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install build tools
RUN pip install --no-cache-dir maturin pytest requests flask opentelemetry-api opentelemetry-sdk hatchling

WORKDIR /app

# Copy necessary Rust files
COPY rust/Cargo.toml rust/Cargo.lock /app/rust/
COPY rust/crates /app/rust/crates/

# Copy Python files
COPY python/otlp-mmap-sdk /app/python/otlp-mmap-sdk/
COPY python/otlp-mmap-example-server /app/python/otlp-mmap-example-server/
COPY python/tests /app/python/tests/
COPY python/pyproject.toml python/README.md /app/python/

# Build and install internal bindings
WORKDIR /app/python
RUN maturin build -m ../rust/crates/otlp-mmap-pybindings/Cargo.toml --out /wheels
RUN pip install /wheels/otlp_mmap_pybindings*.whl

# Install SDK and Example Server in editable mode for testing
RUN pip install -e ./otlp-mmap-sdk -e ./otlp-mmap-example-server

# Run tests
RUN python -m pytest
