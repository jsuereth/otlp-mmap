# Stage 1: Builder & Tester
FROM python:3.9-slim as builder

ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install build tools
RUN pip install maturin hatchling editables pytest requests flask opentelemetry-api opentelemetry-sdk

WORKDIR /app

# Copy necessary Rust files
COPY rust/Cargo.toml rust/Cargo.lock /app/rust/
COPY rust/crates /app/rust/crates/

# Copy Python files
COPY python/otlp-mmap-sdk /app/python/otlp-mmap-sdk/
COPY python/otlp-mmap-example-server /app/python/otlp-mmap-example-server/
COPY python/tests /app/python/tests/
COPY python/pyproject.toml python/README.md /app/python/

# Build otlp-mmap-internal wheel
WORKDIR /app/python
RUN maturin build -r -m ../rust/crates/otlp-mmap-pybindings/Cargo.toml --out /wheels

# Install internal wheel to build/test other packages
RUN pip install /wheels/otlp_mmap_pybindings*.whl

# Build SDK wheel (no-deps because internal is already installed for tests, 
# and will be installed from wheel in final image)
RUN pip wheel --no-deps --wheel-dir /wheels ./otlp-mmap-sdk

# Build Server wheel
RUN pip wheel --no-deps --wheel-dir /wheels ./otlp-mmap-example-server

# --- Testing ---
# Install the built wheels for testing
# We need to install with dependencies here so pytest can run
RUN pip install /wheels/otlp_mmap_sdk*.whl /wheels/otlp_mmap_example_server*.whl

# Run all python tests
RUN python -m pytest

# Stage 2: Runner
FROM python:3.9-slim

WORKDIR /app

# Copy wheels from builder
COPY --from=builder /wheels /wheels

# Install all wheels
# This will install internal, sdk, server and pull in runtime dependencies (Flask, OTel, etc.) from PyPI
RUN pip install /wheels/*.whl && rm -rf /wheels

# Default Env Vars
ENV HTTP_ENDPOINT_PORT=5000

# Expose port
EXPOSE 5000

# Entrypoint - run the example server module
CMD ["python", "-m", "otlp_mmap_example_server.app"]
