# Stage 1: Builder
FROM python:3.9-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install build tools
RUN pip install maturin hatchling editables

WORKDIR /app
COPY . .

# Build otlp-mmap-internal wheel
RUN maturin build -r -m Cargo.toml --out /wheels

# Install internal wheel to build other packages
RUN pip install /wheels/*.whl

# Build SDK wheel
# Use pip wheel with no build isolation to use the installed internal package
RUN pip wheel --no-build-isolation --wheel-dir /wheels ./otlp-mmap-sdk

# Install SDK wheel to build server
RUN pip install /wheels/otlp_mmap_sdk*.whl

# Build Server wheel
RUN pip wheel --no-build-isolation --wheel-dir /wheels ./otlp-mmap-example-server

# Stage 2: Runner
FROM python:3.9-slim

WORKDIR /app

# Copy wheels from builder
COPY --from=builder /wheels /wheels

# Install all wheels
# This will install internal, sdk, server and pull in runtime dependencies (Flask, OTel, etc.) from PyPI
RUN pip install /wheels/*.whl

# Clean up wheels
RUN rm -rf /wheels

# Default Env Vars
ENV HTTP_ENDPOINT_PORT=5000

# Expose port
EXPOSE 5000

# Entrypoint
CMD ["python", "-m", "otlp_mmap_example_server.app"]