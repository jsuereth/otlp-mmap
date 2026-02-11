# --- Build Stage ---
FROM python:3.11-alpine AS builder

# Install Rust toolchain and required system dependencies for compilation
RUN apk add --no-cache rust cargo build-base python3-dev libffi-dev openssl-dev

# Install build tools
RUN pip install maturin hatchling editables patchelf

# Set the working directory
WORKDIR /build

# list out directories to avoid pulling local cargo `target/`
COPY Cargo.toml /build/Cargo.toml
COPY Cargo.lock /build/Cargo.lock
COPY crates /build/crates

# Build with maturin - 
WORKDIR /build/crates/otlp-mmap-pybindings
RUN maturin build --release --out /wheels

# Create layer of python alpine with our module installed.
FROM python:3.11-alpine
WORKDIR /app
# Copy wheels from builder
COPY --from=builder /wheels /wheels
# Install all wheels
# This will install internal, sdk, server and pull in runtime dependencies (Flask, OTel, etc.) from PyPI
RUN pip install /wheels/*.whl
# Clean up wheels
RUN rm -rf /wheels

# TODO - do we need anything else for this image?