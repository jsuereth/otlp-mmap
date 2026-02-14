# --- Test/Development Stage ---
FROM python:3.11-alpine@sha256:303398d5c9f110790bce60d64f902e51e1a061e33292985c72bf6cd07960bf09

# Install Rust toolchain and required system dependencies for compilation
RUN apk add --no-cache rust cargo build-base python3-dev libffi-dev openssl-dev

# Install build tools and dependencies
RUN pip install --no-cache-dir maturin patchelf pytest pytest-benchmark requests flask opentelemetry-api opentelemetry-sdk hatchling

# Create non-root user for security
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser

WORKDIR /app
RUN chown appuser:appuser /app

# Switch to non-root user for building and running
USER appuser

# 1. Build Rust Bindings FIRST
COPY --chown=appuser:appuser rust/Cargo.toml rust/Cargo.lock /app/rust/
COPY --chown=appuser:appuser rust/crates /app/rust/crates/
WORKDIR /app/rust/crates/otlp-mmap-pybindings
RUN maturin build --release --out /tmp/wheels
RUN pip install --user /tmp/wheels/otlp_mmap_pybindings*.whl

# 2. Setup Python environment
WORKDIR /app/python
COPY --chown=appuser:appuser python/otlp-mmap-sdk /app/python/otlp-mmap-sdk/
COPY --chown=appuser:appuser python/otlp-mmap-example-server /app/python/otlp-mmap-example-server/
COPY --chown=appuser:appuser python/tests /app/python/tests/
COPY --chown=appuser:appuser python/pyproject.toml python/README.md /app/python/

# Install SDK and Example Server
RUN pip install --user -e ./otlp-mmap-sdk -e ./otlp-mmap-example-server

# Default to running tests
CMD ["python", "-m", "pytest"]
