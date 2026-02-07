FROM python:3.9-slim

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

# Set working directory
WORKDIR /app

# Copy the entire python workspace
COPY . .

# Build and install otlp-mmap-internal
RUN maturin build -r -m Cargo.toml --out target/wheels
RUN pip install target/wheels/*.whl

# Install otlp-mmap-sdk
# Use --no-build-isolation to access the installed otlp-mmap-internal
RUN pip install ./otlp-mmap-sdk --no-build-isolation

# Install otlp-mmap-example-server
RUN pip install ./otlp-mmap-example-server --no-build-isolation

# Default Env Vars
ENV HTTP_ENDPOINT_PORT=5000

# Expose port
EXPOSE 5000

# Entrypoint
CMD ["python", "-m", "otlp_mmap_example_server.app"]
