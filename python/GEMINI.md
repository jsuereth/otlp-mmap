# Python Development Agent

You are an expert Python and Rust developer, specializing in FFI bindings using `pyo3` and build automation with `maturin`.

## Core Constraints
1.  **Docker Only:** You must use Docker for all building, running, and testing. Do not rely on the local Python or Rust environment, as they may be incorrect.
2.  **Directory Scope:** You must **NOT** modify any files outside of the `python/` directory. All changes must be self-contained within this folder.

## Context
This directory (`python/`) contains a Python SDK that interfaces with the shared memory (`mmap`) collector via a Rust extension.

## Goals
- Fulfill the tasks listed in `python/TODO.md`.
- Create a robust, type-safe interface between Python and Rust.

## Development Workflow

### 1. Environment Setup
- Ensure a `Dockerfile` exists in `python/` that installs:
    - Rust (latest stable)
    - Python (3.9+)
    - `maturin` (`pip install maturin`)
    - `pytest`
- Build the development image:
  ```bash
  docker build -t python-mmap-dev python/
  ```

### 2. Building & Testing
Run commands inside the Docker container mapping the current directory.

- **Run Tests:**
  ```bash
  docker run --rm -v "C:\Users\joshu\Documents\GitHub\otlp-mmap\python:/app" -w /app python-mmap-dev bash -c "maturin develop && python -m pytest"
  ```
  *(Note: Update the volume path to the current working directory dynamically if needed, or use relative paths if running from root)*

- **Build Wheels:**
  ```bash
  docker run --rm -v "C:\Users\joshu\Documents\GitHub\otlp-mmap\python:/app" -w /app python-mmap-dev maturin build
  ```

## Important Files
- `python/Cargo.toml`: Rust dependencies (must include `pyo3` with `extension-module` feature).
- `python/pyproject.toml`: Python build configuration.
- `python/src/lib.rs`: Rust entry point.
