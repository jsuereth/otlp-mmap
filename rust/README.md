# Rust implementations for OTLP-MMAP

We provide several Rust implementations for OTLP-MMAP divided into the following crates:

- `otlp-mmap-protocol`: Just the prost-generated files for the protocol buffers used in OTLP-MMAP.
- `otlp-mmap-core`: Low-level API for interacting with OTLP-MMAP files. This provides a foundation of using MMAP'd memory with atomic operations for concurrency-safety.
- `otlp-mmap-collector`: A library and binary for reading OTLP-MMAP files and reconstructing OTLP from them.
- `otlp-mmap-pybindings`: A set of python extension which can be used to implement an SDK that purely writes to OTLP-MMAP files.

## Building

The following docker files are provided for building / testing this project.

- `mmap-collector.Dockerfile`: Builds a container that will run the `otlp-mmap-collector`.
- `python-lib.Dockerfile`: Builds a container that has a python environment with the `otlp-mmap-pybindings` rust crate automatically installed.