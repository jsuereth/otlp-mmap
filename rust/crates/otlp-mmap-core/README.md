# OTLP-MMAP Core library

This provides low-level support for reading and writing OTLP-MMAP files in Rust.

There are two entry points into this crate:

- `OtlpMmapWriter`: Which will open a file, with a set of configuration, for writing an OTLP-MMAP file, e.g. in Rust or using Rust as an FFI. This allows multiple threads.
- `OtlpMmapReader`: Which will open an existing OTLP-MMAP file and allow you to read from it. This should only be used on one thread.

These each provide access to the header, ringbuffers and dictionary needed to read/write OTLP-MMAP protocool.

OTLP MMAP files are kept safe between threads via compare-and-swap atomic operations. This means "spin-lock" contention, which is not built into the core library, but
must be provided.  

For example, a naive implementation for writing span events could be:

```rust
let my_writer = OtlpMmapWriter::new(...);
while let Ok(false) = my_writer.spans().try_write(&span) {
  // spin or yield
  std::hint::spin_loop();
}
```

This library is purposefuly NOT async and does not provide an exponential back-off spin-lock, enabling your runtime to choose what's best for its environment.