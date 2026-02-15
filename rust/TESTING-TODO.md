# Rust Testing Coverage Gaps

This document tracks identified gaps in test coverage for the Rust implementation of OTLP-MMAP.

## 1. otlp-mmap-core

### ringbuffer.rs
- [ ] **Multi-producer Concurrency**: Verify that multiple writers can successfully claim indices and write to the ring buffer without corruption.
    - **Goal**: Ensure `writer_index` CAS and availability array updates are thread-safe.
- [ ] **Message Size Overflow**: Test behavior when a message is encoded that exceeds `buffer_size`.
    - **Goal**: Verify that `ProtobufEncodeError` is returned and the buffer remains in a consistent state.

### dictionary.rs
- [ ] **Concurrent Remapping**: Test safety when one thread is reading from the dictionary while another thread triggers a file growth and remapping.
    - **Goal**: Verify if `UnsafeCell` and `MmapMut` replacement is safe under load (or identify if locking is needed).
- [ ] **Large Entry Support**: Write an entry larger than `MIN_DICTIONARY_SIZE`.
    - **Goal**: Verify `ensure_capacity` correctly doubles or expands to fit.

### header.rs
- [ ] **Version Validation**: Call `check_version` with headers containing unsupported versions (e.g., 0, 2).
    - **Goal**: Verify `Error::VersionMismatch` is returned.
- [ ] **Initialization Logic**: Verify offset calculations in `initialize` for various `OtlpMmapConfig` settings.
    - **Goal**: Ensure no overlaps between sections.

### convert.rs
- [ ] **Nested AnyValue Conversion**: Test `try_convert_anyvalue` with deeply nested `ArrayValue` and `KvlistValue`.
    - **Goal**: Ensure recursion works correctly and OTLP format is maintained.
- [ ] **ValueRef Resolution**: Test `ValueRef` variant in `AnyValue` resolving to a dictionary entry.
    - **Goal**: Verify that indirection via dictionary works for any value type.

## 2. otlp-mmap-collector

### trace.rs
- [ ] **Span Name Updates**: Send a `ChangeSpanName` event for an active span and verify the final OTLP span has the updated name.
    - **Goal**: Verify state updates in `ActiveSpans`.
- [ ] **Attribute Appending**: Send multiple `UpdateAttributes` events for an active span.
    - **Goal**: Verify attributes are accumulated.
- [x] **Span Link Accumulation**: Send multiple `AddLink` events for an active span.
    - **Goal**: Verify all links are present in the final OTLP span.
- [ ] **Dangling Span GC**: Simulate spans that never receive an `EndSpan` event.
    - **Goal**: Verify mechanism for cleaning up stale spans from `ActiveSpans` (once implemented).

### log.rs
- [ ] **Batching Timeout**: Test that `try_create_next_batch` returns a partial batch if the timeout expires before `max_batch_size` is reached.
    - **Goal**: Verify OTLP export responsiveness.
- [ ] **Malformed Event Body**: Test conversion when `body` dictionary lookup or conversion fails.
    - **Goal**: Ensure the collector doesn't crash and ideally logs/handles the error.

### metric/aggregation/exp_hist.rs
- [ ] **Scale Underflow**: Record measurements that would require a scale lower than `EXPO_MIN_SCALE`.
    - **Goal**: Verify measurements are dropped or handled according to spec without panicking.
- [ ] **Otlp Collection**: Verify `collect` produces a valid `ExponentialHistogramDataPoint` with correct buckets and scale.

### metric/aggregation/sum.rs
- [ ] **Temporality Behavior**: Test that Delta temporality sums reset or behave correctly across multiple `collect` calls.
    - **Goal**: Verify compliance with OTel temporality specs.
- [ ] **Monotonicity**: Test recording negative values when `is_monotonic` is true.
    - **Goal**: Verify violations are handled (e.g., dropped).

## 3. otlp-mmap-pybindings

### sdk.rs
- [ ] **Python to MMap Type Conversion**: Thoroughly test conversion of various Python types (None, bool, int, float, str, bytes, list, dict) into `HashableAnyValue` and OTLP attributes.
    - **Goal**: Ensure `PyO3` integration is robust.
- [ ] **Deduplication Caching**: Verify that interning the same Resource/Scope/Metric twice from Python returns the same reference and doesn't write duplicate entries to the dictionary.
    - **Goal**: Verify `scc::HashIndex` usage and cache key stability.
