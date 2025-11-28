# Benchmarking

This directory contains benchmarking plans, findings and next steps.

## Overall

Right now, the MMAP collector is not what a fully featured collector has, so we can expect some more overhead as that gets
added, however we've found the following to be true:

- For Java
  - Memory usage for the mmap collector is still less than the Java SDK.  That is, *combined*
    the mmap-collector + mmap-sdk in Java use less RAM than just Java SDK itself.
  - Performance on the critical path is on-par, with a slight slowdown: about
    ~10-20 us, which we'd expected due to atomic operations.
  - Overall CPU usage for mmap-collector + mmap-sdk is MUCH higher than Java SDK, with the
    mmap-demo java process consuming ~300ms of a CPU vs. Java SDK using ~50ms in that same time period. This is likely due to spin-wait loops on the RingBuffers, but needs to be profiled.
- General Observations
  - The mmap-collector is able to flood the OTEL collector and force backpressure. This is NOT
    handled well on the mmap-collector side, and may lead to crashing or slowdown of the mmap
    benchmark.


## 2025-11-28 - Benchamrking Results

This was tested using the `mmap-sdk-vs-pure-sdk.docker-compose.yml` scenario.

![Benchmarking Test Results](2025-11-28-single-thread-mmap-collector.png)

- Memory usage for the mmap collector is still less than the Java SDK.  That is, *combined*
the mmap-collector + mmap-sdk in Java use less RAM than just Java SDK itself.
- Performance on the critical path is on-par, with a slight slowdown: about
~10-20 us, which we'd expected due to atomic operations.
- Overall CPU usage for mmap-collector + mmap-sdk is MUCH higher than Java SDK, with the
mmap-demo java process consuming ~300ms of a CPU vs. Java SDK using ~50ms in that same time period. This is likely due to spin-wait loops on the RingBuffers, but needs to be profiled.
- We tweaked the tokio configuration of mmap-collector and found that using `tokio::spawn`
  for interacting with the ringbuffers actually lead to dramatically slowdown due to forcing
  too much contention on the ringbufer spin-locks.

### TODOs

- [ ] Need to investigate/profile higher CPU usage on the Java MMAP SDK implementation. See if
      we can lower it.
- [ ] Run a new benchmark where we can alter the spin-lock wait times and retry cycle
      to try to force a better optimium.
- [ ] mmap-collector needs to support backpressure and crash-restarting.
- [ ] JVM metrics are not making it from MMap SDK -> LGTM even though HTTP server metrics are. This needs to be fixed.
