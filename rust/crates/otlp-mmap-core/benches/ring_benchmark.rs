use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use memmap2::MmapOptions;
use otlp_mmap_core::RingBufferReader;
use otlp_mmap_core::RingBufferWriter;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct MyMessage {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub timestamp: u64,
    #[prost(string, tag = "3")]
    pub payload: ::prost::alloc::string::String,
    #[prost(uint32, repeated, tag = "4")]
    pub tags: ::prost::alloc::vec::Vec<u32>,
}

impl MyMessage {
    fn for_bench() -> Self {
        Self {
            id: 101,
            timestamp: 1700000000,
            // 64-byte string forces heap allocation and cache line usage
            payload: "performance_test_payload_with_enough_length_to_avoid_sso".to_owned(),
            // Small vec to test pointer chasing during serialization
            tags: vec![1, 2, 3, 4, 5],
        }
    }
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("RingBuffer_Throughput");
    let file = tempfile::NamedTempFile::new().expect("Failed to create temp file for benchmark");
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file.path())
        .expect("Failed to open temp file for testing");
    // Give it enough room for our ring.
    let buffer_size: usize = 1024;
    let num_buffers: usize = 512;
    f.set_len(32 + (4 * num_buffers as u64) + (buffer_size as u64 * num_buffers as u64))
        .expect("Failed to create large enough file for ring");
    for msg_count in [10_000, 100_000, 1_000_000].iter() {
        for num_writer_threads in [1, 2, 4, 8].iter() {
            let parameter = (*msg_count, *num_writer_threads);
            let id = BenchmarkId::new(
                "write_and_read",
                format!("{msg_count} msgs, {num_writer_threads} thds"),
            );
            group.bench_with_input(id, &parameter, |b, &(num_msgs, num_threads)| {
                b.iter_custom(|_| {
                    let writer = unsafe {
                        let data = MmapOptions::new()
                            .map_mut(&f)
                            .expect("Failed to create mmap for benchmark");
                        Arc::new(
                            RingBufferWriter::new(data, 0, buffer_size, num_buffers)
                                .expect("Failed to construct ring buffer writer"),
                        )
                    };
                    // Now we can construct the reader.
                    let reader: RingBufferReader<MyMessage> = unsafe {
                        let data = MmapOptions::new()
                            .map_mut(&f)
                            .expect("Failed to create mmap for benchmark");
                        RingBufferReader::new(data, 0)
                            .expect("Failed to construct ring buffer reader")
                    };
                    let msg_per_thread = num_msgs / num_threads;
                    // Don't deal with remainder.
                    let expected_reads = msg_per_thread * num_threads;
                    let start_time = Instant::now();
                    // Spawn reader
                    let handle = thread::spawn(move || {
                        let mut read_count = 0;
                        while read_count < expected_reads {
                            // TODO - we should probably be testing our actual exponential backoffs here.
                            if let Ok(Some(_)) = reader.try_read() {
                                read_count += 1;
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    });
                    // Span writers
                    let mut writers = vec![];
                    for _ in 0..num_threads {
                        // Create new refernce for writer thread.
                        let my_writer = writer.clone();
                        writers.push(std::thread::spawn(move || {
                            for _ in 0..msg_per_thread {
                                let msg = MyMessage::for_bench();
                                while let Ok(false) = my_writer.try_write(&msg) {
                                    // spin or yield
                                    std::hint::spin_loop();
                                }
                            }
                        }));
                    }
                    for w in writers {
                        w.join().expect("Writer never completed!");
                    }
                    handle.join().expect("Reader never completed!");
                    start_time.elapsed()
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
