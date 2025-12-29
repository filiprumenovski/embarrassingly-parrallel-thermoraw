//! Benchmarks comparing sequential vs parallel scan extraction performance.
//!
//! Run with: cargo bench --features rayon,tokio
//!
//! Requires a RAW file at ../tests/data/small.RAW or set RAW_FILE_PATH env var.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use thermorawfilereader::RawFileReader;

fn get_raw_file_path() -> String {
    std::env::var("RAW_FILE_PATH").unwrap_or_else(|_| "../tests/data/small.RAW".to_string())
}

fn bench_sequential_iteration(c: &mut Criterion) {
    let path = get_raw_file_path();
    let reader = match RawFileReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Skipping benchmark: could not open {}: {}", path, e);
            return;
        }
    };

    let num_scans = reader.len();

    let mut group = c.benchmark_group("scan_extraction");
    group.throughput(Throughput::Elements(num_scans as u64));

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let sum: usize = reader
                .iter()
                .map(|s| black_box(s.data().map(|d| d.len()).unwrap_or(0)))
                .sum();
            black_box(sum)
        })
    });

    group.finish();
}

#[cfg(feature = "rayon")]
fn bench_parallel_iteration(c: &mut Criterion) {
    use rayon::prelude::*;

    let path = get_raw_file_path();
    let reader = match RawFileReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Skipping benchmark: could not open {}: {}", path, e);
            return;
        }
    };

    let num_scans = reader.len();

    let mut group = c.benchmark_group("scan_extraction");
    group.throughput(Throughput::Elements(num_scans as u64));

    group.bench_function("parallel_rayon", |b| {
        b.iter(|| {
            let sum: usize = reader
                .par_scans()
                .map(|s| black_box(s.data().map(|d| d.len()).unwrap_or(0)))
                .sum();
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_batched_iteration(c: &mut Criterion) {
    let path = get_raw_file_path();
    let reader = match RawFileReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Skipping benchmark: could not open {}: {}", path, e);
            return;
        }
    };

    let num_scans = reader.len();

    let mut group = c.benchmark_group("scan_extraction");
    group.throughput(Throughput::Elements(num_scans as u64));

    for batch_size in [64, 256, 1024, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::new("batched", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let sum: usize = reader
                        .batched_scans(batch_size)
                        .flat_map(|batch| batch.into_iter())
                        .map(|s| black_box(s.data().map(|d| d.len()).unwrap_or(0)))
                        .sum();
                    black_box(sum)
                })
            },
        );
    }

    group.finish();
}

#[cfg(feature = "rayon")]
fn bench_parallel_batched(c: &mut Criterion) {
    use rayon::prelude::*;

    let path = get_raw_file_path();
    let reader = match RawFileReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Skipping benchmark: could not open {}: {}", path, e);
            return;
        }
    };

    let num_scans = reader.len();

    let mut group = c.benchmark_group("parallel_batched");
    group.throughput(Throughput::Elements(num_scans as u64));

    for batch_size in [64, 256, 1024, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::new("par_batched", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let sum: usize = reader
                        .batched_scans(batch_size)
                        .collect::<Vec<_>>()
                        .into_par_iter()
                        .flat_map(|batch| batch.into_par_iter())
                        .map(|s| black_box(s.data().map(|d| d.len()).unwrap_or(0)))
                        .sum();
                    black_box(sum)
                })
            },
        );
    }

    group.finish();
}

#[cfg(feature = "tokio")]
fn bench_async_stream(c: &mut Criterion) {
    use futures::StreamExt;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();

    let path = get_raw_file_path();
    let reader = match RawFileReader::open(&path) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("Skipping benchmark: could not open {}: {}", path, e);
            return;
        }
    };

    let num_scans = reader.len();

    let mut group = c.benchmark_group("async_stream");
    group.throughput(Throughput::Elements(num_scans as u64));

    for batch_size in [256, 1024, 4096].iter() {
        let reader_clone = Arc::clone(&reader);
        group.bench_with_input(
            BenchmarkId::new("stream_scans", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let r = Arc::clone(&reader_clone);
                    rt.block_on(async move {
                        let mut stream = r.stream_scans(batch_size);
                        let mut sum = 0usize;
                        while let Some((_idx, batch)) = stream.next().await {
                            for s in batch {
                                sum += black_box(s.data().map(|d| d.len()).unwrap_or(0));
                            }
                        }
                        black_box(sum)
                    })
                })
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "rayon"))]
fn bench_parallel_iteration(_c: &mut Criterion) {}

#[cfg(not(feature = "rayon"))]
fn bench_parallel_batched(_c: &mut Criterion) {}

#[cfg(not(feature = "tokio"))]
fn bench_async_stream(_c: &mut Criterion) {}

criterion_group!(
    benches,
    bench_sequential_iteration,
    bench_parallel_iteration,
    bench_batched_iteration,
    bench_parallel_batched,
    bench_async_stream,
);
criterion_main!(benches);
