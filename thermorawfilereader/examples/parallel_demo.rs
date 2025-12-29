//! Demonstration of parallel scan extraction.
//!
//! Run with: cargo run --example parallel_demo --features rayon,tokio -- <path_to_raw_file>
//!
//! Requires .NET 8.0 runtime to be installed.

use std::env;
use std::io;
use std::time::Instant;

use thermorawfilereader::RawFileReader;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = args.get(1).map(|s| s.as_str()).unwrap_or("../tests/data/small.RAW");

    println!("Opening RAW file: {}", path);
    let reader = RawFileReader::open(path)?;
    let num_scans = reader.len();
    println!("Found {} scans\n", num_scans);

    // Sequential iteration
    println!("=== Sequential Iteration ===");
    let start = Instant::now();
    let seq_sum: usize = reader
        .iter()
        .map(|s| s.data().map(|d| d.len()).unwrap_or(0))
        .sum();
    let seq_time = start.elapsed();
    println!("Total data points: {}", seq_sum);
    println!("Time: {:?}\n", seq_time);

    // Batched iteration
    println!("=== Batched Iteration (batch_size=16) ===");
    let start = Instant::now();
    let batched_sum: usize = reader
        .batched_scans(16)
        .flat_map(|batch| batch.into_iter())
        .map(|s| s.data().map(|d| d.len()).unwrap_or(0))
        .sum();
    let batched_time = start.elapsed();
    println!("Total data points: {}", batched_sum);
    println!("Time: {:?}\n", batched_time);

    // Parallel iteration (rayon)
    #[cfg(feature = "rayon")]
    {
        use rayon::prelude::*;

        println!("=== Parallel Iteration (rayon) ===");
        let start = Instant::now();
        let par_sum: usize = reader
            .par_scans()
            .map(|s| s.data().map(|d| d.len()).unwrap_or(0))
            .sum();
        let par_time = start.elapsed();
        println!("Total data points: {}", par_sum);
        println!("Time: {:?}", par_time);
        println!("Speedup: {:.2}x\n", seq_time.as_secs_f64() / par_time.as_secs_f64());
    }

    // Async stream iteration (tokio)
    #[cfg(feature = "tokio")]
    {
        use futures::StreamExt;

        println!("=== Async Stream Iteration (tokio, batch_size=16) ===");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let start = Instant::now();
        let async_sum: usize = rt.block_on(async {
            let mut stream = reader.stream_scans(16);
            let mut sum = 0usize;
            while let Some((_idx, batch)) = stream.next().await {
                for s in batch {
                    sum += s.data().map(|d| d.len()).unwrap_or(0);
                }
            }
            sum
        });
        let async_time = start.elapsed();
        println!("Total data points: {}", async_sum);
        println!("Time: {:?}\n", async_time);
    }

    // Verify all methods produce the same result
    println!("=== Verification ===");
    assert_eq!(seq_sum, batched_sum, "Batched sum mismatch!");
    println!("Sequential == Batched: OK");

    #[cfg(feature = "rayon")]
    {
        use rayon::prelude::*;
        let par_sum: usize = reader
            .par_scans()
            .map(|s| s.data().map(|d| d.len()).unwrap_or(0))
            .sum();
        assert_eq!(seq_sum, par_sum, "Parallel sum mismatch!");
        println!("Sequential == Parallel: OK");
    }

    #[cfg(feature = "tokio")]
    {
        use futures::StreamExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let async_sum: usize = rt.block_on(async {
            let mut stream = reader.stream_scans(16);
            let mut sum = 0usize;
            while let Some((_idx, batch)) = stream.next().await {
                for s in batch {
                    sum += s.data().map(|d| d.len()).unwrap_or(0);
                }
            }
            sum
        });
        assert_eq!(seq_sum, async_sum, "Async sum mismatch!");
        println!("Sequential == Async: OK");
    }

    println!("\nAll tests passed!");

    Ok(())
}
