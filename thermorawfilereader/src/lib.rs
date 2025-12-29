//! Read Thermo RAW files using a self-hosted .NET runtime that uses Thermo Fisher's `RawFileReader` library.
//!
//! The main access point is [`RawFileReader`], via [`RawFileReader::open`].
//!
//! # Disclaimer
//!
//! This is a fork of the original [`thermorawfilereader`](https://crates.io/crates/thermorawfilereader)
//! crate by Joshua Klein (mobiusklein). This fork adds experimental high-performance parallel
//! iteration capabilities. The parallel features are provided "as-is" without warranty.
//! Performance claims are theoretical targets based on ThermoRawFileParser multiprocessing
//! benchmarks and may vary depending on hardware, .NET runtime version, and file characteristics.
//!
//! # Acknowledgements
//!
//! - **Original Author**: Joshua Klein ([mobiusklein](https://github.com/mobiusklein)) -
//!   Creator of the original thermorawfilereader.rs library
//! - **Upstream Repository**: <https://github.com/mobiusklein/thermorawfilereader.rs>
//! - **Thermo Fisher Scientific**: For providing the RawFileReader .NET library
//! - **ThermoRawFileParser**: Performance benchmarks inspired by
//!   <https://github.com/compomics/ThermoRawFileParser>
//!
//! # High-Performance Parallel Iteration
//!
//! This fork provides optional high-performance iterators for parallel scan extraction:
//!
//! - **`par_scans()`** (feature: `rayon`) - Rayon-based parallel iterator, targeting ~15x speedup on 16-core CPUs
//! - **`stream_scans()`** (feature: `tokio`/`async`) - Tokio async stream with prefetching
//! - **`batched_scans()`** (always available) - Batched iterator for Arrow RecordBatch construction
//!
//! ## Performance Notes
//!
//! Actual speedup depends on:
//! - Number of CPU cores and their performance characteristics
//! - .NET runtime version and configuration
//! - RAW file size and spectrum complexity
//! - Storage I/O performance (SSD vs HDD)
//! - Memory bandwidth and cache characteristics
//!
//! The .NET FFI calls are serialized internally by the runtime, so parallelism benefits
//! primarily come from concurrent FlatBuffer deserialization and data processing, not
//! from parallel .NET calls themselves.
//!
//! ## Example
//!
//! ```no_run
//! use thermorawfilereader::RawFileReader;
//!
//! # #[cfg(feature = "rayon")]
//! fn parallel_example() -> std::io::Result<()> {
//!     use rayon::prelude::*;
//!
//!     let reader = RawFileReader::open("sample.RAW")?;
//!     let total: usize = reader.par_scans()
//!         .map(|s| s.data().map(|d| d.len()).unwrap_or(0))
//!         .sum();
//!     println!("Total data points: {}", total);
//!     Ok(())
//! }
//! ```
//!
//! # Limitations
//!
//! ## Platforms
//! `RawFileReader` requires a .NET runtime. The linking between Rust and the host's .NET runtime is managed by [`netcorehost`].
//! While it supports most major operating, you can check which versions which version of .NET supports which OS version at
//! <https://github.com/dotnet/core/blob/main/os-lifecycle-policy.md>.
//!
//! If you wish to link with a local `nethost` library instead of downloading the latest version at build time, please see
//! [`netcorehost`]'s documentation. This is still distinct from actually statically linking with .NET's `coreclr` library
//! which must be installed separately.
//!
//! ## Why no [`Read`](std::io::Read) support?
//! The underlying .NET library from Thermo's public API expects a plain file paths as strings and likes to fiddle with
//! file system locks. There is no way for it to consume .NET streams, let alone Rust analogs like [`Read`](std::io::Read),
//! so for the moment we can only open RAW files on the file system.
//!
//! # Licensing
//!
//! This crate is licensed under Apache-2.0, the same license as the original thermorawfilereader.
//! By using this library, you also agree to the [RawFileReader License](https://github.com/thermofisherlsms/RawFileReader/blob/main/License.doc)
mod constants;
pub(crate) mod r#gen;
pub(crate) mod wrap;
pub mod parallel;

#[doc = "The FlatBuffers schema used to exchange data, see [`schema.fbs`](https://github.com/mobiusklein/thermorawfilereader.rs/blob/main/schema/schema.fbs)"]
pub use crate::r#gen::schema_generated::librawfilereader as schema;
pub use crate::wrap::{
    ChromatogramData, ChromatogramDescription, FileDescription, InstrumentConfiguration,
    InstrumentMethod, InstrumentModel, RawFileReader, RawFileReaderError, RawFileReaderIntoIter,
    RawFileReaderIter, RawSpectrum, SpectrumData, Acquisition, ExtendedSpectrumData,
    StatusLogCollection, StatusLog, TrailerValue, TrailerValues,
};
pub use constants::{IonizationMode, MassAnalyzer, TraceType, MSOrder};

// Re-export parallel iteration types
pub use parallel::BatchedScansIter;

#[cfg(feature = "rayon")]
pub use parallel::ParScansIter;

#[cfg(feature = "tokio")]
pub use parallel::AsyncScanStream;

#[doc(alias = "Re-exported from `dotnetrawfilereader_sys`")]
pub use dotnetrawfilereader_sys::{DotNetRuntimeCreationError, set_runtime_dir, try_get_runtime};