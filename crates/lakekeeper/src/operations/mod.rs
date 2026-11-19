//! Reading Iceberg manifests from inside Lakekeeper: lenient manifest decoding, a manifest reader
//! with retries and a memory budget, the CPU-bound parse gate, and progress tracking.

pub mod error;
pub mod manifest_decode;
pub mod manifest_reader;
pub mod parse_gate;
pub mod progress_tracker;
pub mod task;
