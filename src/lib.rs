pub mod cache;
pub mod index;
pub mod iolib;
pub mod api;
pub mod ingest;
pub mod shard_router;

pub use api::{IndexBuilder, IndexReader, IndexStats};
pub use index::encoding::EncodingMode;
pub use index::wavelet::{WaveletBuildMode, DEFAULT_WAVELET_MAX_BYTES};
pub use shard_router::{MultiShardReader, ShardHit};

#[cfg(test)]
mod tests;
