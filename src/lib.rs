pub mod api;
pub mod cache;
pub mod index;
pub mod ingest;
pub mod iolib;
pub mod shard_router;

pub use api::{IndexBuilder, IndexReader, IndexStats};
pub use index::encoding::EncodingMode;
pub use index::wavelet::{DEFAULT_WAVELET_MAX_BYTES, WaveletBuildMode};
pub use iolib::paged_reader::{
    DEFAULT_PAGE_SIZE, PagedReaderConfig, PrefetchMode, RandomAccessRead, SharedRandomAccessRead,
};
pub use shard_router::{DocHit, MultiShardReader, ShardHit};

#[cfg(test)]
mod tests;
