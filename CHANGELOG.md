# Changelog

## [Unreleased]

### Added

- Global compacted Wavelet Tree bitstream (single paged BV).
- Sampled ISA and extract logic.
- Multi-document support with encoded doc offsets (delta + Elias gamma).
- High-level library API (`IndexBuilder`, `IndexReader`).
- CLI support for multi-doc builds and document extraction.
- 64-bit SA/ISA storage to lift the 4GB shard ceiling.

### Changed

- Header encoding uses `bincode::serde` (legacy config).
- Query engine now supports doc_id mapping and full document reconstruction.

## [0.0.1] - Refinement: Paged I/O

### Changed

- **I/O Strategy:** Replaced `mmap` with `pread` + Application-Level LRU Cache (Buffer Pool).
- **Architecture:** Redesigned `FmIndexShard` to work with a `PagedReader` trait rather than raw slices.
- **Dependencies:** Added `lru` for cache management.
