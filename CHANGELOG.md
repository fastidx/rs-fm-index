# Changelog

## [Unreleased]

Nothing yet.

## [0.0.1] - 2026-02-17

### Added

- Global compacted Wavelet Tree bitstream (single paged BV).
- Sampled ISA and extract logic.
- Multi-document support with Elias-Fano encoded doc offsets.
- High-level library API (`IndexBuilder`, `IndexReader`).
- CLI support for multi-doc builds and document extraction.
- 64-bit SA/ISA storage to lift the 4GB shard ceiling.
- Distributed ingestion (sharded) with per-shard stats/meta and ingest report.

### Changed

- **I/O Strategy:** Replaced `mmap` with `pread` + paged reader + S3-FIFO cache.
- **Architecture:** Redesigned `FmIndexShard` to work with a `PagedReader` rather than raw slices.
- Header encoding uses `bincode::serde` (legacy config).
- Query engine now supports doc_id mapping and full document reconstruction.
