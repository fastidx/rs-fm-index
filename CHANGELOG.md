# Changelog

## [Unreleased]

### Added

- Multi-shard query router with merged locate/count across shard directories.
- CLI support for querying shard directories and extracting documents from shards.
- Shared cache support for multi-shard readers.
- Streaming wavelet-tree build from a BWT reader (no in-memory BWT).

### Changed

- `ShardHeader::new` now uses a params struct to reduce argument count.
- Minor internal refactors for Clippy hygiene.
- Builder now streams BWT to disk and samples SA/ISA on the fly (no full BWT/SA/ISA in memory).

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
