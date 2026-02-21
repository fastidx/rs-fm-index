# Changelog

## [Unreleased] - 2026-02-21

### Added

- Multi-shard query merge/aggregation: doc-level hit grouping with cross-boundary stitching.
- CLI support for querying shard directories and extracting documents from shards.
- Shared cache support for multi-shard readers.
- Configurable PagedReader page size and read-ahead prefetch.
- Added Criterion benchmark for paged reader sequential read patterns.
- Streaming wavelet-tree build from a BWT reader (no in-memory BWT).
- Binary-safe encoding mode (b+1 remap) with header flag and CLI `--binary`.
- Wavelet build strategy selection (`in-memory`, `streaming`, `auto`) with 256MiB default threshold and CLI flags.
- Document-boundary-safe query flag (`--doc-safe`) with safe count/locate helpers.

### Changed

- `ShardHeader::new` now uses a params struct to reduce argument count.
- Minor internal refactors for Clippy hygiene.
- Builder now streams BWT to disk and samples SA/ISA on the fly (no full BWT/SA/ISA in memory).
- Enforced a single trailing `0` sentinel with no internal `0` bytes; multi-doc boundaries use doc offsets only.
- Index format updated for binary mode (wavelet leaf symbols widened); older indexes must be rebuilt.
- Wavelet build now supports an auto/hybrid mode that selects streaming when the plan exceeds the memory threshold.

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
