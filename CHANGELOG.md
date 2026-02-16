# Changelog

## [0.0.1] - Refinement: Paged I/O

### Changed

- **I/O Strategy:** Replaced `mmap` with `pread` + Application-Level LRU Cache (Buffer Pool).
- **Architecture:** Redesigned `FmIndexShard` to work with a `PagedReader` trait rather than raw slices.
- **Dependencies:** Added `lru` for cache management.
