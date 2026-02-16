# TODO List - Infini-gram Rust

## Phase 1: Core I/O & Caching [Current Focus]

- [ ] **Project Setup:** Initialize Cargo with `cdivsufsort`, `twox-hash`, `parking_lot` (optional, for faster Mutex), `libc`.
- [x] **Implement S3-FIFO Cache:**
  - [x] Port the provided `FastS3Fifo` and `ShardedFastS3Fifo` into `src/cache/s3fifo.rs`.
  - [x] Ensure `Entry` struct is optimized for memory layout (using indices `u32` instead of pointers).
  - [x] **Verification:** Run the provided `concurrent_tests` suite to ensure thread safety and eviction logic.
- [x] **Implement `PagedReader`:**
  - [x] Implement `read_at` logic using `pread` (for concurrency).
  - [ ] Integrate `ShardedFastS3Fifo` as the backing store.
  - [ ] Handle page boundary alignment (e.g., reading a u32 that crosses two 4KB pages).

## Phase 2: On-Disk Data Structures

- [x] **Implement `PagedSampledSA`:**
  - [ ] Abstraction to read `SA[i]` via the `PagedReader`.

## Phase 2: On-Disk Data Structures

- [x] **Implement `PagedSampledSA`**
- [ ] **Implement `PagedWaveletTree` (Huffman)**
  - [ ] **Builder (Memory):**
    - [ ] Port Huffman logic (`huffman_lengths`, `canonical_codes`).
    - [ ] Build the Tree Topology (`WaveletNodeShape`).
    - [ ] Pass over text to fill Node BitVectors.
    - [ ] Flatten and write pages to disk.
  - [ ] **Reader (Disk):**
    - [ ] Load `codes` and `nodes` topology from header.
    - [ ] Implement `rank(symbol, i)` using the Huffman path.

## Phase 3: The Builder (Ingestion)

- [ ] **Shard Builder:**
  - [ ] Compute SA/BWT in memory (using `cdivsufsort`).
  - [ ] Build the Wavelet Tree in memory.
  - [ ] **Page Writer:** Serialize the in-memory tree into the Paged Format on disk.

## Phase 4: Search & Optimization

- [ ] **Query Logic:** `count()`, `locate()`, `extract()`.
- [ ] **Benchmark:** Compare `PagedReader` vs `mmap` latency on large files.
