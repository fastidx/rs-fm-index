# TODO List - Infini-gram Rust

## Phase 1: Core I/O & Caching

- [x] **Project Setup:** Initialize Cargo with required dependencies (cdivsufsort, serde, bincode, rayon, anyhow, tempfile, byteorder).
- [x] **Implement S3-FIFO Cache:**
  - [x] Port the provided `FastS3Fifo` and `ShardedFastS3Fifo` into `src/cache/s3fifo.rs`.
  - [x] Ensure `Entry` struct is optimized for memory layout (using indices `u32` instead of pointers).
  - [x] **Verification:** Run the provided `concurrent_tests` suite to ensure thread safety and eviction logic.
- [x] **Implement `PagedReader`:**
  - [x] Implement `read_at` logic using `pread` (for concurrency).
  - [x] Integrate `ShardedFastS3Fifo` as the backing store.
  - [x] Handle page boundary alignment (reads spanning multiple pages).

## Phase 2: On-Disk Data Structures

- [x] **Implement `PagedSampledSA`**
- [x] **Implement `PagedWaveletTree` (Huffman)**
  - [x] **Builder (Memory):**
    - [x] Port Huffman logic (`huffman_lengths`, `canonical_codes`).
    - [x] Build the Tree Topology (`WaveletNodeShape`).
    - [x] Pass over text to fill Node BitVectors.
    - [x] Flatten into a global bitstream and write pages to disk.
  - [x] **Reader (Disk):**
    - [x] Load `codes` and `nodes` topology from header.
    - [x] Implement `rank(symbol, i)` using the Huffman path.

## Phase 3: The Builder (Ingestion)

- [x] **Shard Builder:**
  - [x] Compute SA/BWT in memory (using `cdivsufsort`).
  - [x] Build the Wavelet Tree in memory.
  - [x] **Page Writer:** Serialize the in-memory tree into the Paged Format on disk.
  - [x] **Sampled ISA:** Build and store sampled inverse suffix array.
  - [x] **Doc Offsets:** Encode and store doc offsets (delta + Elias gamma).

## Phase 4: Search & Optimization

- [x] **Query Logic:** `count()`, `locate()`, `extract()`.
- [x] **Multi-Document Support:** doc offsets, `pos_to_doc_id()`, `get_document()`.
- [x] **CLI + Library API:** high-level builder/reader usable as a library.
- [ ] **Benchmark:** Compare `PagedReader` vs `mmap` latency on large files.

## Phase 5: 200TB-Scale Readiness (Next)

- [x] **64-bit SA/ISA:** Move from u32 to u64 (or u40/u48 packing) for >4GB shards.
- [ ] **External-memory SA/BWT:** Replace in-memory `cdivsufsort` with an external-memory algorithm.
- [ ] **Streaming Build:** Avoid materializing full BWT/bitvectors; stream into pages.
- [ ] **Compressed SA/ISA:** Delta + varint/Rice/PFor for sampled arrays.
- [ ] **Run-Length BWT / R-Index:** Replace Huffman WT with RLBWT or wavelet matrix of runs.
- [ ] **Shard & Merge:** Partition documents into shards, add a top-level routing layer.
- [x] **Doc Offsets Indexing:** Elias-Fano or sampled index for fast doc_id lookup at scale.
- [ ] **I/O Pipeline:** Async prefetch, large sequential reads, configurable page size.
