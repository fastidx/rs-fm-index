# Wavelet Tree Encoding (FM-Index)

This project builds and queries an FM-index backed by a Huffman-shaped Wavelet Tree, stored on disk with a paged bitvector and a sampled SA/ISA. It supports both **CLI usage** and **library integration**.

Last updated: 2026-02-21

Key features:

- Disk-backed Wavelet Tree with **global bitstream compaction**
- Sampled Suffix Array (SA) and Inverse Suffix Array (ISA)
- Multi-document indexing with doc boundaries
- Cache-friendly paged I/O
- Library APIs for embedding in other applications

---

## Quick Start (CLI)

### Build a single-document index

```
cargo run --release -- build <input_file> <output_idx>
```

**Example**

```
cargo run --release -- build ./fmindex.txt fm-index-build.txt
```

### Query an index

```
cargo run --release -- query <index_file> <pattern>
```

### Extract a snippet

```
cargo run --release -- extract <index_file> <pos> <len>
```

### Show index size breakdown

```
cargo run --release -- stats <index_file>
```

### Extract an entire document by doc_id

```
cargo run --release -- doc <index_file> <doc_id>
```

### Build a multi-document index

```
cargo run --release -- build-multi <output_idx> <input1> [input2 ...]
```

### Distributed ingestion (sharded)

```
cargo run --release -- ingest --input "data/**/*.txt" --output ./shards --chunk-size 1GiB --workers 8
```

### Distributed ingestion with config file

Create `ingest.toml`:

```toml
input_patterns = ["data/**/*.txt"]
output_dir = "shards"
chunk_size = "1GiB"
read_buffer = "8MiB"
num_workers = 8
sample_rate = 32
scratch_dir = "/mnt/nvme/fm_scratch"
```

Run:

```
cargo run --release -- ingest --config ingest.toml
```

After ingestion, each shard will have:

- `shard_00000.idx` index
- `shard_00000.meta.json` segment/continuation metadata
- `shard_00000.stats.json` size breakdown

And a summary report:

- `ingest_report.json`

Query across shards (pass the shard directory; `query` auto-detects dirs):

```
cargo run --release -- query ./shards "search"
```

Shard queries merge segment hits back into document offsets and account for matches that cross shard boundaries within a document.

Doc-safe query (prevents cross-doc matches):

```
cargo run --release -- query --doc-safe ./index.idx "search"
cargo run --release -- query --doc-safe ./shards "search"
```

Extract a document from shards:

```
cargo run --release -- doc ./shards 2 > doc3.txt
```

Wavelet build mode:

```
# auto (default) with 256MiB threshold
cargo run --release -- build --wavelet-mode auto --wavelet-max-bytes 256MiB ./input.txt ./index.idx

# force in-memory build
cargo run --release -- build --wavelet-mode in-memory ./input.txt ./index.idx

# force streaming build
cargo run --release -- build --wavelet-mode streaming ./input.txt ./index.idx
```

Scratch directory for temporary build files:

```
cargo run --release -- build --scratch-dir /mnt/nvme/fm_scratch ./input.txt ./index.idx
cargo run --release -- ingest --scratch-dir /mnt/nvme/fm_scratch --input "data/**/*.txt" --output ./shards
```

---

## Library Usage

Add it to your project:

```toml
[dependencies]
rust-fm-index = { path = "..." }
```

### Build a single-document index

```rust
use rust_fm_index::IndexBuilder;

let builder = IndexBuilder::new(32);
builder.build_single_document(b"hello world", "index.idx")?;
```

### Build to any writer

```rust
use rust_fm_index::IndexBuilder;

let builder = IndexBuilder::new(32);
let mut index_bytes = Vec::new();
builder.build_single_document_to_writer(b"hello world", &mut index_bytes)?;
```

### Build a multi-document index

```rust
use rust_fm_index::IndexBuilder;

let docs = vec![
    b"doc one".to_vec(),
    b"doc two".to_vec(),
];

let builder = IndexBuilder::new(32);
builder.build_multi_documents(&docs, "index.idx")?;
```

### Build with a custom scratch directory

```rust
use rust_fm_index::IndexBuilder;

let builder = IndexBuilder::new(32).with_scratch_dir("/mnt/nvme/fm_scratch");
builder.build_single_document(b"hello world", "index.idx")?;
```

### Query the index

```rust
use rust_fm_index::IndexReader;

let reader = IndexReader::open("index.idx")?;
let (sp, ep) = reader.count(b"doc")?;
let locs = reader.locate(b"doc")?;
let safe_count = reader.count_doc_safe(b"doc")?;
let safe_locs = reader.locate_doc_safe(b"doc")?;
let snippet = reader.extract(0, 5)?;

let stats = reader.stats()?;
println!("{stats:?}");
```

### Open from a custom random-access source

```rust
use rust_fm_index::{IndexReader, RandomAccessRead};
use std::io;
use std::sync::Arc;

#[derive(Clone)]
struct InMemorySource {
    data: Arc<Vec<u8>>,
}

impl RandomAccessRead for InMemorySource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start = offset as usize;
        let end = start + buf.len();
        if end > self.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Read past EOF"));
        }
        buf.copy_from_slice(&self.data[start..end]);
        Ok(())
    }
}

let index_bytes = std::fs::read("index.idx")?;
let source = InMemorySource {
    data: Arc::new(index_bytes),
};
let reader = IndexReader::open_with_source(source)?;
```

### Reader configuration (page size + prefetch)

```rust
use rust_fm_index::{IndexReader, PagedReaderConfig, PrefetchMode};

let reader = IndexReader::open_with_cache_and_reader_config(
    "index.idx",
    256 * 1024 * 1024,
    16,
    PagedReaderConfig {
        page_size: 64 * 1024,
        prefetch_pages: 2,
        prefetch_mode: PrefetchMode::Async,
    },
)?;
```

`PrefetchMode::None` disables read-ahead, `Sync` performs read-ahead in the caller thread, and `Async` uses a background thread.

### Map positions to documents + reconstruct full docs

```rust
let (doc_id, offset) = reader.pos_to_doc_id(locs[0]).unwrap();
let doc = reader.get_document(doc_id)?;
```

### Query across shards (library)

```rust
use rust_fm_index::MultiShardReader;

let reader = MultiShardReader::open("./shards")?;
let total = reader.count_merged(b"search")?;
let safe_total = reader.count_merged_doc_safe(b"search")?;

let hits = reader.locate_merged(b"search")?;
let safe_hits = reader.locate_merged_doc_safe(b"search")?;

if let Some(hit) = hits.first() {
    if let Some(pos) = hit.positions.first() {
        println!("doc_id={}, offset={}", hit.doc_id, pos);
    }
}

let doc = reader.get_document(42)?;
```

---

## Sentinel Requirements

The implementation uses **byte `0` as a sentinel**.

- Input documents **must not contain `0` bytes**.
- Single-doc builds add the sentinel automatically.
- Multi-doc builds append a single trailing `0`; document boundaries are tracked via doc offsets.

### Binary Mode Support

- This release is **text-mode only**.
- Binary-mode indexing is supported only in **previous tagged releases**.
- Binary-mode indexes produced by those legacy tags are not supported by this release.

*Footnote (future reintroduction steps):*
1. Reintroduce an explicit binary build/query mode in API and CLI.
2. Define sentinel and alphabet handling for binary mode without conflicting with text mode.
3. Add compatibility tests for text-mode and binary-mode round trips across header versions.
4. Document release/tag boundaries and migration guidance for binary users.

---

## File Format Overview

Each `.idx` shard contains:

1. Header (bincode, legacy config)
2. Global Wavelet Tree bitstream (paged with base-rank headers)
3. Sampled SA (bitpacked u32/u64 when possible, else raw u64)
4. Sampled ISA (bitpacked u32/u64 when possible, else raw u64)
5. Doc offsets encoded with **Elias-Fano**

Doc offsets are encoded with Elias-Fano to support compact storage and fast mapping from
global offsets to document IDs.

---

## Configuration Notes

- **Sample rate** controls SA/ISA sampling density.
  - Lower = faster locate/extract, larger index.
  - Higher = smaller index, more LF steps.

- **Wavelet build mode** controls how the wavelet bitvectors are built:
  - `in-memory`: fastest, but uses more RAM.
  - `streaming`: lowest RAM, slower.
  - `auto` (default): uses `in-memory` if the plan fits under 256MiB, otherwise `streaming`.

- **Scratch directory** for temp files can be set with:
  - CLI: `--scratch-dir /path/to/dir`
  - Config file (`ingest`): `scratch_dir = "/path/to/dir"`
  - Library: `IndexBuilder::with_scratch_dir(...)`
  - Env override: `FM_INDEX_SCRATCH_DIR=/path/to/dir`

- **Cache size** can be customized with:
  ```rust
  let reader = IndexReader::open_with_cache("index.idx", 256 * 1024 * 1024, 8)?;
  ```

---

## Documentation

See `docs/USAGE.md` for deeper examples and edge cases.

---

## Limitations / Next Steps

- Inputs containing `0` byte are rejected.
- No compression for SA/ISA yet.
- Multi-shard routing is basic and query-first; no global ranking or caching yet.

---

## License

MIT
