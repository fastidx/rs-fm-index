# Usage Guide

This guide covers common usage patterns for both CLI and library integration.

---

## CLI Workflows

### Single Document

Build:

```
cargo run --release -- build ./input.txt ./index.idx
```

Query:

```
cargo run --release -- query ./index.idx "pattern"
```

Extract:

```
cargo run --release -- extract ./index.idx 100 64
```

Stats:

```
cargo run --release -- stats ./index.idx
```

Extract full document:

```
cargo run --release -- doc ./index.idx 0 > recovered.txt
```

---

### Multiple Documents

Build a multi-doc index:

```
cargo run --release -- build-multi ./index.idx ./doc1.txt ./doc2.txt ./doc3.txt
```

### Distributed Ingestion (Sharded)

```
cargo run --release -- ingest --input "data/**/*.txt" --output ./shards --chunk-size 1GiB --workers 8
```

### Distributed Ingestion with Config

`ingest.toml`:

```toml
input_patterns = ["data/**/*.txt"]
output_dir = "shards"
chunk_size = "1GiB"
read_buffer = "8MiB"
num_workers = 8
sample_rate = 32
```

Run:

```
cargo run --release -- ingest --config ingest.toml
```

Outputs:
- `shard_00000.meta.json` (continuation metadata)
- `shard_00000.stats.json` (per-shard stats)
- `ingest_report.json` (overall ingest report)

Query and map to document:

```
cargo run --release -- query ./index.idx "search"
```

The CLI prints the first few positions and their `(doc_id, offset)` pairs.

Query a shard directory:

```
cargo run --release -- query ./shards "search"
```

Shard queries merge segment hits back into document offsets and account for matches that cross shard boundaries within a document.

Doc-safe query (prevents cross-doc matches):

```
cargo run --release -- query --doc-safe ./index.idx "search"
cargo run --release -- query --doc-safe ./shards "search"
```

Extract a full document:

```
cargo run --release -- doc ./index.idx 2 > doc3.txt
```

Extract a full document from shards:

```
cargo run --release -- doc ./shards 2 > doc3.txt
```

---

## Library Integration

### Build a single-doc index

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

### Wavelet build mode

```rust
use rust_fm_index::{IndexBuilder, WaveletBuildMode};

let builder = IndexBuilder::new(32)
    .with_wavelet_mode(WaveletBuildMode::Auto { max_bytes: 256 * 1024 * 1024 });
builder.build_single_document(b"hello world", "index.idx")?;
```

### Doc-safe queries

```rust
use rust_fm_index::IndexReader;

let reader = IndexReader::open("index.idx")?;
let safe_count = reader.count_doc_safe(b"doc")?;
let safe_locs = reader.locate_doc_safe(b"doc")?;
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

### Build a multi-doc index

```rust
use rust_fm_index::IndexBuilder;

let docs = vec![
    b"doc1".to_vec(),
    b"doc2".to_vec(),
];

let builder = IndexBuilder::new(32);
builder.build_multi_documents(&docs, "index.idx")?;
```

### Open and query

```rust
use rust_fm_index::IndexReader;

let reader = IndexReader::open("index.idx")?;

let (sp, ep) = reader.count(b"doc")?;
let locs = reader.locate(b"doc")?;
let safe_count = reader.count_doc_safe(b"doc")?;
let safe_locs = reader.locate_doc_safe(b"doc")?;

if let Some(pos) = locs.first() {
    if let Some((doc_id, offset)) = reader.pos_to_doc_id(*pos) {
        println!("doc_id={}, offset={}", doc_id, offset);
    }
}

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

### Reconstruct a document

```rust
let doc = reader.get_document(0)?;
```

---

## Sentinel Notes

This implementation uses `0` as a sentinel byte. Multi-document builds append a single trailing
sentinel; document boundaries are tracked via doc offsets (no separators between documents).

Text mode (default):

- Inputs **must not contain `0`**.

Binary mode:

- Enable with `--binary` (CLI) or `IndexBuilder::with_encoding_mode(EncodingMode::Binary)`.
- Bytes are remapped with `b + 1`, and `0` is reserved for the sentinel.

---

## Performance Tips

- Increase `sample_rate` to reduce index size.
- Decrease `sample_rate` for faster locate/extract.
- Increase cache size for better query performance:

```rust
let reader = IndexReader::open_with_cache("index.idx", 512 * 1024 * 1024, 16)?;
```

- Tune page size and prefetch to balance random access vs throughput:

```rust
use rust_fm_index::{PagedReaderConfig, PrefetchMode};

let reader = IndexReader::open_with_cache_and_reader_config(
    "index.idx",
    512 * 1024 * 1024,
    16,
    PagedReaderConfig {
        page_size: 64 * 1024,
        prefetch_pages: 2,
        prefetch_mode: PrefetchMode::Async,
    },
)?;
```

`PrefetchMode::None` disables read-ahead, `Sync` performs read-ahead in the caller thread, and `Async` uses a background thread.

---

## Common Pitfalls

- If extraction doesn’t reproduce the original file, check sentinel handling.
- Doc offsets are Elias-Fano encoded; if you manually construct headers, keep offsets sorted.
- If queries return zero results, verify the input text was built with a sentinel.
