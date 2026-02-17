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

Extract a full document:

```
cargo run --release -- doc ./index.idx 2 > doc3.txt
```

---

## Library Integration

### Build a single-doc index

```rust
use rust_fm_index::IndexBuilder;

let builder = IndexBuilder::new(32);
builder.build_single_document(b"hello world", "index.idx")?;
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

if let Some(pos) = locs.first() {
    if let Some((doc_id, offset)) = reader.pos_to_doc_id(*pos) {
        println!("doc_id={}, offset={}", doc_id, offset);
    }
}

let stats = reader.stats()?;
println!("{stats:?}");
```

### Reconstruct a document

```rust
let doc = reader.get_document(0)?;
```

---

## Sentinel Notes

This implementation uses `0` as a sentinel byte.

Inputs **must not contain `0`**. If you need to index binary files or data that may include 0, you must pre-process (escape) those bytes or adopt a different sentinel.

---

## Performance Tips

- Increase `sample_rate` to reduce index size.
- Decrease `sample_rate` for faster locate/extract.
- Increase cache size for better query performance:

```rust
let reader = IndexReader::open_with_cache("index.idx", 512 * 1024 * 1024, 16)?;
```

---

## Common Pitfalls

- If extraction doesn’t reproduce the original file, check sentinel handling.
- Doc offsets are Elias-Fano encoded; if you manually construct headers, keep offsets sorted.
- If queries return zero results, verify the input text was built with a sentinel.
