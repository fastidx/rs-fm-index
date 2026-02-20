# Wavelet Tree Encoding (FM-Index)

This project builds and queries an FM-index backed by a Huffman-shaped Wavelet Tree, stored on disk with a paged bitvector and a sampled SA/ISA. It supports both **CLI usage** and **library integration**.

Last updated: 2026-02-18

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

### Query the index

```rust
use rust_fm_index::IndexReader;

let reader = IndexReader::open("index.idx")?;
let (sp, ep) = reader.count(b"doc")?;
let locs = reader.locate(b"doc")?;
let snippet = reader.extract(0, 5)?;

let stats = reader.stats()?;
println!("{stats:?}");
```

### Map positions to documents + reconstruct full docs

```rust
let (doc_id, offset) = reader.pos_to_doc_id(locs[0]).unwrap();
let doc = reader.get_document(doc_id)?;
```

---

## Sentinel Requirements

The implementation uses **byte `0` as a sentinel**.

Text mode (default):

- Input documents **must not contain `0` bytes**.
- Single-doc builds add the sentinel automatically.
- Multi-doc builds append a single trailing `0`; document boundaries are tracked via doc offsets.

Binary mode:

- Enable with `--binary` (CLI) or `IndexBuilder::with_encoding_mode(EncodingMode::Binary)`.
- Input bytes are remapped with `b + 1`, and `0` is reserved for the sentinel.
- Queries/extracts are decoded automatically; the mode is stored in the header.

If your input can contain `0`, use binary mode.

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
