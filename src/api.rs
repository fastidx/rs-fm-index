use crate::index::builder::ShardBuilder;
use crate::index::header::ShardHeader;
use crate::index::query::QueryEngine;
use crate::iolib::paged_reader::{GlobalPageCache, PagedReader};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// High-level builder for creating FM-index shards.
pub struct IndexBuilder {
    sample_rate: u32,
}

impl IndexBuilder {
    pub fn new(sample_rate: u32) -> Self {
        Self { sample_rate }
    }

    /// Build a single-document index. A trailing sentinel (0 byte) is added.
    /// Fails if the input already contains a 0 byte.
    pub fn build_single_document<P: AsRef<Path>>(
        &self,
        text: &[u8],
        output_path: P,
    ) -> io::Result<()> {
        if text.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "input contains 0 byte; cannot use 0 as sentinel",
            ));
        }
        let mut data = Vec::with_capacity(text.len() + 1);
        data.extend_from_slice(text);
        data.push(0);
        let builder = ShardBuilder::new(self.sample_rate);
        builder.build_with_offsets(&data, vec![0], output_path)
    }

    /// Build a multi-document index. Each document is separated by a 0 byte sentinel.
    /// Fails if any input contains a 0 byte.
    pub fn build_multi_documents<P: AsRef<Path>>(
        &self,
        docs: &[Vec<u8>],
        output_path: P,
    ) -> io::Result<()> {
        if docs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "docs must contain at least one document",
            ));
        }

        let mut text = Vec::new();
        let mut offsets = Vec::with_capacity(docs.len());

        for doc in docs {
            if doc.contains(&0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "document contains 0 byte; cannot use 0 as sentinel",
                ));
            }
            offsets.push(text.len() as u64);
            text.extend_from_slice(doc);
            text.push(0);
        }

        let builder = ShardBuilder::new(self.sample_rate);
        builder.build_with_offsets(&text, offsets, output_path)
    }

    /// Build a multi-document index from file paths.
    pub fn build_multi_from_paths<P: AsRef<Path>>(
        &self,
        output_path: P,
        inputs: &[PathBuf],
    ) -> io::Result<()> {
        if inputs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inputs must contain at least one path",
            ));
        }

        let mut docs = Vec::with_capacity(inputs.len());
        for path in inputs {
            let data = std::fs::read(path)?;
            docs.push(data);
        }
        self.build_multi_documents(&docs, output_path)
    }

    /// Build from concatenated text and explicit document offsets.
    /// The caller is responsible for sentinel placement if needed.
    pub fn build_from_concatenated<P: AsRef<Path>>(
        &self,
        text: &[u8],
        doc_offsets: &[u64],
        output_path: P,
    ) -> io::Result<()> {
        validate_doc_offsets(text.len(), doc_offsets)?;
        let builder = ShardBuilder::new(self.sample_rate);
        builder.build_with_offsets(text, doc_offsets.to_vec(), output_path)
    }
}

/// High-level reader for querying an index shard.
pub struct IndexReader {
    header: ShardHeader,
    engine: QueryEngine,
}

impl IndexReader {
    /// Open an index with a default cache configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with_cache(path, 128 * 1024 * 1024, 8)
    }

    /// Open an index with a custom cache size and shard count.
    pub fn open_with_cache<P: AsRef<Path>>(
        path: P,
        cache_bytes: usize,
        cache_shards: usize,
    ) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let mut file = std::fs::File::open(path_ref)?;
        let header: ShardHeader =
            bincode::serde::decode_from_std_read(&mut file, bincode::config::legacy())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let cache = Arc::new(GlobalPageCache::new(cache_bytes, cache_shards));

        let mut hasher = DefaultHasher::new();
        path_ref.to_string_lossy().hash(&mut hasher);
        let file_id = hasher.finish();

        let reader = PagedReader::new(path_ref, file_id, cache)?;
        let engine = QueryEngine::new(header.clone(), reader);
        Ok(Self { header, engine })
    }

    pub fn header(&self) -> &ShardHeader {
        &self.header
    }

    pub fn count(&self, pattern: &[u8]) -> io::Result<(usize, usize)> {
        self.engine.count(pattern)
    }

    pub fn locate(&self, pattern: &[u8]) -> io::Result<Vec<usize>> {
        self.engine.locate(pattern)
    }

    pub fn extract(&self, start: usize, len: usize) -> io::Result<Vec<u8>> {
        self.engine.extract(start, len)
    }

    pub fn pos_to_doc_id(&self, pos: usize) -> Option<(usize, usize)> {
        self.engine.pos_to_doc_id(pos)
    }

    pub fn get_document(&self, doc_id: usize) -> io::Result<Vec<u8>> {
        self.engine.get_document(doc_id)
    }

    pub fn doc_count(&self) -> io::Result<usize> {
        let offsets = self.header.decode_doc_offsets()?;
        Ok(offsets.len())
    }
}

fn validate_doc_offsets(text_len: usize, offsets: &[u64]) -> io::Result<()> {
    if offsets.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "doc_offsets must contain at least one entry",
        ));
    }
    if offsets[0] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "doc_offsets must start at 0",
        ));
    }
    let mut prev = 0u64;
    for &off in offsets {
        if off < prev || off as usize > text_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc_offsets must be sorted and within text length",
            ));
        }
        prev = off;
    }
    Ok(())
}
