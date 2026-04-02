use crate::index::builder::ShardBuilder;
use crate::index::encoding::EncodingMode;
use crate::index::header::ShardHeader;
use crate::index::query::QueryEngine;
use crate::index::wavelet::WaveletBuildMode;
use crate::iolib::paged_reader::{
    GlobalPageCache, PagedReader, PagedReaderConfig, RandomAccessRead, SharedRandomAccessRead,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// High-level builder for creating FM-index shards.
pub struct IndexBuilder {
    sample_rate: u32,
    encoding_mode: EncodingMode,
    wavelet_mode: WaveletBuildMode,
    scratch_dir: Option<PathBuf>,
}

impl IndexBuilder {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            encoding_mode: EncodingMode::Text,
            wavelet_mode: WaveletBuildMode::default(),
            scratch_dir: None,
        }
    }

    pub fn with_encoding_mode(mut self, encoding_mode: EncodingMode) -> Self {
        self.encoding_mode = encoding_mode;
        self
    }

    pub fn with_wavelet_mode(mut self, wavelet_mode: WaveletBuildMode) -> Self {
        self.wavelet_mode = wavelet_mode;
        self
    }

    pub fn with_scratch_dir<P: AsRef<Path>>(mut self, scratch_dir: P) -> Self {
        self.scratch_dir = Some(scratch_dir.as_ref().to_path_buf());
        self
    }

    fn shard_builder(&self) -> ShardBuilder {
        let builder =
            ShardBuilder::new_with_modes(self.sample_rate, self.encoding_mode, self.wavelet_mode);
        if let Some(scratch_dir) = self.scratch_dir.as_deref() {
            builder.with_scratch_dir(scratch_dir)
        } else {
            builder
        }
    }

    /// Build a single-document index. A trailing sentinel (0 byte) is added.
    /// In text mode, fails if the input already contains a 0 byte.
    pub fn build_single_document<P: AsRef<Path>>(
        &self,
        text: &[u8],
        output_path: P,
    ) -> io::Result<()> {
        let file = std::fs::File::create(output_path)?;
        self.build_single_document_to_writer(text, file)
    }

    /// Build a single-document index to any writer. A trailing sentinel (0 byte) is added.
    /// In text mode, fails if the input already contains a 0 byte.
    pub fn build_single_document_to_writer<W: Write>(
        &self,
        text: &[u8],
        writer: W,
    ) -> io::Result<()> {
        let builder = self.shard_builder();
        builder.build_with_offsets_to_writer(text, vec![0], writer)
    }

    /// Build a multi-document index by concatenating documents and appending a single 0 byte
    /// sentinel at the end. Document boundaries are tracked via doc offsets.
    /// In text mode, fails if any input contains a 0 byte.
    pub fn build_multi_documents<P: AsRef<Path>>(
        &self,
        docs: &[Vec<u8>],
        output_path: P,
    ) -> io::Result<()> {
        let file = std::fs::File::create(output_path)?;
        self.build_multi_documents_to_writer(docs, file)
    }

    /// Build a multi-document index to any writer by concatenating documents
    /// and appending a single 0 byte sentinel at the end.
    pub fn build_multi_documents_to_writer<W: Write>(
        &self,
        docs: &[Vec<u8>],
        writer: W,
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
            offsets.push(text.len() as u64);
            text.extend_from_slice(doc);
        }

        let builder = self.shard_builder();
        builder.build_with_offsets_to_writer(&text, offsets, writer)
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
    /// The text is treated as raw bytes; a sentinel is appended after encoding.
    /// In text mode, the text must not contain any 0 bytes.
    pub fn build_from_concatenated<P: AsRef<Path>>(
        &self,
        text: &[u8],
        doc_offsets: &[u64],
        output_path: P,
    ) -> io::Result<()> {
        let file = std::fs::File::create(output_path)?;
        self.build_from_concatenated_to_writer(text, doc_offsets, file)
    }

    /// Build from concatenated text and explicit document offsets to any writer.
    pub fn build_from_concatenated_to_writer<W: Write>(
        &self,
        text: &[u8],
        doc_offsets: &[u64],
        writer: W,
    ) -> io::Result<()> {
        validate_doc_offsets(text.len(), doc_offsets)?;
        let builder = self.shard_builder();
        builder.build_with_offsets_to_writer(text, doc_offsets.to_vec(), writer)
    }
}

/// High-level reader for querying an index shard.
pub struct IndexReader {
    header: ShardHeader,
    engine: QueryEngine,
    index_bytes: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexStats {
    pub text_bytes: u64,
    pub index_bytes: u64,
    pub header_bytes: u64,
    pub wavelet_bytes: u64,
    pub sa_bytes: u64,
    pub isa_bytes: u64,
    pub sa_samples: u64,
    pub isa_samples: u64,
    pub sa_sample_rate: u32,
    pub isa_sample_rate: u32,
    pub sa_bits: u8,
    pub isa_bits: u8,
    pub doc_offsets_count: u32,
    pub doc_offsets_u_bits_bytes: u64,
    pub doc_offsets_l_bits_bytes: u64,
}

impl IndexReader {
    /// Open an index from any random-access source.
    pub fn open_with_source<R>(source: R) -> io::Result<Self>
    where
        R: RandomAccessRead + 'static,
    {
        Self::open_with_shared_source(Arc::new(source))
    }

    /// Open an index from a shared random-access source.
    pub fn open_with_shared_source(source: SharedRandomAccessRead) -> io::Result<Self> {
        let index_bytes = source.len();
        let header = read_header_from_source(source.as_ref())?;
        let engine = QueryEngine::new_with_shared_source(header.clone(), source);
        Ok(Self {
            header,
            engine,
            index_bytes,
        })
    }

    /// Open an index with a default cache configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with_cache_and_reader_config(
            path,
            128 * 1024 * 1024,
            8,
            PagedReaderConfig::default(),
        )
    }

    /// Open an index using a shared cache.
    pub fn open_with_shared_cache<P: AsRef<Path>>(
        path: P,
        cache: Arc<GlobalPageCache>,
    ) -> io::Result<Self> {
        Self::open_with_shared_cache_and_reader_config(path, cache, PagedReaderConfig::default())
    }

    /// Open an index using a shared cache and custom reader configuration.
    pub fn open_with_shared_cache_and_reader_config<P: AsRef<Path>>(
        path: P,
        cache: Arc<GlobalPageCache>,
        reader_config: PagedReaderConfig,
    ) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let file_id = file_id_for_path(path_ref);
        let reader = PagedReader::new_with_config(path_ref, file_id, cache, reader_config)?;
        Self::open_with_source(reader)
    }

    /// Open an index with a custom cache size and shard count.
    pub fn open_with_cache<P: AsRef<Path>>(
        path: P,
        cache_bytes: usize,
        cache_shards: usize,
    ) -> io::Result<Self> {
        Self::open_with_cache_and_reader_config(
            path,
            cache_bytes,
            cache_shards,
            PagedReaderConfig::default(),
        )
    }

    /// Open an index with custom cache and reader configuration.
    pub fn open_with_cache_and_reader_config<P: AsRef<Path>>(
        path: P,
        cache_bytes: usize,
        cache_shards: usize,
        reader_config: PagedReaderConfig,
    ) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let cache = Arc::new(GlobalPageCache::new(cache_bytes, cache_shards));
        let file_id = file_id_for_path(path_ref);
        let reader = PagedReader::new_with_config(path_ref, file_id, cache, reader_config)?;
        Self::open_with_source(reader)
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

    pub fn locate_doc_safe(&self, pattern: &[u8]) -> io::Result<Vec<usize>> {
        self.engine.locate_doc_safe(pattern)
    }

    pub fn count_doc_safe(&self, pattern: &[u8]) -> io::Result<usize> {
        self.engine.count_doc_safe(pattern)
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

    pub fn stats(&self) -> io::Result<IndexStats> {
        let header_bytes = bincode::serde::encode_to_vec(&self.header, bincode::config::legacy())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .len() as u64;

        if self.header.wt_start_offset < header_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header offsets are inconsistent",
            ));
        }

        let wt_start = self.header.wt_start_offset;
        let sa_start = self.header.sa_start_offset;
        let isa_start = self.header.isa_start_offset;

        if wt_start > sa_start || sa_start > isa_start || isa_start > self.index_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index offsets exceed file size",
            ));
        }

        let wavelet_bytes = sa_start.saturating_sub(wt_start);
        let sa_bytes = isa_start.saturating_sub(sa_start);
        let isa_bytes = self.index_bytes.saturating_sub(isa_start);

        let text_bytes = self.header.text_len;
        let sa_sample_rate = self.header.sa_sample_rate;
        let isa_sample_rate = self.header.isa_sample_rate;
        let sa_samples = if sa_sample_rate == 0 {
            0
        } else {
            text_bytes.div_ceil(sa_sample_rate as u64)
        };
        let isa_samples = if isa_sample_rate == 0 {
            0
        } else {
            text_bytes.div_ceil(isa_sample_rate as u64)
        };

        Ok(IndexStats {
            text_bytes,
            index_bytes: self.index_bytes,
            header_bytes,
            wavelet_bytes,
            sa_bytes,
            isa_bytes,
            sa_samples,
            isa_samples,
            sa_sample_rate,
            isa_sample_rate,
            sa_bits: self.header.sa_bits,
            isa_bits: self.header.isa_bits,
            doc_offsets_count: self.header.doc_offsets_count,
            doc_offsets_u_bits_bytes: self.header.doc_offsets_u_bits.len() as u64,
            doc_offsets_l_bits_bytes: self.header.doc_offsets_l_bits.len() as u64,
        })
    }
}

fn file_id_for_path(path: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    hasher.finish()
}

struct RandomAccessCursor<'a> {
    source: &'a dyn RandomAccessRead,
    pos: u64,
    len: u64,
}

impl<'a> RandomAccessCursor<'a> {
    fn new(source: &'a dyn RandomAccessRead) -> Self {
        Self {
            source,
            pos: 0,
            len: source.len(),
        }
    }
}

impl Read for RandomAccessCursor<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }

        let remaining = self.len - self.pos;
        let to_read = buf.len().min(remaining.min(usize::MAX as u64) as usize);
        self.source.read_exact_at(self.pos, &mut buf[..to_read])?;
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

fn read_header_from_source(source: &dyn RandomAccessRead) -> io::Result<ShardHeader> {
    let mut cursor = RandomAccessCursor::new(source);
    bincode::serde::decode_from_std_read(&mut cursor, bincode::config::legacy())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
