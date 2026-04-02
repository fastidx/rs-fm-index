use crate::{IndexBuilder, IndexReader, RandomAccessRead, SharedRandomAccessRead};
use std::io;
use std::sync::Arc;

#[derive(Clone)]
struct InMemorySource {
    bytes: Arc<Vec<u8>>,
}

impl InMemorySource {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Arc::new(bytes),
        }
    }
}

impl RandomAccessRead for InMemorySource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start = offset as usize;
        let end = start
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        if end > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read past EOF",
            ));
        }
        buf.copy_from_slice(&self.bytes[start..end]);
        Ok(())
    }
}

#[test]
fn test_builder_writer_and_reader_source_roundtrip() {
    let builder = IndexBuilder::new(4);
    let mut index_bytes = Vec::new();
    builder
        .build_single_document_to_writer(b"banana", &mut index_bytes)
        .unwrap();

    let reader = IndexReader::open_with_source(InMemorySource::new(index_bytes)).unwrap();
    let mut locs = reader.locate(b"ana").unwrap();
    locs.sort();
    assert_eq!(locs, vec![1, 3]);
    assert_eq!(reader.extract(0, 6).unwrap(), b"banana");
}

#[test]
fn test_open_with_shared_source_roundtrip() {
    let builder = IndexBuilder::new(4);
    let mut index_bytes = Vec::new();
    builder
        .build_single_document_to_writer(b"mississippi", &mut index_bytes)
        .unwrap();

    let source: SharedRandomAccessRead = Arc::new(InMemorySource::new(index_bytes));
    let reader = IndexReader::open_with_shared_source(source).unwrap();

    let (sp, ep) = reader.count(b"issi").unwrap();
    assert_eq!(ep - sp + 1, 2);
}

#[test]
fn test_builder_invalid_scratch_dir_fails() {
    let base = tempfile::TempDir::new().unwrap();
    let missing = base.path().join("missing-scratch-dir");

    let builder = IndexBuilder::new(4).with_scratch_dir(&missing);
    let mut index_bytes = Vec::new();
    let err = builder
        .build_single_document_to_writer(b"banana", &mut index_bytes)
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}
