use crate::index::builder::ShardBuilder;
use crate::index::encoding::EncodingMode;
use crate::index::header::ShardHeader;
use crate::index::query::QueryEngine;
use crate::iolib::paged_reader::{GlobalPageCache, PagedReader};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;

fn decode_header_from_path(path: &std::path::Path) -> ShardHeader {
    let mut file = std::fs::File::open(path).unwrap();
    bincode::serde::decode_from_std_read(&mut file, bincode::config::legacy())
        .expect("Header parse failed")
}

fn build_query_engine(text: &[u8], sample_rate: u32) -> QueryEngine {
    build_query_engine_with_mode(text, sample_rate, EncodingMode::Text)
}

fn build_query_engine_with_mode(
    text: &[u8],
    sample_rate: u32,
    mode: EncodingMode,
) -> QueryEngine {
    let builder = ShardBuilder::new_with_mode(sample_rate, mode);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).unwrap();

    let header = decode_header_from_path(tmp_file.path());
    eprintln!("decoded offsets: {:?}", header.decode_doc_offsets().unwrap());
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
    let reader = PagedReader::new(tmp_file.path(), 1234, cache).unwrap();
    QueryEngine::new(header, reader)
}

fn with_sentinel(text: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(text.len() + 1);
    data.extend_from_slice(text);
    data.push(0);
    data
}

fn naive_locations(text: &[u8], pattern: &[u8]) -> Vec<usize> {
    if pattern.is_empty() || pattern.len() > text.len() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for i in 0..=text.len() - pattern.len() {
        if &text[i..i + pattern.len()] == pattern {
            out.push(i);
        }
    }
    out
}

fn naive_count(text: &[u8], pattern: &[u8]) -> usize {
    if pattern.is_empty() || pattern.len() > text.len() {
        return 0;
    }
    let mut count = 0;
    for i in 0..=text.len() - pattern.len() {
        if &text[i..i + pattern.len()] == pattern {
            count += 1;
        }
    }
    count
}

#[test]
fn test_search_mississippi() {
    // 1. Build Index
    let text = b"mississippi";
    let query = build_query_engine(text, 2); // Aggressive sampling (every 2nd)

    // 2. Test Count (FM-index backward search over BWT)
    let (sp, ep) = query.count(b"issi").unwrap();
    assert_eq!(ep - sp + 1, 2);

    let (sp_ss, ep_ss) = query.count(b"ss").unwrap();
    assert_eq!(ep_ss - sp_ss + 1, 2);

    let (sp_z, ep_z) = query.count(b"z").unwrap();
    assert!(sp_z > ep_z);

    // 3. Test Locate
    let mut locs = query.locate(b"issi").unwrap();
    locs.sort();
    assert_eq!(locs, vec![1, 4]);

    let mut i_locs = query.locate(b"i").unwrap();
    i_locs.sort();
    assert_eq!(i_locs, vec![1, 4, 7, 10]);

    let mut ss_locs = query.locate(b"ss").unwrap();
    ss_locs.sort();
    assert_eq!(ss_locs, vec![2, 5]);
}

#[test]
fn test_query_random_text_matches_naive() {
    let mut rng = StdRng::seed_from_u64(2025);
    let len = 500;
    let mut text = Vec::with_capacity(len + 1);
    for _ in 0..len {
        text.push(rng.random_range(1..=8));
    }
    let query = build_query_engine(&text, 3);

    for _ in 0..200 {
        let pat_len = rng.random_range(1..=4);
        let mut pattern = Vec::with_capacity(pat_len);
        for _ in 0..pat_len {
            pattern.push(rng.random_range(1..=8));
        }

        let expected = naive_locations(&text, &pattern);
        let (sp, ep) = query.count(&pattern).unwrap();
        let count = if sp > ep { 0 } else { ep - sp + 1 };
        assert_eq!(count, expected.len());

        let mut locs = query.locate(&pattern).unwrap();
        locs.sort();
        assert_eq!(locs, expected);
    }
}

#[test]
fn test_query_edge_cases() {
    let text = b"aaaaa";
    let query = build_query_engine(text, 4);

    let mut locs = query.locate(b"a").unwrap();
    locs.sort();
    assert_eq!(locs, vec![0, 1, 2, 3, 4]);

    let (sp, ep) = query.count(b"aaaa").unwrap();
    assert_eq!(ep - sp + 1, 2);

    let (sp_none, ep_none) = query.count(b"b").unwrap();
    assert!(sp_none > ep_none);
}

#[test]
fn test_query_10mb_file() {
    let total_len = 10 * 1024 * 1024; // 10MB data
    let data_len = total_len;
    let base = b"abcd";

    let mut text = Vec::with_capacity(total_len);
    while text.len() < data_len {
        let remaining = data_len - text.len();
        if remaining >= base.len() {
            text.extend_from_slice(base);
        } else {
            text.extend_from_slice(&base[..remaining]);
        }
    }

    let marker1 = [200u8, 201, 202, 203, 204];
    let marker2 = [210u8, 211, 212, 213, 214];
    let marker3 = [220u8, 221, 222, 223, 224];

    let pos1 = 12345usize;
    let pos2 = data_len / 2;
    let pos3 = data_len - marker3.len() - 123;

    text[pos1..pos1 + marker1.len()].copy_from_slice(&marker1);
    text[pos2..pos2 + marker2.len()].copy_from_slice(&marker2);
    text[pos3..pos3 + marker3.len()].copy_from_slice(&marker3);

    let sample_rate = 64;
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(&text, tmp_file.path()).unwrap();

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(32 * 1024 * 1024, 2));
    let reader = PagedReader::new(tmp_file.path(), 5678, cache).unwrap();
    let query = QueryEngine::new(header, reader);

    // Count checks (BWT-based)
    let expected_abcd = naive_count(&text, base);
    let (sp_abcd, ep_abcd) = query.count(base).unwrap();
    let count_abcd = if sp_abcd > ep_abcd {
        0
    } else {
        ep_abcd - sp_abcd + 1
    };
    assert_eq!(count_abcd, expected_abcd);

    let (sp_none, ep_none) = query.count(&[250, 251, 252]).unwrap();
    assert!(sp_none > ep_none);

    // Locate checks for unique markers
    let locs1 = query.locate(&marker1).unwrap();
    assert_eq!(locs1, vec![pos1]);

    let locs2 = query.locate(&marker2).unwrap();
    assert_eq!(locs2, vec![pos2]);

    let locs3 = query.locate(&marker3).unwrap();
    assert_eq!(locs3, vec![pos3]);

    // Extract checks for unique markers
    let extract1 = query.extract(pos1, marker1.len()).unwrap();
    assert_eq!(extract1, marker1);
    let extract2 = query.extract(pos2, marker2.len()).unwrap();
    assert_eq!(extract2, marker2);
    let extract3 = query.extract(pos3, marker3.len()).unwrap();
    assert_eq!(extract3, marker3);
}

#[test]
fn test_extract_roundtrip_random() {
    let mut rng = StdRng::seed_from_u64(4242);
    let len = 2000;
    let mut text = Vec::with_capacity(len + 1);
    for _ in 0..len {
        text.push(rng.random_range(1..=50));
    }
    let query = build_query_engine(&text, 7);

    for _ in 0..200 {
        let start = rng.random_range(0..text.len());
        let max_len = text.len() - start;
        let slice_len = rng.random_range(0..=max_len.min(64));
        let expected = text[start..start + slice_len].to_vec();
        let actual = query.extract(start, slice_len).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_extract_end_boundary() {
    let text = b"abcd";
    let query = build_query_engine(text, 2);

    let tail = query.extract(4, 1).unwrap();
    assert_eq!(tail, vec![0u8]);

    let end_two = query.extract(3, 2).unwrap();
    assert_eq!(end_two, b"d\0");

    let full = query.extract(0, text.len() + 1).unwrap();
    assert_eq!(full, with_sentinel(text));
}

#[test]
fn test_multi_document_retrieval() {
    let doc1 = b"Hello World";
    let doc2 = b"Rust is fast";
    let doc3 = b"FM-index search";

    let mut text = Vec::new();
    let mut offsets = Vec::new();

    offsets.push(text.len() as u64);
    text.extend_from_slice(doc1);

    offsets.push(text.len() as u64);
    text.extend_from_slice(doc2);

    offsets.push(text.len() as u64);
    text.extend_from_slice(doc3);

    let builder = ShardBuilder::new(4);
    let tmp_file = NamedTempFile::new().unwrap();
    builder
        .build_with_offsets(&text, offsets.clone(), tmp_file.path())
        .unwrap();

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(1024 * 1024, 1));
    let reader = PagedReader::new(tmp_file.path(), 888, cache).unwrap();
    let query = QueryEngine::new(header, reader);

    let locs = query.locate(b"fast").unwrap();
    assert_eq!(locs.len(), 1);
    let global_pos = locs[0];
    let (doc_id, offset) = query.pos_to_doc_id(global_pos).unwrap();
    assert_eq!(doc_id, 1);
    assert_eq!(offset, 8);

    let retrieved_doc1 = query.get_document(0).unwrap();
    assert_eq!(retrieved_doc1, doc1);
    let retrieved_doc2 = query.get_document(1).unwrap();
    assert_eq!(retrieved_doc2, doc2);
    let retrieved_doc3 = query.get_document(2).unwrap();
    assert_eq!(retrieved_doc3, doc3);
}

#[test]
fn test_binary_mode_roundtrip_and_search() {
    let data = vec![0u8, 1, 2, 255, 0, 5, 6, 7, 255];
    let query = build_query_engine_with_mode(&data, 4, EncodingMode::Binary);

    let mut locs = query.locate(&[0, 1]).unwrap();
    locs.sort();
    assert_eq!(locs, vec![0]);

    let mut locs_255 = query.locate(&[255]).unwrap();
    locs_255.sort();
    assert_eq!(locs_255, vec![3, 8]);

    let extracted = query.extract(0, data.len()).unwrap();
    assert_eq!(extracted, data);
}
