use crate::index::builder::ShardBuilder;
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
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).unwrap();

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
    let reader = PagedReader::new(tmp_file.path(), 1234, cache).unwrap();
    QueryEngine::new(header, reader)
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

#[test]
fn test_search_mississippi() {
    // 1. Build Index
    let text = b"mississippi\0";
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
    text.push(0); // sentinel

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
    let text = b"aaaaa\0";
    let query = build_query_engine(text, 4);

    let mut locs = query.locate(b"a").unwrap();
    locs.sort();
    assert_eq!(locs, vec![0, 1, 2, 3, 4]);

    let (sp, ep) = query.count(b"aaaa").unwrap();
    assert_eq!(ep - sp + 1, 2);

    let (sp_none, ep_none) = query.count(b"b").unwrap();
    assert!(sp_none > ep_none);
}
