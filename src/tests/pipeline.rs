use crate::index::builder::ShardBuilder;
use crate::index::header::{ShardHeader, CURRENT_VERSION, MAGIC_BYTES};
use crate::index::sampled_sa::PagedSampledSA;
use crate::index::wavelet::{HuffmanCode, PagedWaveletTree, WaveletNodeShape};
use crate::iolib::paged_reader::{GlobalPageCache, PagedReader};
use cdivsufsort::sort as div_sort;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;

fn decode_header_from_path(path: &std::path::Path) -> ShardHeader {
    let mut file = std::fs::File::open(path).unwrap();
    bincode::serde::decode_from_std_read(&mut file, bincode::config::legacy())
        .expect("Header parse failed")
}

fn sample_count(len: usize, rate: u32) -> usize {
    if len == 0 {
        0
    } else {
        (len - 1) / rate as usize + 1
    }
}

fn build_bwt(text: &[u8]) -> Vec<u8> {
    let (_, sa) = div_sort(text).into_parts();
    let len = text.len();
    let mut bwt = Vec::with_capacity(len);
    for &sa_val in &sa {
        let pos = sa_val as usize;
        if pos == 0 {
            bwt.push(text[len - 1]);
        } else {
            bwt.push(text[pos - 1]);
        }
    }
    bwt
}

#[test]
fn test_header_roundtrip() {
    let mut c_table = [0u64; 256];
    for i in 0..256 {
        c_table[i] = (i as u64) * 7;
    }

    let mut codes = [None; 256];
    codes[0] = Some(HuffmanCode { bits: 0b1, len: 1 });
    codes[255] = Some(HuffmanCode { bits: 0b10, len: 2 });

    let tree_shape = vec![
        WaveletNodeShape::Internal {
            left_idx: 1,
            right_idx: 2,
            bit_start: 123,
            bit_len: 456,
        },
        WaveletNodeShape::Leaf { symbol: 0 },
        WaveletNodeShape::Leaf { symbol: 255 },
    ];

    let doc_offsets = vec![0u64, 5, 10, 20, 21];
    let mut header = ShardHeader::new(101, 4, 4, c_table, codes, tree_shape.clone(), doc_offsets.clone());
    header.wt_start_offset = 777;
    header.sa_start_offset = 888;

    let bytes = bincode::serde::encode_to_vec(&header, bincode::config::legacy()).unwrap();
    let (decoded, _) =
        bincode::serde::decode_from_slice::<ShardHeader, _>(&bytes, bincode::config::legacy())
            .unwrap();

    assert_eq!(decoded.magic, MAGIC_BYTES);
    assert_eq!(decoded.version, CURRENT_VERSION);
    assert_eq!(decoded.text_len, 101);
    assert_eq!(decoded.sa_sample_rate, 4);
    assert_eq!(decoded.wt_start_offset, 777);
    assert_eq!(decoded.sa_start_offset, 888);
    assert_eq!(decoded.tree_shape.len(), tree_shape.len());
    assert_eq!(decoded.c_table[10], c_table[10]);
    assert_eq!(decoded.codes[0], codes[0]);
    assert_eq!(decoded.codes[255], codes[255]);
    let decoded_offsets = decoded.decode_doc_offsets().unwrap();
    assert_eq!(decoded_offsets, doc_offsets);
}

#[test]
fn test_builder_offsets_and_lengths() {
    let text = b"banana\0";
    let sample_rate = 3;
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).expect("Build failed");

    let header = decode_header_from_path(tmp_file.path());
    assert_eq!(header.magic, MAGIC_BYTES);
    assert_eq!(header.version, CURRENT_VERSION);

    let header_size =
        bincode::serde::encode_to_vec(&header, bincode::config::legacy()).unwrap().len() as u64;
    assert_eq!(header.wt_start_offset, header_size);

    let file_len = std::fs::metadata(tmp_file.path()).unwrap().len();
    let expected_sa_bytes = sample_count(text.len(), sample_rate) as u64 * 8;
    let expected_isa_bytes = sample_count(text.len(), header.isa_sample_rate) as u64 * 8;
    assert_eq!(header.isa_start_offset, header.sa_start_offset + expected_sa_bytes);
    assert_eq!(header.isa_start_offset + expected_isa_bytes, file_len);
}

#[test]
fn test_sampled_sa_matches_reference() {
    let text = b"mississippi\0";
    let sample_rate = 4;
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).expect("Build failed");

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
    let reader = PagedReader::new(tmp_file.path(), 999, cache).unwrap();

    let sa_len = sample_count(text.len(), sample_rate);
    let sa = PagedSampledSA::new(reader, sa_len, header.sa_start_offset);

    let (_, full_sa) = div_sort(text).into_parts();
    let expected: Vec<u64> = full_sa
        .iter()
        .step_by(sample_rate as usize)
        .map(|&v| v as u64)
        .collect();

    assert_eq!(expected.len(), sa_len);
    for (i, &exp) in expected.iter().enumerate() {
        assert_eq!(sa.get(i).unwrap(), exp);
    }
}

#[test]
fn test_wavelet_rank_matches_naive_random() {
    let mut rng = StdRng::seed_from_u64(12345);
    let len = 1000;
    let mut text = Vec::with_capacity(len);
    for _ in 0..len {
        text.push(rng.random_range(0..=15));
    }

    let sample_rate = 5;
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(&text, tmp_file.path()).expect("Build failed");

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 2));
    let reader = PagedReader::new(tmp_file.path(), 4242, cache).unwrap();
    let wt = PagedWaveletTree::new(
        reader,
        header.tree_shape,
        header.codes,
        header.text_len as usize,
        header.wt_start_offset,
    );

    let bwt = build_bwt(&text);
    for _ in 0..200 {
        let sym = rng.random_range(0..=15);
        let idx = rng.random_range(0..=len);
        let expected = bwt[..idx].iter().filter(|&&c| c == sym).count();
        let actual = wt.rank(sym, idx).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_full_ingestion_pipeline() {
    // 1. Prepare Data (with sentinel)
    let text = b"mississippi\0";

    // 2. Run Builder
    let builder = ShardBuilder::new(4); // Sample rate = 4
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).expect("Build failed");

    // 3. Parse Header
    let header = decode_header_from_path(tmp_file.path());
    assert_eq!(header.text_len, text.len() as u64);
    assert_eq!(header.sa_sample_rate, 4);

    // 4. Initialize Paged Structures
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1)); // 10MB cache
    let reader = PagedReader::new(tmp_file.path(), 999, cache).unwrap();

    // 4a. Sampled SA
    let sa = PagedSampledSA::new(reader.clone(), sample_count(text.len(), 4), header.sa_start_offset);

    let val0 = sa.get(0).unwrap(); // Index 0 in stored array (real index 0)
    assert_eq!(val0, 11); // sentinel position

    // 4b. Wavelet Tree
    let wt = PagedWaveletTree::new(
        reader,
        header.tree_shape,
        header.codes,
        text.len(),
        header.wt_start_offset,
    );

    let rank_i = wt.rank(b'i', 12).unwrap();
    assert_eq!(rank_i, 4);

    let rank_s = wt.rank(b's', 12).unwrap();
    assert_eq!(rank_s, 4);

    let rank_m = wt.rank(b'm', 12).unwrap();
    assert_eq!(rank_m, 1);
}
