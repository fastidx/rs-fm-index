use crate::index::builder::ShardBuilder;
use crate::index::encoding::{EncodingMode, ALPHABET_SIZE};
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

fn build_bwt(text: &[u8]) -> Vec<u16> {
    let (_, sa) = div_sort(text).into_parts();
    let len = text.len();
    let mut bwt = Vec::with_capacity(len);
    for &sa_val in &sa {
        let pos = sa_val as usize;
        if pos == 0 {
            bwt.push(text[len - 1] as u16);
        } else {
            bwt.push(text[pos - 1] as u16);
        }
    }
    bwt
}

#[test]
fn test_header_roundtrip() {
    let mut c_table = [0u64; ALPHABET_SIZE];
    for i in 0..ALPHABET_SIZE {
        c_table[i] = (i as u64) * 7;
    }

    let mut codes: [Option<HuffmanCode>; ALPHABET_SIZE] = [None; ALPHABET_SIZE];
    codes[0] = Some(HuffmanCode { bits: 0b1, len: 1 });
    codes[ALPHABET_SIZE - 1] = Some(HuffmanCode { bits: 0b10, len: 2 });

    let tree_shape = vec![
        WaveletNodeShape::Internal {
            left_idx: 1,
            right_idx: 2,
            bit_start: 123,
            bit_len: 456,
        },
        WaveletNodeShape::Leaf { symbol: 0 },
        WaveletNodeShape::Leaf {
            symbol: (ALPHABET_SIZE - 1) as u16,
        },
    ];

    let doc_offsets = vec![0u64, 5, 10, 20, 21];
    let mut header = ShardHeader::new(crate::index::header::ShardHeaderParams {
        encoding_mode: EncodingMode::Text,
        text_len: 101,
        sa_sample_rate: 4,
        isa_sample_rate: 4,
        sa_bits: 0,
        isa_bits: 0,
        c_table,
        codes,
        tree_shape: tree_shape.clone(),
        doc_offsets: doc_offsets.clone(),
    });
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
    assert_eq!(decoded.encoding_mode, EncodingMode::Text);
    assert_eq!(decoded.tree_shape.len(), tree_shape.len());
    assert_eq!(decoded.c_table[10], c_table[10]);
    assert_eq!(decoded.codes[0], codes[0]);
    assert_eq!(decoded.codes[ALPHABET_SIZE - 1], codes[ALPHABET_SIZE - 1]);
    let decoded_offsets = decoded.decode_doc_offsets().unwrap();
    assert_eq!(decoded_offsets, doc_offsets);
}

#[test]
fn test_builder_offsets_and_lengths() {
    let text = b"banana";
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
    let sa_bits = if header.sa_bits == 0 { 64 } else { header.sa_bits as u64 };
    let isa_bits = if header.isa_bits == 0 { 64 } else { header.isa_bits as u64 };
    let sa_word_bits = if header.sa_bits == 0 {
        64
    } else if header.sa_bits <= 32 {
        32
    } else {
        64
    } as u64;
    let isa_word_bits = if header.isa_bits == 0 {
        64
    } else if header.isa_bits <= 32 {
        32
    } else {
        64
    } as u64;
    let indexed_len = text.len() + 1;
    let sa_words =
        ((sample_count(indexed_len, sample_rate) as u64 * sa_bits) + sa_word_bits - 1)
            / sa_word_bits;
    let isa_words =
        ((sample_count(indexed_len, header.isa_sample_rate) as u64 * isa_bits) + isa_word_bits - 1)
            / isa_word_bits;
    let expected_sa_bytes = sa_words * (sa_word_bits / 8);
    let expected_isa_bytes = isa_words * (isa_word_bits / 8);
    assert_eq!(header.isa_start_offset, header.sa_start_offset + expected_sa_bytes);
    assert_eq!(header.isa_start_offset + expected_isa_bytes, file_len);
}

#[test]
fn test_sampled_sa_matches_reference() {
    let text = b"mississippi";
    let sample_rate = 4;
    let builder = ShardBuilder::new(sample_rate);
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).expect("Build failed");

    let header = decode_header_from_path(tmp_file.path());
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
    let reader = PagedReader::new(tmp_file.path(), 999, cache).unwrap();

    let indexed = {
        let mut data = Vec::from(text.as_slice());
        data.push(0);
        data
    };
    let sa_len = sample_count(indexed.len(), sample_rate);
    let sa = PagedSampledSA::new(reader, sa_len, header.sa_start_offset, header.sa_bits);

    let (_, full_sa) = div_sort(&indexed).into_parts();
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
    let mut text = Vec::with_capacity(len + 1);
    for _ in 0..len {
        text.push(rng.random_range(1..=15));
    }
    let indexed = {
        let mut data = text.clone();
        data.push(0);
        data
    };

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

    let bwt = build_bwt(&indexed);
    for _ in 0..200 {
        let sym = rng.random_range(0..=15) as u16;
        let idx = rng.random_range(0..=indexed.len());
        let expected = bwt[..idx].iter().filter(|&&c| c == sym).count();
        let actual = wt.rank(sym, idx).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_full_ingestion_pipeline() {
    // 1. Prepare Data (sentinel appended during build)
    let text = b"mississippi";

    // 2. Run Builder
    let builder = ShardBuilder::new(4); // Sample rate = 4
    let tmp_file = NamedTempFile::new().unwrap();
    builder.build(text, tmp_file.path()).expect("Build failed");

    // 3. Parse Header
    let header = decode_header_from_path(tmp_file.path());
    assert_eq!(header.text_len, (text.len() + 1) as u64);
    assert_eq!(header.sa_sample_rate, 4);

    // 4. Initialize Paged Structures
    let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1)); // 10MB cache
    let reader = PagedReader::new(tmp_file.path(), 999, cache).unwrap();

    // 4a. Sampled SA
    let sa = PagedSampledSA::new(
        reader.clone(),
        sample_count(text.len() + 1, 4),
        header.sa_start_offset,
        header.sa_bits,
    );

    let val0 = sa.get(0).unwrap(); // Index 0 in stored array (real index 0)
    assert_eq!(val0, 11); // sentinel position

    // 4b. Wavelet Tree
    let wt = PagedWaveletTree::new(
        reader,
        header.tree_shape,
        header.codes,
        header.text_len as usize,
        header.wt_start_offset,
    );

    let rank_i = wt.rank(b'i' as u16, 12).unwrap();
    assert_eq!(rank_i, 4);

    let rank_s = wt.rank(b's' as u16, 12).unwrap();
    assert_eq!(rank_s, 4);

    let rank_m = wt.rank(b'm' as u16, 12).unwrap();
    assert_eq!(rank_m, 1);
}

#[test]
fn test_orchestrator_chunking() {
    use crate::ingest::orchestrator::{IngestConfig, Orchestrator};
    use std::io::Write;
    use tempfile::TempDir;

    let input_dir = TempDir::new().unwrap();
    for i in 0..3 {
        let p = input_dir.path().join(format!("doc{}.txt", i));
        let mut f = std::fs::File::create(p).unwrap();
        let content = vec![b'a' + i as u8; 100];
        f.write_all(&content).unwrap();
    }

    let output_dir = TempDir::new().unwrap();

    let config = IngestConfig {
        input_patterns: vec![input_dir.path().join("*.txt").to_string_lossy().to_string()],
        output_dir: output_dir.path().to_path_buf(),
        chunk_size: 150,
        read_buffer: 64,
        num_workers: 2,
        sample_rate: 4,
        encoding_mode: EncodingMode::Text,
        wavelet_mode: crate::index::wavelet::WaveletBuildMode::default(),
    };

    let orch = Orchestrator::new(config);
    orch.run().expect("Orchestrator failed");

    let pattern = output_dir
        .path()
        .join("shard_*.idx")
        .to_string_lossy()
        .to_string();
    let shards: Vec<_> = glob::glob(&pattern).unwrap().map(|x| x.unwrap()).collect();

    assert!(shards.len() >= 2, "Expected multiple shards, got {}", shards.len());

    let stats_pattern = output_dir
        .path()
        .join("shard_*.stats.json")
        .to_string_lossy()
        .to_string();
    let stats_files: Vec<_> = glob::glob(&stats_pattern)
        .unwrap()
        .map(|x| x.unwrap())
        .collect();
    assert_eq!(stats_files.len(), shards.len());

    let report_path = output_dir.path().join("ingest_report.json");
    assert!(report_path.exists());
}

#[test]
fn test_orchestrator_oversized_split_metadata() {
    use crate::ingest::orchestrator::{IngestConfig, Orchestrator, ShardMeta};
    use std::io::Write;
    use tempfile::TempDir;

    let input_dir = TempDir::new().unwrap();
    let p = input_dir.path().join("big.txt");
    let mut f = std::fs::File::create(&p).unwrap();
    let content = vec![b'x'; 400];
    f.write_all(&content).unwrap();

    let output_dir = TempDir::new().unwrap();
    let config = IngestConfig {
        input_patterns: vec![input_dir.path().join("*.txt").to_string_lossy().to_string()],
        output_dir: output_dir.path().to_path_buf(),
        chunk_size: 150,
        read_buffer: 64,
        num_workers: 2,
        sample_rate: 4,
        encoding_mode: EncodingMode::Text,
        wavelet_mode: crate::index::wavelet::WaveletBuildMode::default(),
    };

    let orch = Orchestrator::new(config);
    orch.run().expect("Orchestrator failed");

    let meta_paths: Vec<_> = glob::glob(
        &output_dir
            .path()
            .join("shard_*.meta.json")
            .to_string_lossy()
            .to_string(),
    )
    .unwrap()
    .map(|x| x.unwrap())
    .collect();

    assert!(meta_paths.len() >= 2, "Expected multiple meta files");

    let mut all_segments = Vec::new();
    for mp in meta_paths {
        let data = std::fs::read_to_string(mp).unwrap();
        let meta: ShardMeta = serde_json::from_str(&data).unwrap();
        all_segments.extend(meta.segments);
    }

    all_segments.sort_by_key(|s| s.part_index);
    assert!(all_segments.len() >= 2);
    let first = &all_segments[0];
    let last = &all_segments[all_segments.len() - 1];
    assert!(first.is_first);
    assert!(!first.is_last);
    assert!(last.is_last);
    assert_eq!(first.doc_id, last.doc_id);
    assert_eq!(first.part_index, 0);
}

#[test]
fn test_ingest_config_parse() {
    use crate::ingest::config::{size_value_to_usize, IngestConfigFile};
    use tempfile::Builder;

    let toml = r#"
input_patterns = ["data/*.txt"]
output_dir = "out"
chunk_size = "64MiB"
read_buffer = 1048576
num_workers = 8
sample_rate = 64
"#;
    let mut file = Builder::new().suffix(".toml").tempfile().unwrap();
    std::io::Write::write_all(&mut file, toml.as_bytes()).unwrap();
    let cfg = IngestConfigFile::load(file.path()).unwrap();

    assert_eq!(cfg.input_patterns.unwrap()[0], "data/*.txt");
    assert_eq!(cfg.output_dir.unwrap().to_string_lossy(), "out");
    assert_eq!(size_value_to_usize(cfg.chunk_size.as_ref().unwrap()).unwrap(), 64 * 1024 * 1024);
    assert_eq!(size_value_to_usize(cfg.read_buffer.as_ref().unwrap()).unwrap(), 1_048_576);
    assert_eq!(cfg.num_workers.unwrap(), 8);
    assert_eq!(cfg.sample_rate.unwrap(), 64);
}
