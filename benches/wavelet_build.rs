use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rust_fm_index::index::encoding::ALPHABET_SIZE;
use rust_fm_index::index::wavelet::{
    canonical_codes, huffman_lengths, plan_wavelet_stream, write_wavelet_stream_from_bwt,
    WaveletTreeBuilder,
};
use rust_fm_index::DEFAULT_WAVELET_MAX_BYTES;
use std::io::{BufWriter, Cursor, Write};
use tempfile::tempfile;

fn generate_symbols(len: usize, alphabet_max: u16, seed: u64) -> Vec<u16> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(rng.random_range(0..=alphabet_max));
    }
    out
}

fn counts_from_symbols(data: &[u16]) -> [u64; ALPHABET_SIZE] {
    let mut counts = [0u64; ALPHABET_SIZE];
    for &sym in data {
        let idx = sym as usize;
        if idx < ALPHABET_SIZE {
            counts[idx] += 1;
        }
    }
    counts
}

fn u16_to_le_bytes(data: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() * 2);
    for &sym in data {
        out.extend_from_slice(&sym.to_le_bytes());
    }
    out
}

fn bench_wavelet_build(c: &mut Criterion) {
    let sizes = [100_000usize, 500_000, 2_000_000];
    let mut group = c.benchmark_group("wavelet_build");
    group.sample_size(10);

    for &len in &sizes {
        let symbols = generate_symbols(len, 256, 1337 + len as u64);
        let counts = counts_from_symbols(&symbols);
        let lens = huffman_lengths(&counts);
        let codes = canonical_codes(&lens);
        let plan = plan_wavelet_stream(&codes, &counts);
        let bwt_bytes = u16_to_le_bytes(&symbols);

        let total_bits = plan.total_bits();
        let auto_use_in_memory = total_bits <= DEFAULT_WAVELET_MAX_BYTES;
        let auto_small_bytes = 1 * 1024 * 1024;
        let auto_small_use_in_memory = total_bits <= auto_small_bytes;

        group.bench_function(format!("streaming/{}", len), |b| {
            b.iter_batched(
                || tempfile().expect("tempfile"),
                |file| {
                    let reader = Cursor::new(&bwt_bytes);
                    let mut writer = BufWriter::new(file);
                    write_wavelet_stream_from_bwt(reader, &codes, &plan, &mut writer).unwrap();
                    writer.flush().unwrap();
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("in_memory/{}", len), |b| {
            b.iter_batched(
                || tempfile().expect("tempfile"),
                |mut file| {
                    let mut builder = WaveletTreeBuilder::new(&symbols);
                    builder.process_symbols(&symbols);
                    let _ = builder.write_to_file(&mut file).unwrap();
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("auto_256MiB/{}", len), |b| {
            b.iter_batched(
                || tempfile().expect("tempfile"),
                |file| {
                    if auto_use_in_memory {
                        let mut file = file;
                        let mut builder = WaveletTreeBuilder::new(&symbols);
                        builder.process_symbols(&symbols);
                        let _ = builder.write_to_file(&mut file).unwrap();
                    } else {
                        let reader = Cursor::new(&bwt_bytes);
                        let mut writer = BufWriter::new(file);
                        write_wavelet_stream_from_bwt(reader, &codes, &plan, &mut writer).unwrap();
                        writer.flush().unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("auto_1MiB/{}", len), |b| {
            b.iter_batched(
                || tempfile().expect("tempfile"),
                |file| {
                    if auto_small_use_in_memory {
                        let mut file = file;
                        let mut builder = WaveletTreeBuilder::new(&symbols);
                        builder.process_symbols(&symbols);
                        let _ = builder.write_to_file(&mut file).unwrap();
                    } else {
                        let reader = Cursor::new(&bwt_bytes);
                        let mut writer = BufWriter::new(file);
                        write_wavelet_stream_from_bwt(reader, &codes, &plan, &mut writer).unwrap();
                        writer.flush().unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_wavelet_build);
criterion_main!(benches);
