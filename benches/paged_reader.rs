use criterion::{
    black_box, BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use rand::rngs::StdRng;
use rand::RngExt;
use rand::SeedableRng;
use rust_fm_index::iolib::paged_reader::{GlobalPageCache, PagedReader, PagedReaderConfig};
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

const FILE_SIZE: usize = 64 * 1024 * 1024;
const READ_LEN: usize = 64;
const STRIDE: usize = 4096;
const CACHE_BYTES: usize = 128 * 1024 * 1024;

fn create_temp_file() -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("tempfile");
    let mut buf = vec![0u8; 1024 * 1024];
    let mut remaining = FILE_SIZE;
    let mut seed = 0u8;
    while remaining > 0 {
        for byte in buf.iter_mut() {
            *byte = seed;
            seed = seed.wrapping_add(1);
        }
        let to_write = std::cmp::min(remaining, buf.len());
        file.write_all(&buf[..to_write]).expect("write temp file");
        remaining -= to_write;
    }
    file.as_file().sync_all().expect("sync temp file");
    file
}

fn offsets() -> Vec<u64> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + READ_LEN <= FILE_SIZE {
        out.push(pos as u64);
        pos += STRIDE;
    }
    out
}

fn bench_paged_reader(c: &mut Criterion) {
    let file = create_temp_file();
    let offsets = offsets();
    let total_bytes = (offsets.len() * READ_LEN) as u64;

    let configs = [
        ("4k_no_prefetch", PagedReaderConfig { page_size: 4 * 1024, prefetch_pages: 0 }),
        ("4k_prefetch_4", PagedReaderConfig { page_size: 4 * 1024, prefetch_pages: 4 }),
        ("64k_prefetch_2", PagedReaderConfig { page_size: 64 * 1024, prefetch_pages: 2 }),
    ];

    let mut group = c.benchmark_group("paged_reader_seq_small");
    group.sample_size(120);
    group.throughput(Throughput::Bytes(total_bytes));

    for (name, config) in configs {
        group.bench_with_input(BenchmarkId::from_parameter(name), &config, |b, cfg| {
            b.iter_batched(
                || {
                    let cache = Arc::new(GlobalPageCache::new(CACHE_BYTES, 16));
                    PagedReader::new_with_config(file.path(), 1, cache, *cfg).unwrap()
                },
                |reader| {
                    for &offset in &offsets {
                        let data = reader.read_at(offset, READ_LEN).unwrap();
                        black_box(data);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    let mut rng = StdRng::seed_from_u64(1234);
    let mut random_offsets = Vec::with_capacity(offsets.len());
    for _ in 0..offsets.len() {
        let max = FILE_SIZE - READ_LEN;
        let pos = rng.random_range(0..=max);
        random_offsets.push(pos as u64);
    }
    let random_total_bytes = (random_offsets.len() * READ_LEN) as u64;

    let mut random_group = c.benchmark_group("paged_reader_random_small");
    random_group.sample_size(120);
    random_group.throughput(Throughput::Bytes(random_total_bytes));

    for (name, config) in configs {
        random_group.bench_with_input(BenchmarkId::from_parameter(name), &config, |b, cfg| {
            b.iter_batched(
                || {
                    let cache = Arc::new(GlobalPageCache::new(CACHE_BYTES, 16));
                    PagedReader::new_with_config(file.path(), 2, cache, *cfg).unwrap()
                },
                |reader| {
                    for &offset in &random_offsets {
                        let data = reader.read_at(offset, READ_LEN).unwrap();
                        black_box(data);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    random_group.finish();
}

criterion_group!(benches, bench_paged_reader);
criterion_main!(benches);
