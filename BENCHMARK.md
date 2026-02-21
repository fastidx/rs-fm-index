# Benchmarks

Last updated: 2026-02-21

This file captures the most recent paged reader benchmarks after adding configurable page size and prefetch mode (sync/async).

## Setup

- Benchmark: `cargo bench --bench paged_reader`
- File size: 64 MiB
- Read length: 64 bytes
- Sequential stride: 4096 bytes
- Samples: 120

Configurations:

- `4k_no_prefetch`: page_size=4KiB, prefetch_pages=0, prefetch_mode=None
- `4k_prefetch_4`: page_size=4KiB, prefetch_pages=4, prefetch_mode=Sync
- `4k_prefetch_4_async`: page_size=4KiB, prefetch_pages=4, prefetch_mode=Async
- `64k_prefetch_2`: page_size=64KiB, prefetch_pages=2, prefetch_mode=Sync
- `64k_prefetch_2_async`: page_size=64KiB, prefetch_pages=2, prefetch_mode=Async

## Results (Sequential Small Reads)

| Config | Time (ms) | Throughput (MiB/s) |
| --- | --- | --- |
| 4k_no_prefetch | 33.611 .. 34.710 | 28.810 .. 29.752 |
| 4k_prefetch_4 | 25.960 .. 27.372 | 36.534 .. 38.521 |
| 4k_prefetch_4_async | 59.040 .. 62.845 | 15.912 .. 16.938 |
| 64k_prefetch_2 | 14.588 .. 14.848 | 67.351 .. 68.549 |
| 64k_prefetch_2_async | 18.585 .. 19.424 | 51.482 .. 53.808 |

## Results (Random Small Reads)

| Config | Time (ms) | Throughput (MiB/s) |
| --- | --- | --- |
| 4k_no_prefetch | 27.981 .. 28.929 | 34.567 .. 35.738 |
| 4k_prefetch_4 | 43.770 .. 45.207 | 22.121 .. 22.847 |
| 4k_prefetch_4_async | 34.329 .. 36.328 | 27.527 .. 29.130 |
| 64k_prefetch_2 | 22.729 .. 23.234 | 43.040 .. 43.997 |
| 64k_prefetch_2_async | 16.465 .. 17.821 | 56.114 .. 60.734 |

## Notes

- Sync prefetch with larger pages performs best for sequential access.
- Async prefetch can help random access but was worse for sequential in this run.
- Criterion warned that 120 samples exceeded the 5s default target time for some configs; results are still usable but can be rerun with a longer target if needed.
