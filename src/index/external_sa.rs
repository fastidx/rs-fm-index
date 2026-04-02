use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::Path;

use crate::index::scratch;
use tempfile::NamedTempFile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Tuple {
    r1: u64,
    r2: u64,
    idx: u64,
}

impl Ord for Tuple {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.r1, self.r2, self.idx).cmp(&(other.r1, other.r2, other.idx))
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeapItem {
    tuple: Tuple,
    run_idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tuple.cmp(&other.tuple)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct SaStream {
    file: NamedTempFile,
    len: usize,
}

impl SaStream {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> io::Result<SaFileIter> {
        let file = self.file.reopen()?;
        Ok(SaFileIter::new(file, self.len))
    }
}

pub struct SaFileIter {
    reader: BufReader<File>,
    remaining: usize,
}

impl SaFileIter {
    fn new(file: File, len: usize) -> Self {
        Self {
            reader: BufReader::new(file),
            remaining: len,
        }
    }
}

impl Iterator for SaFileIter {
    type Item = io::Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut buf = [0u8; 8];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => {
                self.remaining -= 1;
                Some(Ok(u64::from_le_bytes(buf)))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

pub fn build_sa_external(text: &[u16], mem_limit_bytes: usize) -> io::Result<SaStream> {
    build_sa_external_with_scratch(text, mem_limit_bytes, None)
}

pub fn build_sa_external_with_scratch(
    text: &[u16],
    mem_limit_bytes: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<SaStream> {
    if text.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "text must be non-empty",
        ));
    }

    if text.len() <= u32::MAX as usize {
        let mut rank = Vec::with_capacity(text.len());
        for &b in text {
            rank.push(b as u32 + 1);
        }
        build_sa_external_from_rank_u32(rank, mem_limit_bytes, scratch_dir)
    } else {
        let mut rank = Vec::with_capacity(text.len());
        for &b in text {
            rank.push(b as u64 + 1);
        }
        build_sa_external_from_rank_u64(rank, mem_limit_bytes, scratch_dir)
    }
}

/// Build a SA stream from text-mode bytes without allocating an intermediate encoded u16 buffer.
/// The trailing sentinel is virtualized here as symbol 0.
pub fn build_sa_external_text_bytes_with_scratch(
    text: &[u8],
    mem_limit_bytes: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<SaStream> {
    if text.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "text must be non-empty",
        ));
    }
    if text.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input contains 0 byte; cannot use 0 as sentinel",
        ));
    }

    let n = text.len().saturating_add(1);
    if n <= u32::MAX as usize {
        // External SA rank domain stores rank-0 sentinel as value 1 and data bytes as 2..=256.
        let mut rank = Vec::with_capacity(n);
        for &b in text {
            rank.push(b as u32 + 1);
        }
        rank.push(1);
        build_sa_external_from_rank_u32(rank, mem_limit_bytes, scratch_dir)
    } else {
        let mut rank = Vec::with_capacity(n);
        for &b in text {
            rank.push(b as u64 + 1);
        }
        rank.push(1);
        build_sa_external_from_rank_u64(rank, mem_limit_bytes, scratch_dir)
    }
}

fn build_sa_external_from_rank_u32(
    mut rank: Vec<u32>,
    mem_limit_bytes: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<SaStream> {
    let n = rank.len();
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "text must be non-empty",
        ));
    }

    let chunk_len = chunk_len_from_mem_limit(mem_limit_bytes);

    let mut next_rank = vec![0u32; n];
    let mut step = 1usize;

    loop {
        let runs = build_runs_u32(&rank, step, chunk_len, scratch_dir)?;
        let (rank_count, sa_file) = merge_runs_u32(runs, &mut next_rank, scratch_dir)?;

        if rank_count == n {
            return Ok(SaStream {
                file: sa_file,
                len: n,
            });
        }

        std::mem::swap(&mut rank, &mut next_rank);
        step = step.saturating_mul(2);

        if step >= n {
            // Fallback: return last SA order if we fail to reach unique ranks.
            return Ok(SaStream {
                file: sa_file,
                len: n,
            });
        }
    }
}

fn build_sa_external_from_rank_u64(
    mut rank: Vec<u64>,
    mem_limit_bytes: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<SaStream> {
    let n = rank.len();
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "text must be non-empty",
        ));
    }

    let chunk_len = chunk_len_from_mem_limit(mem_limit_bytes);

    let mut next_rank = vec![0u64; n];
    let mut step = 1usize;

    loop {
        let runs = build_runs_u64(&rank, step, chunk_len, scratch_dir)?;
        let (rank_count, sa_file) = merge_runs_u64(runs, &mut next_rank, scratch_dir)?;

        if rank_count == n {
            return Ok(SaStream {
                file: sa_file,
                len: n,
            });
        }

        std::mem::swap(&mut rank, &mut next_rank);
        step = step.saturating_mul(2);

        if step >= n {
            // Fallback: return last SA order if we fail to reach unique ranks.
            return Ok(SaStream {
                file: sa_file,
                len: n,
            });
        }
    }
}

fn chunk_len_from_mem_limit(mem_limit_bytes: usize) -> usize {
    let tuple_bytes = size_of::<Tuple>().max(1);
    let mut chunk_len = mem_limit_bytes / tuple_bytes;
    if chunk_len == 0 {
        chunk_len = 1;
    }
    chunk_len
}

fn build_runs_u32(
    rank: &[u32],
    step: usize,
    chunk_len: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<Vec<NamedTempFile>> {
    let n = rank.len();
    let mut runs = Vec::new();
    let mut start = 0usize;

    while start < n {
        let end = (start + chunk_len).min(n);
        let mut tuples = Vec::with_capacity(end - start);

        for i in start..end {
            let r1 = rank[i] as u64;
            let r2 = if i + step < n {
                rank[i + step] as u64
            } else {
                0
            };
            tuples.push(Tuple {
                r1,
                r2,
                idx: i as u64,
            });
        }

        tuples.sort_unstable();
        let run_file = write_run(&tuples, scratch_dir)?;
        runs.push(run_file);

        start = end;
    }

    Ok(runs)
}

fn build_runs_u64(
    rank: &[u64],
    step: usize,
    chunk_len: usize,
    scratch_dir: Option<&Path>,
) -> io::Result<Vec<NamedTempFile>> {
    let n = rank.len();
    let mut runs = Vec::new();
    let mut start = 0usize;

    while start < n {
        let end = (start + chunk_len).min(n);
        let mut tuples = Vec::with_capacity(end - start);

        for i in start..end {
            let r1 = rank[i];
            let r2 = if i + step < n { rank[i + step] } else { 0 };
            tuples.push(Tuple {
                r1,
                r2,
                idx: i as u64,
            });
        }

        tuples.sort_unstable();
        let run_file = write_run(&tuples, scratch_dir)?;
        runs.push(run_file);

        start = end;
    }

    Ok(runs)
}

fn write_run(tuples: &[Tuple], scratch_dir: Option<&Path>) -> io::Result<NamedTempFile> {
    let mut file = scratch::named_temp_file(scratch_dir)?;
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        for t in tuples {
            writer.write_all(&t.r1.to_le_bytes())?;
            writer.write_all(&t.r2.to_le_bytes())?;
            writer.write_all(&t.idx.to_le_bytes())?;
        }
        writer.flush()?;
    }
    Ok(file)
}

struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn new(file: &NamedTempFile) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(file.reopen()?),
        })
    }

    fn next_tuple(&mut self) -> io::Result<Option<Tuple>> {
        let mut buf = [0u8; 24];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => {
                let r1 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let r2 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let idx = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                Ok(Some(Tuple { r1, r2, idx }))
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err),
        }
    }
}

fn merge_runs_u32(
    runs: Vec<NamedTempFile>,
    new_rank: &mut [u32],
    scratch_dir: Option<&Path>,
) -> io::Result<(usize, NamedTempFile)> {
    let mut readers: Vec<RunReader> = Vec::with_capacity(runs.len());
    for run in &runs {
        readers.push(RunReader::new(run)?);
    }

    let mut heap: BinaryHeap<std::cmp::Reverse<HeapItem>> = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(tuple) = reader.next_tuple()? {
            heap.push(std::cmp::Reverse(HeapItem { tuple, run_idx }));
        }
    }

    let mut rank_count = 0usize;
    let mut prev_key: Option<(u64, u64)> = None;

    let mut sa_file = scratch::named_temp_file(scratch_dir)?;
    {
        let mut sa_writer = BufWriter::new(sa_file.as_file_mut());
        while let Some(std::cmp::Reverse(item)) = heap.pop() {
            let key = (item.tuple.r1, item.tuple.r2);
            if prev_key != Some(key) {
                rank_count += 1;
                prev_key = Some(key);
            }

            let idx = item.tuple.idx as usize;
            if idx < new_rank.len() {
                new_rank[idx] = rank_count as u32;
            }

            sa_writer.write_all(&item.tuple.idx.to_le_bytes())?;

            if let Some(next_tuple) = readers[item.run_idx].next_tuple()? {
                heap.push(std::cmp::Reverse(HeapItem {
                    tuple: next_tuple,
                    run_idx: item.run_idx,
                }));
            }
        }
        sa_writer.flush()?;
    }

    Ok((rank_count, sa_file))
}

fn merge_runs_u64(
    runs: Vec<NamedTempFile>,
    new_rank: &mut [u64],
    scratch_dir: Option<&Path>,
) -> io::Result<(usize, NamedTempFile)> {
    let mut readers: Vec<RunReader> = Vec::with_capacity(runs.len());
    for run in &runs {
        readers.push(RunReader::new(run)?);
    }

    let mut heap: BinaryHeap<std::cmp::Reverse<HeapItem>> = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(tuple) = reader.next_tuple()? {
            heap.push(std::cmp::Reverse(HeapItem { tuple, run_idx }));
        }
    }

    let mut rank_count = 0usize;
    let mut prev_key: Option<(u64, u64)> = None;

    let mut sa_file = scratch::named_temp_file(scratch_dir)?;
    {
        let mut sa_writer = BufWriter::new(sa_file.as_file_mut());
        while let Some(std::cmp::Reverse(item)) = heap.pop() {
            let key = (item.tuple.r1, item.tuple.r2);
            if prev_key != Some(key) {
                rank_count += 1;
                prev_key = Some(key);
            }

            let idx = item.tuple.idx as usize;
            if idx < new_rank.len() {
                new_rank[idx] = rank_count as u64;
            }

            sa_writer.write_all(&item.tuple.idx.to_le_bytes())?;

            if let Some(next_tuple) = readers[item.run_idx].next_tuple()? {
                heap.push(std::cmp::Reverse(HeapItem {
                    tuple: next_tuple,
                    run_idx: item.run_idx,
                }));
            }
        }
        sa_writer.flush()?;
    }

    Ok((rank_count, sa_file))
}
