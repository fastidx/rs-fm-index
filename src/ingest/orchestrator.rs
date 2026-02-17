use crate::api::IndexStats;
use crate::index::builder::ShardBuilder;
use crate::IndexReader;
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use glob::glob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::thread;

pub struct IngestConfig {
    pub input_patterns: Vec<String>,
    pub output_dir: PathBuf,
    pub chunk_size: usize,
    pub read_buffer: usize,
    pub num_workers: usize,
    pub sample_rate: u32,
}

struct ShardJob {
    id: usize,
    data: Vec<u8>,
    doc_offsets: Vec<u64>,
    segments: Vec<DocumentSegmentMeta>,
}

pub struct Orchestrator {
    config: IngestConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSegmentMeta {
    pub doc_id: u64,
    pub part_index: u32,
    pub is_first: bool,
    pub is_last: bool,
    pub source_path: String,
    pub doc_offset: u64,
    pub len: u64,
    pub shard_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMeta {
    pub shard_id: usize,
    pub index_path: PathBuf,
    pub shard_bytes: u64,
    pub doc_offsets: Vec<u64>,
    pub segments: Vec<DocumentSegmentMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardReport {
    pub shard_id: usize,
    pub index_path: PathBuf,
    pub stats_path: PathBuf,
    pub meta_path: PathBuf,
    pub stats: IndexStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestReport {
    pub output_dir: PathBuf,
    pub total_input_bytes: u64,
    pub total_index_bytes: u64,
    pub total_docs: u64,
    pub total_segments: u64,
    pub total_shards: u64,
    pub elapsed_secs: f64,
    pub throughput_bytes_per_sec: f64,
    pub shard_reports: Vec<ShardReport>,
}

impl Orchestrator {
    pub fn new(config: IngestConfig) -> Self {
        Self { config }
    }

    pub fn run(&self) -> Result<()> {
        if self.config.input_patterns.is_empty() {
            anyhow::bail!("At least one input pattern is required");
        }
        if self.config.chunk_size == 0 {
            anyhow::bail!("chunk_size must be > 0");
        }
        if self.config.chunk_size < 2 {
            anyhow::bail!("chunk_size must be at least 2 to allow sentinel");
        }
        if self.config.read_buffer == 0 {
            anyhow::bail!("read_buffer must be > 0");
        }
        if self.config.num_workers == 0 {
            anyhow::bail!("num_workers must be > 0");
        }
        if self.config.sample_rate == 0 {
            anyhow::bail!("sample_rate must be > 0");
        }

        std::fs::create_dir_all(&self.config.output_dir)?;

        let m = MultiProgress::new();
        let pb_main = m.add(ProgressBar::new_spinner());
        pb_main.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner} {msg}")
                .unwrap(),
        );
        pb_main.set_message("Scanning files...");

        let (tx, rx) = bounded::<ShardJob>(self.config.num_workers * 2);
        let (report_tx, report_rx) = bounded::<ShardReport>(self.config.num_workers * 2);
        let workers = self.spawn_workers(rx, report_tx, &m);

        let start = std::time::Instant::now();
        let produce = self.produce_chunks(tx.clone(), &pb_main);
        drop(tx);

        pb_main.finish_with_message("Reading complete. Waiting for workers...");

        let report_handle = thread::spawn(move || report_rx.iter().collect::<Vec<_>>());
        for w in workers {
            w.join().expect("Worker thread panicked");
        }
        let reports = report_handle.join().unwrap();

        let elapsed = start.elapsed().as_secs_f64();
        let report = self.build_report(&reports, produce.as_ref().ok(), elapsed);
        self.write_report(&report)?;

        produce.map(|_| ())
    }

    fn spawn_workers(
        &self,
        rx: Receiver<ShardJob>,
        report_tx: Sender<ShardReport>,
        m: &MultiProgress,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::new();
        let output_dir = self.config.output_dir.clone();
        let sample_rate = self.config.sample_rate;

        for id in 0..self.config.num_workers {
            let rx = rx.clone();
            let output_dir = output_dir.clone();
            let pb = m.add(ProgressBar::new(0));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(&format!("[Worker {id}] {{bar:40}} {{msg}}"))
                    .unwrap(),
            );

            let report_tx = report_tx.clone();
            handles.push(thread::spawn(move || {
                while let Ok(job) = rx.recv() {
                    let shard_path = output_dir.join(format!("shard_{:05}.idx", job.id));
                    let size_mb = job.data.len() / 1024 / 1024;

                    pb.set_length(1);
                    pb.set_message(format!("Building shard {} ({} MB)", job.id, size_mb));

                    let builder = ShardBuilder::new(sample_rate);
                    if let Err(e) =
                        builder.build_with_offsets(&job.data, job.doc_offsets.clone(), &shard_path)
                    {
                        eprintln!("Error building shard {}: {:?}", job.id, e);
                        pb.set_message("Error");
                        continue;
                    }

                    let stats = match IndexReader::open(&shard_path) {
                        Ok(reader) => match reader.stats() {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("Error reading stats for shard {}: {:?}", job.id, e);
                                pb.set_message("Stats error");
                                continue;
                            }
                        },
                        Err(e) => {
                            eprintln!("Error opening shard {}: {:?}", job.id, e);
                            pb.set_message("Open error");
                            continue;
                        }
                    };

                    let meta_path = shard_path.with_extension("meta.json");
                    let stats_path = shard_path.with_extension("stats.json");

                    let meta = ShardMeta {
                        shard_id: job.id,
                        index_path: shard_path.clone(),
                        shard_bytes: job.data.len() as u64,
                        doc_offsets: job.doc_offsets,
                        segments: job.segments,
                    };
                    if let Err(e) = write_json(&meta_path, &meta) {
                        eprintln!("Error writing meta for shard {}: {:?}", job.id, e);
                    }
                    if let Err(e) = write_json(&stats_path, &stats) {
                        eprintln!("Error writing stats for shard {}: {:?}", job.id, e);
                    }

                    let report = ShardReport {
                        shard_id: job.id,
                        index_path: shard_path,
                        stats_path,
                        meta_path,
                        stats,
                    };
                    let _ = report_tx.send(report);

                    pb.inc(1);
                    pb.set_message("Idle");
                }
                pb.finish_with_message("Done");
            }));
        }
        handles
    }

    fn produce_chunks(&self, tx: Sender<ShardJob>, pb: &ProgressBar) -> Result<ProduceStats> {
        let mut current_shard_id = 0;

        let mut buffer: Vec<u8> = Vec::with_capacity(self.config.chunk_size);
        let mut doc_offsets: Vec<u64> = Vec::new();
        let mut segments: Vec<DocumentSegmentMeta> = Vec::new();

        let mut total_input_bytes = 0u64;
        let mut total_docs = 0u64;
        let mut total_segments = 0u64;

        let mut paths = self.collect_paths()?;
        paths.sort();

        for path in paths {
            pb.set_message(format!("Processing {:?}", path.file_name().unwrap_or_default()));
            let file_size = std::fs::metadata(&path)?.len() as u64;
            total_input_bytes += file_size;

            let doc_id = total_docs;
            total_docs += 1;

            let mut part_index: u32 = 0;
            let mut doc_offset: u64 = 0;
            let mut remaining = file_size;

            let mut file = File::open(&path)?;
            while remaining > 0 {
                let available = self
                    .config
                    .chunk_size
                    .saturating_sub(buffer.len())
                    .saturating_sub(1);

                if available == 0 {
                    self.flush_chunk(
                        &tx,
                        &mut buffer,
                        &mut doc_offsets,
                        &mut segments,
                        &mut current_shard_id,
                    )?;
                    continue;
                }

                if remaining as usize > available && !buffer.is_empty() {
                    self.flush_chunk(
                        &tx,
                        &mut buffer,
                        &mut doc_offsets,
                        &mut segments,
                        &mut current_shard_id,
                    )?;
                    continue;
                }

                let take = std::cmp::min(remaining as usize, available) as u64;
                let segment_start = buffer.len() as u64;

                self.read_into_buffer(&mut file, take, &mut buffer, self.config.read_buffer, &path)?;

                buffer.push(0);
                doc_offsets.push(segment_start);

                let is_first = part_index == 0;
                let is_last = take == remaining;
                segments.push(DocumentSegmentMeta {
                    doc_id,
                    part_index,
                    is_first,
                    is_last,
                    source_path: path.to_string_lossy().to_string(),
                    doc_offset,
                    len: take,
                    shard_offset: segment_start,
                });
                total_segments += 1;

                remaining -= take;
                doc_offset += take;
                part_index += 1;
            }
        }

        self.flush_chunk(
            &tx,
            &mut buffer,
            &mut doc_offsets,
            &mut segments,
            &mut current_shard_id,
        )?;

        Ok(ProduceStats {
            total_input_bytes,
            total_docs,
            total_segments,
            total_shards: current_shard_id as u64,
        })
    }

    fn flush_chunk(
        &self,
        tx: &Sender<ShardJob>,
        buffer: &mut Vec<u8>,
        doc_offsets: &mut Vec<u64>,
        segments: &mut Vec<DocumentSegmentMeta>,
        shard_id: &mut usize,
    ) -> Result<()> {
        if buffer.is_empty() {
            doc_offsets.clear();
            segments.clear();
            return Ok(());
        }

        let job = ShardJob {
            id: *shard_id,
            data: std::mem::take(buffer),
            doc_offsets: std::mem::take(doc_offsets),
            segments: std::mem::take(segments),
        };

        tx.send(job).context("Worker threads died")?;
        *shard_id += 1;
        Ok(())
    }

    fn collect_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for pattern in &self.config.input_patterns {
            let entries = glob(pattern).with_context(|| format!("Invalid glob pattern: {}", pattern))?;
            for entry in entries {
                match entry {
                    Ok(path) if path.is_file() => paths.push(path),
                    Ok(_) => {}
                    Err(e) => eprintln!("Glob error: {:?}", e),
                }
            }
        }
        Ok(paths)
    }

    fn read_into_buffer(
        &self,
        file: &mut File,
        mut bytes_to_read: u64,
        buffer: &mut Vec<u8>,
        read_buffer: usize,
        path: &Path,
    ) -> Result<()> {
        let mut temp = vec![0u8; read_buffer];
        while bytes_to_read > 0 {
            let want = std::cmp::min(bytes_to_read as usize, temp.len());
            let n = file.read(&mut temp[..want])?;
            if n == 0 {
                break;
            }
            if temp[..n].contains(&0) {
                anyhow::bail!(
                    "Input file {:?} contains 0 byte; sentinel conflicts with indexing rules",
                    path
                );
            }
            buffer.extend_from_slice(&temp[..n]);
            bytes_to_read -= n as u64;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ProduceStats {
    total_input_bytes: u64,
    total_docs: u64,
    total_segments: u64,
    total_shards: u64,
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}

impl Orchestrator {
    fn build_report(
        &self,
        shard_reports: &[ShardReport],
        produce: Option<&ProduceStats>,
        elapsed_secs: f64,
    ) -> IngestReport {
        let mut total_input_bytes = 0u64;
        let mut total_docs = 0u64;
        let mut total_segments = 0u64;
        let mut total_shards = shard_reports.len() as u64;
        if let Some(p) = produce {
            total_input_bytes = p.total_input_bytes;
            total_docs = p.total_docs;
            total_segments = p.total_segments;
            total_shards = p.total_shards;
        }

        let total_index_bytes: u64 = shard_reports.iter().map(|r| r.stats.index_bytes).sum();
        let throughput = if elapsed_secs > 0.0 {
            total_input_bytes as f64 / elapsed_secs
        } else {
            0.0
        };

        IngestReport {
            output_dir: self.config.output_dir.clone(),
            total_input_bytes,
            total_index_bytes,
            total_docs,
            total_segments,
            total_shards,
            elapsed_secs,
            throughput_bytes_per_sec: throughput,
            shard_reports: shard_reports.to_vec(),
        }
    }

    fn write_report(&self, report: &IngestReport) -> Result<()> {
        let report_path = self.config.output_dir.join("ingest_report.json");
        write_json(&report_path, report)?;
        println!("Ingest report written to {:?}", report_path);
        println!(
            "Ingested {} bytes in {:.2}s ({:.2} MB/s), shards={}, docs={}, segments={}",
            report.total_input_bytes,
            report.elapsed_secs,
            report.throughput_bytes_per_sec / (1024.0 * 1024.0),
            report.total_shards,
            report.total_docs,
            report.total_segments
        );
        println!(
            "Total index bytes: {} ({:.2} MiB)",
            report.total_index_bytes,
            report.total_index_bytes as f64 / (1024.0 * 1024.0)
        );
        Ok(())
    }
}
