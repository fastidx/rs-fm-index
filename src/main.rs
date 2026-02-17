use clap::{Args, Parser, Subcommand};
use rust_fm_index::ingest::config::{parse_size, size_value_to_usize, IngestConfigFile};
use rust_fm_index::ingest::orchestrator::{IngestConfig, Orchestrator};
use rust_fm_index::{IndexBuilder, IndexReader, MultiShardReader};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "infigram", version, about = "Infini-gram FM-index CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Build(BuildArgs),
    BuildMulti(BuildMultiArgs),
    Ingest(IngestArgs),
    Query(QueryArgs),
    Stats(StatsArgs),
    Doc(DocArgs),
    Extract(ExtractArgs),
}

#[derive(Args)]
struct BuildArgs {
    input: PathBuf,
    output: PathBuf,
    #[arg(long, default_value_t = 32)]
    sample_rate: u32,
}

#[derive(Args)]
struct BuildMultiArgs {
    output: PathBuf,
    #[arg(num_args = 1..)]
    inputs: Vec<PathBuf>,
    #[arg(long, default_value_t = 32)]
    sample_rate: u32,
}

#[derive(Args)]
struct IngestArgs {
    /// Input file patterns (glob). Can be repeated.
    #[arg(short, long, num_args = 1..)]
    input: Vec<String>,
    /// Output directory for shards
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Optional config file (TOML or JSON)
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Target shard size, e.g. 1GiB, 500MiB, 1024MB, 1000000000
    #[arg(long, value_parser = parse_size)]
    chunk_size: Option<usize>,
    /// Read buffer size per file read
    #[arg(long, value_parser = parse_size)]
    read_buffer: Option<usize>,
    /// Number of worker threads
    #[arg(short, long)]
    workers: Option<usize>,
    /// SA/ISA sample rate
    #[arg(long)]
    sample_rate: Option<u32>,
}

#[derive(Args)]
struct QueryArgs {
    index: PathBuf,
    pattern: String,
}

#[derive(Args)]
struct StatsArgs {
    index: PathBuf,
}

#[derive(Args)]
struct DocArgs {
    index: PathBuf,
    doc_id: usize,
}

#[derive(Args)]
struct ExtractArgs {
    index: PathBuf,
    pos: usize,
    len: usize,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build(args) => run_build(args),
        Commands::BuildMulti(args) => run_build_multi(args),
        Commands::Ingest(args) => run_ingest(args),
        Commands::Query(args) => run_query(args),
        Commands::Stats(args) => run_stats(args),
        Commands::Doc(args) => run_doc(args),
        Commands::Extract(args) => run_extract(args),
    }
}

fn run_build(args: BuildArgs) {
    println!("Building index for {:?} -> {:?}", args.input, args.output);
    let start = Instant::now();

    let data = std::fs::read(&args.input).expect("Failed to read input");
    let builder = IndexBuilder::new(args.sample_rate);
    builder
        .build_single_document(&data, &args.output)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}

fn run_query(args: QueryArgs) {
    if args.index.is_dir() {
        run_query_shards(args);
        return;
    }

    let engine = IndexReader::open(&args.index).expect("Failed to open index");
    let start = Instant::now();

    match engine.count(args.pattern.as_bytes()) {
        Ok((sp, ep)) => {
            if sp > ep {
                println!("Pattern not found.");
            } else {
                let count = ep - sp + 1;
                println!("Found {} occurrences in {:.2?}", count, start.elapsed());

                // Locate first few
                if count > 0 {
                    let locs = engine.locate(args.pattern.as_bytes()).unwrap();
                    let preview = locs.iter().take(5).collect::<Vec<_>>();
                    println!("Locations (first 5): {:?}", preview);
                    for &&pos in preview.iter() {
                        if let Some((doc_id, offset)) = engine.pos_to_doc_id(pos) {
                            println!("  pos {} -> doc {} @ {}", pos, doc_id, offset);
                        }
                    }
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_query_shards(args: QueryArgs) {
    let router = MultiShardReader::open(&args.index).expect("Failed to open shard directory");
    let start = Instant::now();

    match router.count(args.pattern.as_bytes()) {
        Ok(count) => {
            if count == 0 {
                println!("Pattern not found.");
                return;
            }

            println!(
                "Found {} occurrences across {} shards in {:.2?}",
                count,
                router.shard_count(),
                start.elapsed()
            );

            let hits = router.locate(args.pattern.as_bytes()).unwrap_or_default();
            let preview = hits.iter().take(5).collect::<Vec<_>>();
            println!("Locations (first 5): {:?}", preview);
            for hit in preview {
                println!(
                    "  shard {} pos {} -> doc {} @ {}",
                    hit.shard_id, hit.shard_pos, hit.doc_id, hit.doc_offset
                );
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_extract(args: ExtractArgs) {
    let engine = IndexReader::open(&args.index).expect("Failed to open index");

    match engine.extract(args.pos, args.len) {
        Ok(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            println!("Extracted: {:?}", s);
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_stats(args: StatsArgs) {
    let engine = IndexReader::open(&args.index).expect("Failed to open index");
    match engine.stats() {
        Ok(s) => {
            println!("Index stats for {:?}", args.index);
            println!("Text bytes: {} ({})", s.text_bytes, format_bytes(s.text_bytes));
            println!("Index bytes: {} ({})", s.index_bytes, format_bytes(s.index_bytes));
            if s.text_bytes > 0 {
                let ratio = s.index_bytes as f64 / s.text_bytes as f64;
                println!("Index/Text ratio: {:.3}", ratio);
            }
            println!("Header bytes: {} ({})", s.header_bytes, format_bytes(s.header_bytes));
            println!("Wavelet bytes: {} ({})", s.wavelet_bytes, format_bytes(s.wavelet_bytes));
            println!("SA bytes: {} ({})", s.sa_bytes, format_bytes(s.sa_bytes));
            println!("ISA bytes: {} ({})", s.isa_bytes, format_bytes(s.isa_bytes));
            println!("SA sample rate: {}", s.sa_sample_rate);
            println!("ISA sample rate: {}", s.isa_sample_rate);
            println!(
                "SA bits: {}",
                if s.sa_bits == 0 {
                    "u64".to_string()
                } else {
                    format!("{}", s.sa_bits)
                }
            );
            println!(
                "ISA bits: {}",
                if s.isa_bits == 0 {
                    "u64".to_string()
                } else {
                    format!("{}", s.isa_bits)
                }
            );
            println!("SA samples: {}", s.sa_samples);
            println!("ISA samples: {}", s.isa_samples);
            println!("Doc offsets count: {}", s.doc_offsets_count);
            println!(
                "Doc offsets EF bytes: {} ({})",
                s.doc_offsets_u_bits_bytes + s.doc_offsets_l_bits_bytes,
                format_bytes(s.doc_offsets_u_bits_bytes + s.doc_offsets_l_bits_bytes)
            );
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn format_bytes(n: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let v = n as f64;
    if v >= GB {
        format!("{:.2} GiB", v / GB)
    } else if v >= MB {
        format!("{:.2} MiB", v / MB)
    } else if v >= KB {
        format!("{:.2} KiB", v / KB)
    } else {
        format!("{} B", n)
    }
}

fn run_doc(args: DocArgs) {
    if args.index.is_dir() {
        let router = MultiShardReader::open(&args.index).expect("Failed to open shard directory");
        match router.get_document(args.doc_id as u64) {
            Ok(bytes) => {
                let mut out = std::io::stdout();
                out.write_all(&bytes).expect("Failed to write output");
            }
            Err(e) => println!("Error: {}", e),
        }
    } else {
        let engine = IndexReader::open(&args.index).expect("Failed to open index");
        match engine.get_document(args.doc_id) {
            Ok(bytes) => {
                let mut out = std::io::stdout();
                out.write_all(&bytes).expect("Failed to write output");
            }
            Err(e) => println!("Error: {}", e),
        }
    }
}

fn run_build_multi(args: BuildMultiArgs) {
    println!("Building multi-document index -> {:?}", args.output);
    let start = Instant::now();

    let builder = IndexBuilder::new(args.sample_rate);
    let input_paths = args.inputs.clone();
    builder
        .build_multi_from_paths(&args.output, &input_paths)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}

fn run_ingest(args: IngestArgs) {
    let mut file_cfg = None;
    if let Some(path) = &args.config {
        match IngestConfigFile::load(path) {
            Ok(cfg) => file_cfg = Some(cfg),
            Err(e) => {
                eprintln!("Failed to load config {:?}: {:?}", path, e);
                std::process::exit(1);
            }
        }
    }

    let input_patterns = if !args.input.is_empty() {
        args.input
    } else {
        file_cfg
            .as_ref()
            .and_then(|c| c.input_patterns.clone())
            .unwrap_or_default()
    };

    if input_patterns.is_empty() {
        eprintln!("No input patterns provided. Use --input or config file.");
        std::process::exit(1);
    }

    let output_dir = if let Some(out) = args.output {
        out
    } else {
        file_cfg
            .as_ref()
            .and_then(|c| c.output_dir.clone())
            .unwrap_or_else(|| {
                eprintln!("No output directory provided. Use --output or config file.");
                std::process::exit(1);
            })
    };

    let chunk_size = if let Some(v) = args.chunk_size {
        v
    } else if let Some(cfg) = file_cfg.as_ref().and_then(|c| c.chunk_size.as_ref()) {
        size_value_to_usize(cfg).unwrap_or_else(|e| {
            eprintln!("Invalid chunk_size in config: {:?}", e);
            std::process::exit(1);
        })
    } else {
        parse_size("1GiB").expect("default chunk size")
    };

    let read_buffer = if let Some(v) = args.read_buffer {
        v
    } else if let Some(cfg) = file_cfg.as_ref().and_then(|c| c.read_buffer.as_ref()) {
        size_value_to_usize(cfg).unwrap_or_else(|e| {
            eprintln!("Invalid read_buffer in config: {:?}", e);
            std::process::exit(1);
        })
    } else {
        parse_size("8MiB").expect("default read buffer")
    };

    let workers = if let Some(v) = args.workers {
        v
    } else if let Some(v) = file_cfg.as_ref().and_then(|c| c.num_workers) {
        v
    } else {
        4
    };

    let sample_rate = if let Some(v) = args.sample_rate {
        v
    } else if let Some(v) = file_cfg.as_ref().and_then(|c| c.sample_rate) {
        v
    } else {
        32
    };

    println!("Starting distributed ingestion");
    println!("Patterns: {:?}", input_patterns);
    println!("Output: {:?}", output_dir);
    println!("Chunk size: {} ({})", chunk_size, format_bytes(chunk_size as u64));
    println!(
        "Read buffer: {} ({})",
        read_buffer,
        format_bytes(read_buffer as u64)
    );
    println!("Workers: {}", workers);
    println!("Sample rate: {}", sample_rate);

    let config = IngestConfig {
        input_patterns,
        output_dir,
        chunk_size,
        read_buffer,
        num_workers: workers,
        sample_rate,
    };

    let orchestrator = Orchestrator::new(config);
    match orchestrator.run() {
        Ok(_) => println!("Ingestion completed successfully."),
        Err(e) => {
            eprintln!("Ingestion failed: {e:?}");
            std::process::exit(1);
        }
    }
}
