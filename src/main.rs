// use index::builder::ShardBuilder;
// use infini_gram_rs::index::header::ShardHeader;
// use infini_gram_rs::index::query::QueryEngine;
// use infini_gram_rs::io::paged_reader::{GlobalPageCache, PagedReader};

use std::env;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use wavelet_tree_encoding::index::builder::ShardBuilder;
use wavelet_tree_encoding::index::header::ShardHeader;
use wavelet_tree_encoding::index::query::QueryEngine;
use wavelet_tree_encoding::iolib::paged_reader::{GlobalPageCache, PagedReader};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "build" => {
            if args.len() < 4 {
                println!("Usage: infigram build <input_file> <output_idx>");
                return;
            }
            run_build(&args[2], &args[3]);
        }
        "build-multi" => {
            if args.len() < 4 {
                println!("Usage: infigram build-multi <output_idx> <input1> [input2 ...]");
                return;
            }
            run_build_multi(&args[2], &args[3..]);
        }
        "query" => {
            if args.len() < 4 {
                println!("Usage: infigram query <index_file> <pattern>");
                return;
            }
            run_query(&args[2], &args[3]);
        }
        "doc" => {
            if args.len() < 4 {
                println!("Usage: infigram doc <index_file> <doc_id>");
                return;
            }
            let doc_id = args[3].parse().expect("Invalid doc_id");
            run_doc(&args[2], doc_id);
        }
        "extract" => {
            if args.len() < 5 {
                println!("Usage: infigram extract <index_file> <pos> <len>");
                return;
            }
            let pos = args[3].parse().expect("Invalid pos");
            let len = args[4].parse().expect("Invalid len");
            run_extract(&args[2], pos, len);
        }
        _ => print_usage(),
    }
}

fn print_usage() {
    println!("Infini-gram Rust");
    println!("Commands:");
    println!("  build   <input> <output>   Create an index");
    println!("  build-multi <output> <input...>   Create an index from multiple documents");
    println!("  query   <index> <pattern>  Count and locate occurrences");
    println!("  doc     <index> <doc_id>   Extract a full document by ID");
    println!("  extract <index> <pos> <len> Extract text");
}

fn run_build(input: &str, output: &str) {
    println!("Building index for {} -> {}", input, output);
    let start = Instant::now();

    let mut data = std::fs::read(input).expect("Failed to read input");
    if data.contains(&0) {
        panic!("Input contains 0 byte; cannot use 0 as sentinel");
    }
    data.push(0); // sentinel for single-doc builds
    let builder = ShardBuilder::new(32); // Sample rate 32
    builder
        .build_with_offsets(&data, vec![0], output)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}

fn run_query(index_path: &str, pattern: &str) {
    let (header, reader) = load_index(index_path);
    let engine = QueryEngine::new(header, reader);
    let start = Instant::now();

    match engine.count(pattern.as_bytes()) {
        Ok((sp, ep)) => {
            if sp > ep {
                println!("Pattern not found.");
            } else {
                let count = ep - sp + 1;
                println!("Found {} occurrences in {:.2?}", count, start.elapsed());

                // Locate first few
                if count > 0 {
                    let locs = engine.locate(pattern.as_bytes()).unwrap();
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

fn run_extract(index_path: &str, pos: usize, len: usize) {
    let (header, reader) = load_index(index_path);
    let engine = QueryEngine::new(header, reader);

    match engine.extract(pos, len) {
        Ok(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            println!("Extracted: {:?}", s);
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_doc(index_path: &str, doc_id: usize) {
    let (header, reader) = load_index(index_path);
    let engine = QueryEngine::new(header, reader);
    match engine.get_document(doc_id) {
        Ok(bytes) => {
            let mut out = std::io::stdout();
            out.write_all(&bytes).expect("Failed to write output");
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_build_multi(output: &str, inputs: &[String]) {
    println!("Building multi-document index -> {}", output);
    let start = Instant::now();

    let mut text = Vec::new();
    let mut offsets = Vec::new();

    for path in inputs {
        let data = std::fs::read(path).expect("Failed to read input");
        if data.contains(&0) {
            panic!("Input contains 0 byte; cannot use 0 as sentinel");
        }
        offsets.push(text.len() as u64);
        text.extend_from_slice(&data);
        text.push(0); // separator
    }

    let builder = ShardBuilder::new(32);
    builder
        .build_with_offsets(&text, offsets, output)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}

fn load_index(path: &str) -> (ShardHeader, PagedReader) {
    let mut file = std::fs::File::open(path).expect("Failed to open index");
    // 1GB Cache
    let cache = Arc::new(GlobalPageCache::new(1024 * 1024 * 1024, 16));
    let header: ShardHeader = bincode::serde::decode_from_std_read(&mut file, bincode::config::legacy())
        .expect("Invalid header");

    // Hash path to get ID
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    let file_id = hasher.finish();

    let reader = PagedReader::new(path, file_id, cache).expect("Failed to create reader");
    (header, reader)
}
