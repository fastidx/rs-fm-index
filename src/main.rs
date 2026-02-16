// use index::builder::ShardBuilder;
// use infini_gram_rs::index::header::ShardHeader;
// use infini_gram_rs::index::query::QueryEngine;
// use infini_gram_rs::io::paged_reader::{GlobalPageCache, PagedReader};

use std::env;
use std::io::Write;
use std::time::Instant;

use rust_fm_index::{IndexBuilder, IndexReader};

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

    let data = std::fs::read(input).expect("Failed to read input");
    let builder = IndexBuilder::new(32);
    builder
        .build_single_document(&data, output)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}

fn run_query(index_path: &str, pattern: &str) {
    let engine = IndexReader::open(index_path).expect("Failed to open index");
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
    let engine = IndexReader::open(index_path).expect("Failed to open index");

    match engine.extract(pos, len) {
        Ok(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            println!("Extracted: {:?}", s);
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn run_doc(index_path: &str, doc_id: usize) {
    let engine = IndexReader::open(index_path).expect("Failed to open index");
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

    let builder = IndexBuilder::new(32);
    let input_paths = inputs.iter().map(|s| s.into()).collect::<Vec<_>>();
    builder
        .build_multi_from_paths(output, &input_paths)
        .expect("Build failed");

    println!("Done in {:.2?}", start.elapsed());
}
