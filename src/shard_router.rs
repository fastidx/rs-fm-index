use crate::api::IndexReader;
use crate::ingest::orchestrator::{DocumentSegmentMeta, ShardMeta};
use crate::iolib::paged_reader::GlobalPageCache;
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ShardHit {
    pub shard_id: usize,
    pub shard_pos: usize,
    pub doc_id: u64,
    pub doc_offset: u64,
}

#[derive(Debug, Clone)]
struct SegmentRef {
    shard_idx: usize,
    part_index: u32,
    len: u64,
    shard_offset: u64,
}

struct ShardHandle {
    shard_id: usize,
    reader: IndexReader,
    segments: Vec<DocumentSegmentMeta>,
}

pub struct MultiShardReader {
    shards: Vec<ShardHandle>,
    doc_segments: HashMap<u64, Vec<SegmentRef>>,
}

impl MultiShardReader {
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::open_with_cache(dir, 256 * 1024 * 1024, 16)
    }

    pub fn open_with_cache<P: AsRef<Path>>(
        dir: P,
        cache_bytes: usize,
        cache_shards: usize,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        let meta_paths = collect_meta_paths(dir)?;
        if meta_paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no shard_*.meta.json files found",
            ));
        }

        let cache = Arc::new(GlobalPageCache::new(cache_bytes, cache_shards));
        let mut shards = Vec::with_capacity(meta_paths.len());
        let mut doc_segments: HashMap<u64, Vec<SegmentRef>> = HashMap::new();

        for meta_path in meta_paths {
            let data = fs::read_to_string(&meta_path)?;
            let meta: ShardMeta =
                serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let index_path = resolve_index_path(&meta_path, &meta.index_path);
            let reader = IndexReader::open_with_shared_cache(&index_path, cache.clone())?;

            let shard_idx = shards.len();
            for seg in meta.segments.iter() {
                doc_segments
                    .entry(seg.doc_id)
                    .or_default()
                    .push(SegmentRef {
                        shard_idx,
                        part_index: seg.part_index,
                        len: seg.len,
                        shard_offset: seg.shard_offset,
                    });
            }

            shards.push(ShardHandle {
                shard_id: meta.shard_id,
                reader,
                segments: meta.segments,
            });
        }

        for segments in doc_segments.values_mut() {
            segments.sort_by_key(|s| s.part_index);
        }

        Ok(Self { shards, doc_segments })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn doc_count(&self) -> usize {
        self.doc_segments.len()
    }

    pub fn count(&self, pattern: &[u8]) -> io::Result<u64> {
        let mut total = 0u64;
        for shard in &self.shards {
            let (sp, ep) = shard.reader.count(pattern)?;
            if sp <= ep {
                total = total.saturating_add((ep - sp + 1) as u64);
            }
        }
        Ok(total)
    }

    pub fn count_doc_safe(&self, pattern: &[u8]) -> io::Result<u64> {
        let mut total = 0u64;
        for shard in &self.shards {
            total = total.saturating_add(shard.reader.count_doc_safe(pattern)? as u64);
        }
        Ok(total)
    }

    pub fn locate(&self, pattern: &[u8]) -> io::Result<Vec<ShardHit>> {
        let mut hits = Vec::new();
        for shard in &self.shards {
            let locs = shard.reader.locate(pattern)?;
            for pos in locs {
                if let Some((seg_idx, seg_offset)) = shard.reader.pos_to_doc_id(pos)
                    && let Some(seg) = shard.segments.get(seg_idx)
                {
                    let doc_offset = seg.doc_offset + seg_offset as u64;
                    hits.push(ShardHit {
                        shard_id: shard.shard_id,
                        shard_pos: pos,
                        doc_id: seg.doc_id,
                        doc_offset,
                    });
                }
            }
        }
        hits.sort_by_key(|h| (h.doc_id, h.doc_offset, h.shard_id, h.shard_pos));
        Ok(hits)
    }

    pub fn locate_doc_safe(&self, pattern: &[u8]) -> io::Result<Vec<ShardHit>> {
        let mut hits = Vec::new();
        for shard in &self.shards {
            let locs = shard.reader.locate_doc_safe(pattern)?;
            for pos in locs {
                if let Some((seg_idx, seg_offset)) = shard.reader.pos_to_doc_id(pos)
                    && let Some(seg) = shard.segments.get(seg_idx)
                {
                    let doc_offset = seg.doc_offset + seg_offset as u64;
                    hits.push(ShardHit {
                        shard_id: shard.shard_id,
                        shard_pos: pos,
                        doc_id: seg.doc_id,
                        doc_offset,
                    });
                }
            }
        }
        hits.sort_by_key(|h| (h.doc_id, h.doc_offset, h.shard_id, h.shard_pos));
        Ok(hits)
    }

    pub fn get_document(&self, doc_id: u64) -> io::Result<Vec<u8>> {
        let segments = self.doc_segments.get(&doc_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "doc_id not found")
        })?;

        let mut out = Vec::new();
        for seg in segments {
            let shard = &self.shards[seg.shard_idx];
            let bytes = shard
                .reader
                .extract(seg.shard_offset as usize, seg.len as usize)?;
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }
}

fn collect_meta_paths(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|s| s.to_str())
            && name.starts_with("shard_")
            && name.ends_with(".meta.json")
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn resolve_index_path(meta_path: &Path, index_path: &Path) -> PathBuf {
    if index_path.is_absolute() {
        index_path.to_path_buf()
    } else if index_path.exists() {
        index_path.to_path_buf()
    } else {
        meta_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(index_path)
    }
}
