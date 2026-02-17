use crate::iolib::paged_reader::PagedReader;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{self, Read, Write};

// --- Constants ---
const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 8;
const BITS_PER_PAGE: usize = (PAGE_SIZE - HEADER_SIZE) * 8;

// =================================================================================
//  Storage Engine: Paged BitVector
// =================================================================================

pub struct PagedBitVector {
    reader: PagedReader,
    start_offset: u64,
    len_bits: usize,
}

impl PagedBitVector {
    pub fn new(reader: PagedReader, start_offset: u64, len_bits: usize) -> Self {
        Self {
            reader,
            start_offset,
            len_bits,
        }
    }

    pub fn rank1(&self, i: usize) -> io::Result<usize> {
        let limit = i.min(self.len_bits);
        if limit == 0 {
            return Ok(0);
        }

        let last_bit = limit - 1;
        let page_idx = last_bit / BITS_PER_PAGE;
        let bit_offset_in_page = (last_bit % BITS_PER_PAGE) + 1;

        let page_start_global = self.start_offset + (page_idx as u64 * PAGE_SIZE as u64);
        let page_data = self.reader.read_at(page_start_global, PAGE_SIZE)?;

        let base_rank = LittleEndian::read_u64(&page_data[0..8]) as usize;
        let data_slice = &page_data[8..];

        let byte_len = bit_offset_in_page / 8;
        let rem_bits = bit_offset_in_page % 8;

        let mut local_count = 0;
        for &b in &data_slice[0..byte_len] {
            local_count += b.count_ones() as usize;
        }

        if rem_bits > 0 {
            let last_byte = data_slice[byte_len];
            let mask = (1 << rem_bits) - 1;
            local_count += (last_byte & mask).count_ones() as usize;
        }

        Ok(base_rank + local_count)
    }

    pub fn rank0(&self, i: usize) -> io::Result<usize> {
        let limit = i.min(self.len_bits);
        let r1 = self.rank1(limit)?;
        Ok(limit - r1)
    }

    /// Get the bit at index `i`
    pub fn get(&self, i: usize) -> io::Result<bool> {
        if i >= self.len_bits {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }

        let page_idx = i / BITS_PER_PAGE;
        let bit_offset = i % BITS_PER_PAGE;

        let page_start = self.start_offset + (page_idx as u64 * PAGE_SIZE as u64);
        let page_data = self.reader.read_at(page_start, PAGE_SIZE)?;

        // Data starts at byte 8
        let byte_offset = 8 + (bit_offset / 8);
        let bit_in_byte = bit_offset % 8;

        let byte = page_data[byte_offset];
        Ok((byte >> bit_in_byte) & 1 == 1)
    }
}

// =================================================================================
//  Huffman Logic
// =================================================================================

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HuffmanCode {
    pub bits: u32,
    pub len: u8,
}

#[derive(Clone)]
enum Node {
    Leaf(u8),
    Internal(usize, usize),
}

fn count_freq(data: &[u8]) -> [u64; 256] {
    let mut f = [0u64; 256];
    for &b in data {
        f[b as usize] += 1;
    }
    f
}

pub(crate) fn huffman_lengths(freq: &[u64; 256]) -> [u8; 256] {
    let mut heap = BinaryHeap::new();
    let mut nodes = Vec::new();

    for (sym, &w) in freq.iter().enumerate() {
        if w > 0 {
            let idx = nodes.len();
            nodes.push(Node::Leaf(sym as u8));
            heap.push((Reverse(w), idx));
        }
    }

    if heap.is_empty() {
        return [0u8; 256];
    }
    if heap.len() == 1 {
        let mut lens = [0u8; 256];
        let (_, idx) = heap.pop().unwrap();
        if let Node::Leaf(s) = nodes[idx] {
            lens[s as usize] = 1;
        }
        return lens;
    }

    while heap.len() > 1 {
        let (Reverse(w1), a) = heap.pop().unwrap();
        let (Reverse(w2), b) = heap.pop().unwrap();
        let idx = nodes.len();
        nodes.push(Node::Internal(a, b));
        heap.push((Reverse(w1 + w2), idx));
    }

    let mut lens = [0u8; 256];
    let root = heap.pop().unwrap().1;
    fn dfs(nodes: &[Node], idx: usize, depth: u8, lens: &mut [u8; 256]) {
        match nodes[idx] {
            Node::Leaf(s) => lens[s as usize] = depth,
            Node::Internal(l, r) => {
                dfs(nodes, l, depth + 1, lens);
                dfs(nodes, r, depth + 1, lens);
            }
        }
    }
    dfs(&nodes, root, 0, &mut lens);
    lens
}

pub fn canonical_codes(lens: &[u8; 256]) -> [Option<HuffmanCode>; 256] {
    let mut syms: Vec<(u8, u8)> = (0u16..256)
        .filter_map(|s| {
            let l = lens[s as usize];
            if l > 0 { Some((s as u8, l)) } else { None }
        })
        .collect();
    syms.sort_by_key(|&(s, l)| (l, s));

    let mut out = [None; 256];
    let mut code: u32 = 0;
    let mut prev_len = 0;

    for &(sym, len) in &syms {
        if prev_len == 0 {
            prev_len = len;
        }
        if len > prev_len {
            code <<= len - prev_len;
            prev_len = len;
        }
        out[sym as usize] = Some(HuffmanCode { bits: code, len });
        code = code.wrapping_add(1);
    }
    out
}

// =================================================================================
//  Wavelet Tree Builder
// =================================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WaveletNodeShape {
    Leaf {
        symbol: u8,
    },
    Internal {
        left_idx: usize,
        right_idx: usize,
        bit_start: usize,
        bit_len: usize,
    },
}

#[derive(Clone)]
enum BuilderNode {
    Leaf(u8),
    Internal { left: usize, right: usize },
}

pub struct WaveletTreeBuilder {
    pub codes: [Option<HuffmanCode>; 256],
    node_bits: Vec<Vec<bool>>,
    nodes: Vec<BuilderNode>,
}

impl WaveletTreeBuilder {
    pub fn new(data: &[u8]) -> Self {
        let freq = count_freq(data);
        let lens = huffman_lengths(&freq);
        let codes = canonical_codes(&lens);

        let nodes = Self::build_nodes(&codes);
        let node_count = nodes.len();
        Self {
            codes,
            node_bits: vec![Vec::new(); node_count],
            nodes,
        }
    }

    pub fn from_codes(codes: [Option<HuffmanCode>; 256]) -> Self {
        let nodes = Self::build_nodes(&codes);
        let node_count = nodes.len();
        Self {
            codes,
            node_bits: vec![Vec::new(); node_count],
            nodes,
        }
    }

    pub fn process_text(&mut self, data: &[u8]) {
        for &b in data {
            self.process_byte(b);
        }
    }

    pub fn process_reader<R: Read>(&mut self, mut reader: R) -> io::Result<()> {
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            for &b in &buf[..n] {
                self.process_byte(b);
            }
        }
        Ok(())
    }

    fn build_nodes(codes: &[Option<HuffmanCode>; 256]) -> Vec<BuilderNode> {
        let mut nodes = vec![BuilderNode::Internal { left: 0, right: 0 }];

        for (sym, code) in codes.iter().enumerate() {
            if let Some(code) = code {
                let mut curr_idx = 0;
                // MSB first for traversal
                for depth in (0..code.len).rev() {
                    let bit = (code.bits >> depth) & 1;

                    let next_child_idx = match nodes[curr_idx] {
                        BuilderNode::Internal { left, right } => {
                            if bit == 0 {
                                left
                            } else {
                                right
                            }
                        }
                        BuilderNode::Leaf(_) => panic!("Invalid prefix code: path collision"),
                    };

                    if next_child_idx == 0 {
                        let new_node_idx = nodes.len();
                        nodes.push(BuilderNode::Internal { left: 0, right: 0 });

                        match &mut nodes[curr_idx] {
                            BuilderNode::Internal { left, right } => {
                                if bit == 0 {
                                    *left = new_node_idx;
                                } else {
                                    *right = new_node_idx;
                                }
                            }
                            _ => unreachable!(),
                        }
                        curr_idx = new_node_idx;
                    } else {
                        curr_idx = next_child_idx;
                    }
                }
                nodes[curr_idx] = BuilderNode::Leaf(sym as u8);
            }
        }

        nodes
    }

    fn process_byte(&mut self, b: u8) {
        let code = self.codes[b as usize].unwrap();
        let mut curr = 0;
        for depth in (0..code.len).rev() {
            let bit = (code.bits >> depth) & 1;
            self.node_bits[curr].push(bit == 1);

            match self.nodes[curr] {
                BuilderNode::Internal { left, right } => {
                    curr = if bit == 0 { left } else { right };
                }
                _ => break,
            }
        }
    }

    pub fn write_to_file<W: Write + std::io::Seek>(
        self,
        writer: &mut W,
    ) -> io::Result<(u64, Vec<WaveletNodeShape>)> {
        let start_pos = writer.stream_position()?;
        let mut shape_map = Vec::with_capacity(self.nodes.len());
        let mut global_bit_cursor = 0usize;

        // Flatten all node bitvectors into a single packed bitstream.
        let mut packed_data: Vec<u8> = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_in_byte = 0u8;

        let push_bit = |bit: bool,
                        packed_data: &mut Vec<u8>,
                        current_byte: &mut u8,
                        bit_in_byte: &mut u8| {
            if bit {
                *current_byte |= 1 << *bit_in_byte;
            }
            *bit_in_byte += 1;
            if *bit_in_byte == 8 {
                packed_data.push(*current_byte);
                *current_byte = 0;
                *bit_in_byte = 0;
            }
        };

        for (i, node) in self.nodes.iter().enumerate() {
            match node {
                BuilderNode::Leaf(sym) => {
                    shape_map.push(WaveletNodeShape::Leaf { symbol: *sym });
                }
                BuilderNode::Internal { left, right } => {
                    let bits = &self.node_bits[i];
                    let start = global_bit_cursor;
                    let len = bits.len();

                    for &bit in bits {
                        push_bit(bit, &mut packed_data, &mut current_byte, &mut bit_in_byte);
                    }

                    global_bit_cursor += len;
                    shape_map.push(WaveletNodeShape::Internal {
                        left_idx: *left,
                        right_idx: *right,
                        bit_start: start,
                        bit_len: len,
                    });
                }
            }
        }

        if bit_in_byte > 0 {
            packed_data.push(current_byte);
        }

        // Write the global bitstream using paged layout with base-rank headers.
        let payload_size = PAGE_SIZE - HEADER_SIZE;
        let mut current_base_rank = 0u64;

        for chunk in packed_data.chunks(payload_size) {
            writer.write_u64::<LittleEndian>(current_base_rank)?;
            writer.write_all(chunk)?;

            for &b in chunk {
                current_base_rank += b.count_ones() as u64;
            }

            if chunk.len() < payload_size {
                let pad_len = payload_size - chunk.len();
                writer.write_all(&vec![0u8; pad_len])?;
            }
        }

        Ok((start_pos, shape_map))
    }
}

// =================================================================================
//  Wavelet Tree Reader
// =================================================================================

pub struct PagedWaveletTree {
    global_bv: PagedBitVector,
    nodes: Vec<WaveletNodeShape>,
    codes: [Option<HuffmanCode>; 256],
    text_len: usize,
}

impl PagedWaveletTree {
    pub fn new(
        reader: PagedReader,
        nodes: Vec<WaveletNodeShape>,
        codes: [Option<HuffmanCode>; 256],
        text_len: usize,
        wt_start_offset: u64,
    ) -> Self {
        let total_bits = nodes
            .iter()
            .map(|n| match n {
                WaveletNodeShape::Internal { bit_start, bit_len, .. } => bit_start + bit_len,
                _ => 0,
            })
            .max()
            .unwrap_or(0);

        let global_bv = PagedBitVector::new(reader, wt_start_offset, total_bits);
        Self {
            global_bv,
            nodes,
            codes,
            text_len,
        }
    }

    pub fn rank(&self, symbol: u8, mut i: usize) -> io::Result<usize> {
        if i == 0 {
            return Ok(0);
        }
        if i > self.text_len {
            i = self.text_len;
        }

        let code = match self.codes[symbol as usize] {
            Some(c) => c,
            None => return Ok(0),
        };

        let mut curr_node_idx = 0;

        for depth in (0..code.len).rev() {
            match &self.nodes[curr_node_idx] {
                WaveletNodeShape::Leaf { .. } => return Ok(i),
                WaveletNodeShape::Internal {
                    left_idx,
                    right_idx,
                    bit_start,
                    bit_len,
                } => {
                    let bit = (code.bits >> depth) & 1;
                    let go_right = bit == 1;
                    let global_start = *bit_start;
                    let local_i = i.min(*bit_len);
                    let global_pos = global_start + local_i;
                    let rank_at_start = self.global_bv.rank1(global_start)?;
                    let rank_at_pos = self.global_bv.rank1(global_pos)?;
                    let ones = rank_at_pos.saturating_sub(rank_at_start);

                    if go_right {
                        i = ones;
                        curr_node_idx = *right_idx;
                    } else {
                        i = local_i.saturating_sub(ones);
                        curr_node_idx = *left_idx;
                    }
                }
            }
        }
        Ok(i)
    }

    /// Retrieve the symbol at index `i` (BWT[i])
    pub fn access(&self, mut i: usize) -> io::Result<u8> {
        if i >= self.text_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }

        let mut curr_node_idx = 0;

        // We traverse strictly based on the bit values we find
        loop {
            match &self.nodes[curr_node_idx] {
                WaveletNodeShape::Leaf { symbol } => return Ok(*symbol),
                WaveletNodeShape::Internal {
                    left_idx,
                    right_idx,
                    bit_start,
                    bit_len,
                } => {
                    let local_i = i.min(*bit_len);
                    let global_pos = *bit_start + local_i;
                    let bit = self.global_bv.get(global_pos)?;

                    let rank_at_start = self.global_bv.rank1(*bit_start)?;
                    let rank_at_pos = self.global_bv.rank1(global_pos)?;
                    let ones = rank_at_pos.saturating_sub(rank_at_start);

                    if bit {
                        i = ones;
                        curr_node_idx = *right_idx;
                    } else {
                        i = local_i.saturating_sub(ones);
                        curr_node_idx = *left_idx;
                    }
                }
            }
        }
    }
}

// --- Extensive Integration Tests ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::sharded_fifo::ShardedFastS3Fifo;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    // Helper: Runs a full cycle of build -> save -> load -> query
    fn run_wavelet_test(text: &[u8]) {
        // 1. Build
        let mut builder = WaveletTreeBuilder::new(text);
        builder.process_text(text);
        let codes_copy = builder.codes; // Keep for reader

        // 2. Write
        let mut file = NamedTempFile::new().unwrap();
        let (wt_start, shape) = builder.write_to_file(file.as_file_mut()).unwrap();
        file.as_file_mut().sync_all().unwrap();

        // 3. Reader Setup
        let cache = Arc::new(ShardedFastS3Fifo::new(1024 * 1024, 2));
        let reader = PagedReader::new(file.path(), 100, cache).unwrap();
        let wt = PagedWaveletTree::new(reader, shape, codes_copy, text.len(), wt_start);

        // 4. Verify Rank for ALL characters at ALL positions
        // This is the "Hero Test"
        let symbols: Vec<u8> = text
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        for &sym in &symbols {
            for i in 0..=text.len() {
                // Naive count
                let expected = text[0..i].iter().filter(|&&c| c == sym).count();
                let actual = wt.rank(sym, i).expect("Rank failed");
                assert_eq!(
                    actual, expected,
                    "Rank mismatch for char {} at index {}",
                    sym, i
                );
            }
        }
    }

    #[test]
    fn test_simple_string() {
        run_wavelet_test(b"banana");
    }

    #[test]
    fn test_skewed_distribution() {
        // A:8, B:4, C:2, D:1 - Perfect Huffman powers of 2
        let text = b"aaaaaaaabbbbccde";
        run_wavelet_test(text);
    }

    #[test]
    fn test_binary_alphabet_spanning_pages() {
        // Generates enough data to force multiple pages in the root bitvector
        // 40,000 bits > 32,704 bits (1 page payload)
        let len = 40_000;
        let mut text = Vec::with_capacity(len);
        for i in 0..len {
            text.push(if i % 3 == 0 { b'A' } else { b'B' });
        }
        run_wavelet_test(&text);
    }

    #[test]
    fn test_full_byte_range() {
        // Force a complex tree with 256 leaves
        let mut text = Vec::new();
        for i in 0..=255 {
            text.push(i as u8);
            text.push(i as u8); // Ensure freq > 0
        }
        run_wavelet_test(&text);
    }
}

#[cfg(test)]
mod comprehensive_tests {
    use super::*;
    use crate::cache::sharded_fifo::ShardedFastS3Fifo;
    use crate::iolib::paged_reader::GlobalPageCache;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    // --- Fuzz Testing Utility ---
    // Generates random text, builds tree, verifies rank queries match naive implementation.
    fn fuzz_wavelet_tree(alphabet_size: u8, len: usize, seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);

        // 1. Generate Random Text
        let mut text = Vec::with_capacity(len);
        for _ in 0..len {
            let sym = rng.random_range(0..=alphabet_size);
            text.push(sym);
        }

        // 2. Build Tree
        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_text(&text);
        let codes_copy = builder.codes;

        let mut file = NamedTempFile::new().unwrap();
        let (wt_start, shape) = builder.write_to_file(file.as_file_mut()).unwrap();
        file.as_file().sync_all().unwrap();

        // 3. Open Reader
        let cache = Arc::new(ShardedFastS3Fifo::new(50 * 1024 * 1024, 4));
        let reader = PagedReader::new(file.path(), seed, cache).unwrap();
        let wt = PagedWaveletTree::new(reader, shape, codes_copy, text.len(), wt_start);

        // 4. Verify Rank
        // Check random positions and symbols
        for _ in 0..1000 {
            // Use saturating_add so u8::MAX doesn't overflow; when full range is used,
            // we can't generate a non-existent symbol anyway.
            let upper = alphabet_size.saturating_add(1);
            let query_sym = rng.random_range(0..=upper); // +1 to check non-existent chars when possible
            let query_idx = rng.random_range(0..=len);

            let expected = text[0..query_idx]
                .iter()
                .filter(|&&c| c == query_sym)
                .count();
            let actual = wt.rank(query_sym, query_idx).unwrap();

            if actual != expected {
                panic!(
                    "Mismatch! Sym: {}, Idx: {}. Expected: {}, Got: {}\nText snippet: {:?}",
                    query_sym,
                    query_idx,
                    expected,
                    actual,
                    &text[0..std::cmp::min(len, 20)]
                );
            }
        }
    }

    #[test]
    fn test_fuzz_small_alphabet_short() {
        // 4 symbols, 100 length
        fuzz_wavelet_tree(3, 100, 42);
    }

    #[test]
    fn test_fuzz_full_alphabet_medium() {
        // 255 symbols, 10,000 length
        fuzz_wavelet_tree(255, 10_000, 999);
    }

    #[test]
    fn test_large_bitvector_paging() {
        // This test specifically targets the "Self-Indexing Page" logic.
        // We need enough bits in a SINGLE node to cross 4KB.
        // 4096 bytes * 8 = 32,768 bits per page (approx).
        // Let's use 100,000 bits.
        // Alphabet: 2 symbols (0 and 1).
        // This forces a single root node with a huge BitVector.

        let len = 100_000;
        let mut text = Vec::with_capacity(len);
        for i in 0..len {
            text.push((i % 2) as u8); // 0, 1, 0, 1...
        }

        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_text(&text);
        let codes = builder.codes;

        let mut file = NamedTempFile::new().unwrap();
        let (wt_start, shape) = builder.write_to_file(file.as_file_mut()).unwrap();
        file.as_file_mut().sync_all().unwrap();

        let cache = Arc::new(ShardedFastS3Fifo::new(10 * 1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 10101, cache).unwrap();
        let wt = PagedWaveletTree::new(reader, shape, codes, len, wt_start);

        // Verify Rank at deep offsets
        // At index 80,000 (Page ~2 or 3)
        // Count of '0' should be 40,000
        assert_eq!(wt.rank(0, 80_000).unwrap(), 40_000);
        // Count of '1' should be 40,000
        assert_eq!(wt.rank(1, 80_000).unwrap(), 40_000);

        // Verify odd index
        // At index 80,001 (next is '0', so 0-count increases)
        // 0, 1, 0... index 80,000 is '0'.
        assert_eq!(wt.rank(0, 80_001).unwrap(), 40_001);
    }

    #[test]
    fn test_edge_case_empty_input() {
        let text: Vec<u8> = vec![];
        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_text(&text);

        // FIX: Extract codes before 'builder' is consumed by 'write_to_file'
        let codes = builder.codes;

        let mut file = NamedTempFile::new().unwrap();
        // 'builder' is moved here
        let (wt_start, shape) = builder.write_to_file(file.as_file_mut()).unwrap();

        let cache = Arc::new(GlobalPageCache::new(1024, 1));
        let reader = PagedReader::new(file.path(), 0, cache).unwrap();

        // Use the extracted codes
        let wt = PagedWaveletTree::new(reader, shape, codes, 0, wt_start);

        assert_eq!(wt.rank(b'a', 0).unwrap(), 0);
    }
}
