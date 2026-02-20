use crate::iolib::paged_reader::PagedReader;
use crate::index::encoding::ALPHABET_SIZE;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use tempfile::{tempfile, NamedTempFile};

// --- Constants ---
const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 8;
const BITS_PER_PAGE: usize = (PAGE_SIZE - HEADER_SIZE) * 8;

pub const DEFAULT_WAVELET_MAX_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaveletBuildMode {
    InMemory,
    Streaming,
    Auto { max_bytes: usize },
}

impl Default for WaveletBuildMode {
    fn default() -> Self {
        Self::Auto {
            max_bytes: DEFAULT_WAVELET_MAX_BYTES,
        }
    }
}

pub(crate) fn paged_wavelet_bytes(total_bits: usize) -> u64 {
    let payload_size = PAGE_SIZE - HEADER_SIZE;
    let packed_bytes = total_bits.div_ceil(8);
    if packed_bytes == 0 {
        return 0;
    }
    let pages = packed_bytes.div_ceil(payload_size);
    (pages * PAGE_SIZE) as u64
}

pub(crate) trait WaveletBuildStrategy {
    fn tree_shape(&self) -> &[WaveletNodeShape];
    fn wavelet_bytes(&self) -> u64;
    fn write_to(&self, bwt_file: &NamedTempFile, writer: &mut dyn Write) -> io::Result<()>;
}

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
    Leaf(u16),
    Internal(usize, usize),
}

fn count_freq(data: &[u16]) -> [u64; ALPHABET_SIZE] {
    let mut f = [0u64; ALPHABET_SIZE];
    for &sym in data {
        let idx = sym as usize;
        if idx >= ALPHABET_SIZE {
            continue;
        }
        f[idx] += 1;
    }
    f
}

#[doc(hidden)]
pub fn huffman_lengths(freq: &[u64; ALPHABET_SIZE]) -> [u8; ALPHABET_SIZE] {
    let mut heap = BinaryHeap::new();
    let mut nodes = Vec::new();

    for (sym, &w) in freq.iter().enumerate() {
        if w > 0 {
            let idx = nodes.len();
            nodes.push(Node::Leaf(sym as u16));
            heap.push((Reverse(w), idx));
        }
    }

    if heap.is_empty() {
        return [0u8; ALPHABET_SIZE];
    }
    if heap.len() == 1 {
        let mut lens = [0u8; ALPHABET_SIZE];
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

    let mut lens = [0u8; ALPHABET_SIZE];
    let root = heap.pop().unwrap().1;
    fn dfs(nodes: &[Node], idx: usize, depth: u8, lens: &mut [u8; ALPHABET_SIZE]) {
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

pub fn canonical_codes(lens: &[u8; ALPHABET_SIZE]) -> [Option<HuffmanCode>; ALPHABET_SIZE] {
    let mut syms: Vec<(u16, u8)> = (0..ALPHABET_SIZE)
        .filter_map(|s| {
            let l = lens[s as usize];
            if l > 0 {
                Some((s as u16, l))
            } else {
                None
            }
        })
        .collect();
    syms.sort_by_key(|&(s, l)| (l, s));

    let mut out: [Option<HuffmanCode>; ALPHABET_SIZE] = [None; ALPHABET_SIZE];
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
        symbol: u16,
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
    Leaf(u16),
    Internal { left: usize, right: usize },
}

#[doc(hidden)]
pub struct WaveletStreamPlan {
    nodes: Vec<BuilderNode>,
    tree_shape: Vec<WaveletNodeShape>,
    bit_lens: Vec<usize>,
    total_bits: usize,
}

impl WaveletStreamPlan {
    #[doc(hidden)]
    pub fn tree_shape(&self) -> &[WaveletNodeShape] {
        &self.tree_shape
    }

    #[doc(hidden)]
    pub fn total_bits(&self) -> usize {
        self.total_bits
    }
}

#[doc(hidden)]
pub fn plan_wavelet_stream(
    codes: &[Option<HuffmanCode>; ALPHABET_SIZE],
    counts: &[u64; ALPHABET_SIZE],
) -> WaveletStreamPlan {
    let nodes = build_nodes(codes);
    let mut bit_lens = vec![0usize; nodes.len()];

    for (sym, code_opt) in codes.iter().enumerate() {
        let Some(code) = code_opt else { continue };
        let freq = counts[sym] as usize;
        if freq == 0 {
            continue;
        }

        let mut curr = 0usize;
        for depth in (0..code.len).rev() {
            bit_lens[curr] = bit_lens[curr].saturating_add(freq);
            let bit = (code.bits >> depth) & 1;
            match nodes[curr] {
                BuilderNode::Internal { left, right } => {
                    curr = if bit == 0 { left } else { right };
                }
                BuilderNode::Leaf(_) => break,
            }
        }
    }

    let mut tree_shape = Vec::with_capacity(nodes.len());
    let mut cursor = 0usize;
    for (i, node) in nodes.iter().enumerate() {
        match node {
            BuilderNode::Leaf(sym) => {
                tree_shape.push(WaveletNodeShape::Leaf { symbol: *sym });
            }
            BuilderNode::Internal { left, right } => {
                let bit_len = bit_lens[i];
                tree_shape.push(WaveletNodeShape::Internal {
                    left_idx: *left,
                    right_idx: *right,
                    bit_start: cursor,
                    bit_len,
                });
                cursor += bit_len;
            }
        }
    }

    WaveletStreamPlan {
        nodes,
        tree_shape,
        bit_lens,
        total_bits: cursor,
    }
}

pub struct WaveletTreeBuilder {
    pub codes: [Option<HuffmanCode>; ALPHABET_SIZE],
    node_bits: Vec<Vec<bool>>,
    nodes: Vec<BuilderNode>,
}

impl WaveletTreeBuilder {
    pub fn new(data: &[u16]) -> Self {
        let freq = count_freq(data);
        let lens = huffman_lengths(&freq);
        let codes = canonical_codes(&lens);

        let nodes = build_nodes(&codes);
        let node_count = nodes.len();
        Self {
            codes,
            node_bits: vec![Vec::new(); node_count],
            nodes,
        }
    }

    pub fn from_codes(codes: [Option<HuffmanCode>; ALPHABET_SIZE]) -> Self {
        let nodes = build_nodes(&codes);
        let node_count = nodes.len();
        Self {
            codes,
            node_bits: vec![Vec::new(); node_count],
            nodes,
        }
    }

    pub fn process_symbols(&mut self, data: &[u16]) {
        for &sym in data {
            self.process_symbol(sym);
        }
    }

    pub fn process_reader_u16<R: Read>(&mut self, mut reader: R) -> io::Result<()> {
        let mut buf = [0u8; 8192];
        let mut carry: Option<u8> = None;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            let mut i = 0usize;
            if let Some(prev) = carry.take() {
                if i >= n {
                    carry = Some(prev);
                    break;
                }
                let sym = u16::from_le_bytes([prev, buf[i]]);
                self.process_symbol(sym);
                i += 1;
            }
            while i + 1 < n {
                let sym = u16::from_le_bytes([buf[i], buf[i + 1]]);
                self.process_symbol(sym);
                i += 2;
            }
            if i < n {
                carry = Some(buf[i]);
            }
        }
        if carry.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "trailing byte in u16 stream",
            ));
        }
        Ok(())
    }

    // build_nodes now lives at module scope

    fn process_symbol(&mut self, symbol: u16) {
        let idx = symbol as usize;
        let code = self.codes[idx].unwrap();
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

fn build_nodes(codes: &[Option<HuffmanCode>; ALPHABET_SIZE]) -> Vec<BuilderNode> {
    let mut nodes = vec![BuilderNode::Internal { left: 0, right: 0 }];

    for (sym, code) in codes.iter().enumerate() {
        if let Some(code) = code {
            let mut curr_idx = 0;
            // MSB first for traversal
            for depth in (0..code.len).rev() {
                let bit = (code.bits >> depth) & 1;

                let next_child_idx = match nodes[curr_idx] {
                    BuilderNode::Internal { left, right } => {
                        if bit == 0 { left } else { right }
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
            nodes[curr_idx] = BuilderNode::Leaf(sym as u16);
        }
    }

    nodes
}

struct NodeBitWriter {
    writer: BufWriter<File>,
    current_byte: u8,
    bit_in_byte: u8,
    bits_written: usize,
}

impl NodeBitWriter {
    fn new() -> io::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(tempfile()?),
            current_byte: 0,
            bit_in_byte: 0,
            bits_written: 0,
        })
    }

    fn push_bit(&mut self, bit: bool) -> io::Result<()> {
        if bit {
            self.current_byte |= 1 << self.bit_in_byte;
        }
        self.bit_in_byte += 1;
        self.bits_written += 1;
        if self.bit_in_byte == 8 {
            self.writer.write_all(&[self.current_byte])?;
            self.current_byte = 0;
            self.bit_in_byte = 0;
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<NodeBitFile> {
        if self.bit_in_byte > 0 {
            self.writer.write_all(&[self.current_byte])?;
            self.current_byte = 0;
            self.bit_in_byte = 0;
        }
        self.writer.flush()?;
        let mut file = self.writer.into_inner()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(NodeBitFile { file })
    }
}

struct InMemoryWaveletBuild {
    tree_shape: Vec<WaveletNodeShape>,
    bytes: Vec<u8>,
}

impl InMemoryWaveletBuild {
    fn build(
        bwt_file: &NamedTempFile,
        codes: [Option<HuffmanCode>; ALPHABET_SIZE],
    ) -> io::Result<Self> {
        let mut wt_builder = WaveletTreeBuilder::from_codes(codes);
        let bwt_read = bwt_file.reopen()?;
        let mut bwt_reader = BufReader::new(bwt_read);
        wt_builder.process_reader_u16(&mut bwt_reader)?;

        let mut wt_buf = Cursor::new(Vec::new());
        let (_offset, tree_shape) = wt_builder.write_to_file(&mut wt_buf)?;
        let bytes = wt_buf.into_inner();
        Ok(Self { tree_shape, bytes })
    }
}

impl WaveletBuildStrategy for InMemoryWaveletBuild {
    fn tree_shape(&self) -> &[WaveletNodeShape] {
        &self.tree_shape
    }

    fn wavelet_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn write_to(&self, _bwt_file: &NamedTempFile, writer: &mut dyn Write) -> io::Result<()> {
        writer.write_all(&self.bytes)
    }
}

struct StreamingWaveletBuild {
    tree_shape: Vec<WaveletNodeShape>,
    codes: [Option<HuffmanCode>; ALPHABET_SIZE],
    plan: WaveletStreamPlan,
    wavelet_bytes: u64,
}

impl WaveletBuildStrategy for StreamingWaveletBuild {
    fn tree_shape(&self) -> &[WaveletNodeShape] {
        &self.tree_shape
    }

    fn wavelet_bytes(&self) -> u64 {
        self.wavelet_bytes
    }

    fn write_to(&self, bwt_file: &NamedTempFile, writer: &mut dyn Write) -> io::Result<()> {
        let bwt_read = bwt_file.reopen()?;
        let bwt_reader = BufReader::new(bwt_read);
        write_wavelet_stream_from_bwt(bwt_reader, &self.codes, &self.plan, writer)
    }
}

pub(crate) fn make_wavelet_build_strategy(
    mode: WaveletBuildMode,
    codes: [Option<HuffmanCode>; ALPHABET_SIZE],
    counts: &[u64; ALPHABET_SIZE],
    bwt_file: &NamedTempFile,
) -> io::Result<Box<dyn WaveletBuildStrategy>> {
    let plan = plan_wavelet_stream(&codes, counts);
    let total_bits = plan.total_bits();
    let resolved = match mode {
        WaveletBuildMode::Auto { max_bytes } => {
            if total_bits > max_bytes {
                WaveletBuildMode::Streaming
            } else {
                WaveletBuildMode::InMemory
            }
        }
        other => other,
    };

    match resolved {
        WaveletBuildMode::InMemory => Ok(Box::new(InMemoryWaveletBuild::build(bwt_file, codes)?)),
        WaveletBuildMode::Streaming => {
            let tree_shape = plan.tree_shape().to_vec();
            let wavelet_bytes = paged_wavelet_bytes(total_bits);
            Ok(Box::new(StreamingWaveletBuild {
                tree_shape,
                codes,
                plan,
                wavelet_bytes,
            }))
        }
        WaveletBuildMode::Auto { .. } => unreachable!(),
    }
}

struct NodeBitFile {
    file: File,
}

struct NodeBitReader {
    reader: BufReader<File>,
    current_byte: u8,
    bit_in_byte: u8,
    bits_remaining: usize,
}

impl NodeBitReader {
    fn new(file: File, bits_remaining: usize) -> Self {
        Self {
            reader: BufReader::new(file),
            current_byte: 0,
            bit_in_byte: 8,
            bits_remaining,
        }
    }

    fn next_bit(&mut self) -> io::Result<Option<bool>> {
        if self.bits_remaining == 0 {
            return Ok(None);
        }
        if self.bit_in_byte >= 8 {
            let mut buf = [0u8; 1];
            self.reader.read_exact(&mut buf)?;
            self.current_byte = buf[0];
            self.bit_in_byte = 0;
        }
        let bit = (self.current_byte >> self.bit_in_byte) & 1 == 1;
        self.bit_in_byte += 1;
        self.bits_remaining -= 1;
        Ok(Some(bit))
    }
}

struct PagedBitWriter<'a> {
    writer: &'a mut dyn Write,
    payload_size: usize,
    page_buf: Vec<u8>,
    current_byte: u8,
    bit_in_byte: u8,
    page_ones: u64,
    base_rank: u64,
}

impl<'a> PagedBitWriter<'a> {
    fn new(writer: &'a mut dyn Write) -> Self {
        let payload_size = PAGE_SIZE - HEADER_SIZE;
        Self {
            writer,
            payload_size,
            page_buf: Vec::with_capacity(payload_size),
            current_byte: 0,
            bit_in_byte: 0,
            page_ones: 0,
            base_rank: 0,
        }
    }

    fn push_bit(&mut self, bit: bool) -> io::Result<()> {
        if bit {
            self.current_byte |= 1 << self.bit_in_byte;
        }
        self.bit_in_byte += 1;
        if self.bit_in_byte == 8 {
            self.flush_byte()?;
        }
        Ok(())
    }

    fn flush_byte(&mut self) -> io::Result<()> {
        let byte = self.current_byte;
        self.current_byte = 0;
        self.bit_in_byte = 0;
        self.page_buf.push(byte);
        self.page_ones += byte.count_ones() as u64;
        if self.page_buf.len() == self.payload_size {
            self.flush_page()?;
        }
        Ok(())
    }

    fn flush_page(&mut self) -> io::Result<()> {
        self.writer.write_u64::<LittleEndian>(self.base_rank)?;
        self.writer.write_all(&self.page_buf)?;
        if self.page_buf.len() < self.payload_size {
            let pad = self.payload_size - self.page_buf.len();
            self.writer.write_all(&vec![0u8; pad])?;
        }
        self.base_rank = self.base_rank.saturating_add(self.page_ones);
        self.page_buf.clear();
        self.page_ones = 0;
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        if self.bit_in_byte > 0 {
            self.flush_byte()?;
        }
        if !self.page_buf.is_empty() {
            self.flush_page()?;
        }
        Ok(())
    }
}

#[doc(hidden)]
pub fn write_wavelet_stream_from_bwt<R: Read>(
    mut reader: R,
    codes: &[Option<HuffmanCode>; ALPHABET_SIZE],
    plan: &WaveletStreamPlan,
    writer: &mut dyn Write,
) -> io::Result<()> {
    let mut node_writers: Vec<Option<NodeBitWriter>> = Vec::with_capacity(plan.nodes.len());
    for node in &plan.nodes {
        match node {
            BuilderNode::Internal { .. } => node_writers.push(Some(NodeBitWriter::new()?)),
            BuilderNode::Leaf(_) => node_writers.push(None),
        }
    }

    let mut bits_written = vec![0usize; plan.nodes.len()];

    let mut emit_symbol = |symbol: u16| -> io::Result<()> {
        let idx = symbol as usize;
        if idx >= ALPHABET_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symbol out of range for wavelet codes",
            ));
        }
        let code = codes[idx].ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "missing code for symbol in wavelet stream",
            )
        })?;
        let mut curr = 0usize;
        for depth in (0..code.len).rev() {
            let bit = (code.bits >> depth) & 1;
            if let Some(writer) = node_writers[curr].as_mut() {
                writer.push_bit(bit == 1)?;
                bits_written[curr] = bits_written[curr].saturating_add(1);
            }
            match plan.nodes[curr] {
                BuilderNode::Internal { left, right } => {
                    curr = if bit == 0 { left } else { right };
                }
                BuilderNode::Leaf(_) => break,
            }
        }
        Ok(())
    };

    let mut buf = [0u8; 8192];
    let mut carry: Option<u8> = None;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let mut i = 0usize;
        if let Some(prev) = carry.take() {
            if i >= n {
                carry = Some(prev);
                break;
            }
            let sym = u16::from_le_bytes([prev, buf[i]]);
            emit_symbol(sym)?;
            i += 1;
        }
        while i + 1 < n {
            let sym = u16::from_le_bytes([buf[i], buf[i + 1]]);
            emit_symbol(sym)?;
            i += 2;
        }
        if i < n {
            carry = Some(buf[i]);
        }
    }
    if carry.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "trailing byte in u16 BWT stream",
        ));
    }

    let mut node_files: Vec<Option<NodeBitFile>> = Vec::with_capacity(plan.nodes.len());
    for (idx, writer_opt) in node_writers.into_iter().enumerate() {
        if let Some(writer) = writer_opt {
            if bits_written[idx] != plan.bit_lens[idx] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "wavelet bit count mismatch while streaming",
                ));
            }
            let file = writer.finish()?;
            node_files.push(Some(file));
        } else {
            node_files.push(None);
        }
    }

    let mut paged_writer = PagedBitWriter::new(writer);
    for (idx, node) in plan.nodes.iter().enumerate() {
        if let BuilderNode::Internal { .. } = node {
            let bit_len = plan.bit_lens[idx];
            if bit_len == 0 {
                continue;
            }
            let file = node_files[idx]
                .take()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing node file"))?;
            let mut reader = NodeBitReader::new(file.file, bit_len);
            while let Some(bit) = reader.next_bit()? {
                paged_writer.push_bit(bit)?;
            }
        }
    }
    paged_writer.finish()
}

// =================================================================================
//  Wavelet Tree Reader
// =================================================================================

pub struct PagedWaveletTree {
    global_bv: PagedBitVector,
    nodes: Vec<WaveletNodeShape>,
    codes: [Option<HuffmanCode>; ALPHABET_SIZE],
    text_len: usize,
}

impl PagedWaveletTree {
    pub fn new(
        reader: PagedReader,
        nodes: Vec<WaveletNodeShape>,
        codes: [Option<HuffmanCode>; ALPHABET_SIZE],
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

    pub fn rank(&self, symbol: u16, mut i: usize) -> io::Result<usize> {
        if i == 0 {
            return Ok(0);
        }
        if i > self.text_len {
            i = self.text_len;
        }

        let idx = symbol as usize;
        if idx >= self.codes.len() {
            return Ok(0);
        }

        let code = match self.codes[idx] {
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
    pub fn access(&self, mut i: usize) -> io::Result<u16> {
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
    fn run_wavelet_test(text: &[u16]) {
        // 1. Build
        let mut builder = WaveletTreeBuilder::new(text);
        builder.process_symbols(text);
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
        let symbols: Vec<u16> = text
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
        let text: Vec<u16> = b"banana".iter().map(|&b| b as u16).collect();
        run_wavelet_test(&text);
    }

    #[test]
    fn test_skewed_distribution() {
        // A:8, B:4, C:2, D:1 - Perfect Huffman powers of 2
        let text: Vec<u16> = b"aaaaaaaabbbbccde".iter().map(|&b| b as u16).collect();
        run_wavelet_test(&text);
    }

    #[test]
    fn test_binary_alphabet_spanning_pages() {
        // Generates enough data to force multiple pages in the root bitvector
        // 40,000 bits > 32,704 bits (1 page payload)
        let len = 40_000;
        let mut text = Vec::with_capacity(len);
        for i in 0..len {
            let sym = if i % 3 == 0 { b'A' } else { b'B' };
            text.push(sym as u16);
        }
        run_wavelet_test(&text);
    }

    #[test]
    fn test_full_byte_range() {
        // Force a complex tree with full byte range (+256 for binary mode)
        let mut text = Vec::new();
        for i in 0..=256u16 {
            text.push(i);
            text.push(i); // Ensure freq > 0
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
    fn fuzz_wavelet_tree(alphabet_size: u16, len: usize, seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);

        // 1. Generate Random Text
        let mut text = Vec::with_capacity(len);
        for _ in 0..len {
            let sym = rng.random_range(0..=alphabet_size);
            text.push(sym);
        }

        // 2. Build Tree
        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_symbols(&text);
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
            // Use saturating_add so u16::MAX doesn't overflow; when full range is used,
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
        // 256 symbols, 10,000 length (0..=255)
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
            text.push((i % 2) as u16); // 0, 1, 0, 1...
        }

        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_symbols(&text);
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
        let text: Vec<u16> = vec![];
        let mut builder = WaveletTreeBuilder::new(&text);
        builder.process_symbols(&text);

        // FIX: Extract codes before 'builder' is consumed by 'write_to_file'
        let codes = builder.codes;

        let mut file = NamedTempFile::new().unwrap();
        // 'builder' is moved here
        let (wt_start, shape) = builder.write_to_file(file.as_file_mut()).unwrap();

        let cache = Arc::new(GlobalPageCache::new(1024, 1));
        let reader = PagedReader::new(file.path(), 0, cache).unwrap();

        // Use the extracted codes
        let wt = PagedWaveletTree::new(reader, shape, codes, 0, wt_start);

        assert_eq!(wt.rank(b'a' as u16, 0).unwrap(), 0);
    }
}
