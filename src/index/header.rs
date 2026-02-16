use crate::index::wavelet::{HuffmanCode, WaveletNodeShape};
use serde::{Deserialize, Serialize};
use std::io;

mod serde_arrays {
    use super::HuffmanCode;
    use serde::de::Error as DeError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize_c_table<S>(value: &[u64; 256], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_slice().serialize(serializer)
    }

    pub fn deserialize_c_table<'de, D>(deserializer: D) -> Result<[u64; 256], D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u64>::deserialize(deserializer)?;
        vec.try_into()
            .map_err(|vec: Vec<u64>| D::Error::invalid_length(vec.len(), &"256 elements"))
    }

    pub fn serialize_codes<S>(
        value: &[Option<HuffmanCode>; 256],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_slice().serialize(serializer)
    }

    pub fn deserialize_codes<'de, D>(
        deserializer: D,
    ) -> Result<[Option<HuffmanCode>; 256], D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<Option<HuffmanCode>>::deserialize(deserializer)?;
        vec.try_into().map_err(|vec: Vec<Option<HuffmanCode>>| {
            D::Error::invalid_length(vec.len(), &"256 elements")
        })
    }
}

pub const MAGIC_BYTES: u64 = 0x494E_4649_4752_414D; // "INFIGRAM" in hex
pub const CURRENT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardHeader {
    pub magic: u64,
    pub version: u32,
    pub text_len: u64,
    pub sa_sample_rate: u32,
    pub isa_sample_rate: u32,
    pub doc_offsets_count: u32,
    pub doc_offsets_l: u8,
    pub doc_offsets_u_bits_len: u64,

    // Core Metadata
    #[serde(
        serialize_with = "serde_arrays::serialize_c_table",
        deserialize_with = "serde_arrays::deserialize_c_table"
    )]
    pub c_table: [u64; 256], // Start index of each character in F-column
    #[serde(
        serialize_with = "serde_arrays::serialize_codes",
        deserialize_with = "serde_arrays::deserialize_codes"
    )]
    pub codes: [Option<HuffmanCode>; 256], // Huffman Codebook
    pub tree_shape: Vec<WaveletNodeShape>, // Wavelet Tree Topology

    // File Offsets (Pointers to Paged Data)
    pub wt_start_offset: u64, // Where the Wavelet Tree BitVectors begin
    pub sa_start_offset: u64, // Where the Sampled SA integers begin
    pub isa_start_offset: u64,

    // Encoded document offsets (Elias-Fano)
    pub doc_offsets_u_bits: Vec<u8>,
    pub doc_offsets_l_bits: Vec<u8>,
}

impl ShardHeader {
    pub fn new(
        text_len: u64,
        sa_sample_rate: u32,
        isa_sample_rate: u32,
        c_table: [u64; 256],
        codes: [Option<HuffmanCode>; 256],
        tree_shape: Vec<WaveletNodeShape>,
        doc_offsets: Vec<u64>,
    ) -> Self {
        let doc_offsets_count = doc_offsets.len() as u32;
        let (doc_offsets_l, doc_offsets_u_bits_len, doc_offsets_u_bits, doc_offsets_l_bits) =
            encode_doc_offsets_ef(&doc_offsets).unwrap_or((0, 0, Vec::new(), Vec::new()));
        Self {
            magic: MAGIC_BYTES,
            version: CURRENT_VERSION,
            text_len,
            sa_sample_rate,
            isa_sample_rate,
            doc_offsets_count,
            doc_offsets_l,
            doc_offsets_u_bits_len,
            c_table,
            codes,
            tree_shape,
            wt_start_offset: 0, // Placeholder, filled during write
            sa_start_offset: 0, // Placeholder
            isa_start_offset: 0,
            doc_offsets_u_bits,
            doc_offsets_l_bits,
        }
    }

    pub fn decode_doc_offsets(&self) -> io::Result<Vec<u64>> {
        decode_doc_offsets_ef(
            self.doc_offsets_count as usize,
            self.doc_offsets_l,
            self.doc_offsets_u_bits_len,
            &self.doc_offsets_u_bits,
            &self.doc_offsets_l_bits,
        )
    }
}

struct BitWriter {
    buf: Vec<u8>,
    current: u8,
    bit_in_byte: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            current: 0,
            bit_in_byte: 0,
        }
    }

    fn push_bit(&mut self, bit: bool) {
        if bit {
            self.current |= 1 << self.bit_in_byte;
        }
        self.bit_in_byte += 1;
        if self.bit_in_byte == 8 {
            self.buf.push(self.current);
            self.current = 0;
            self.bit_in_byte = 0;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bit_in_byte > 0 {
            self.buf.push(self.current);
        }
        self.buf
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    byte_idx: usize,
    bit_idx: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_idx: 0,
            bit_idx: 0,
        }
    }

    fn read_bit(&mut self) -> io::Result<bool> {
        if self.byte_idx >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bits",
            ));
        }
        let byte = self.data[self.byte_idx];
        let bit = (byte >> self.bit_idx) & 1 == 1;
        self.bit_idx += 1;
        if self.bit_idx == 8 {
            self.bit_idx = 0;
            self.byte_idx += 1;
        }
        Ok(bit)
    }
}

fn write_bits_lsb(val: u64, bits: u8, w: &mut BitWriter) {
    for i in 0..bits {
        w.push_bit(((val >> i) & 1) != 0);
    }
}

fn read_bits_lsb(r: &mut BitReader, bits: u8) -> io::Result<u64> {
    let mut val = 0u64;
    for i in 0..bits {
        if r.read_bit()? {
            val |= 1u64 << i;
        }
    }
    Ok(val)
}

fn encode_doc_offsets_ef(offsets: &[u64]) -> io::Result<(u8, u64, Vec<u8>, Vec<u8>)> {
    if offsets.is_empty() {
        return Ok((0, 0, Vec::new(), Vec::new()));
    }
    let mut prev = 0u64;
    for &off in offsets {
        if off < prev {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc_offsets must be non-decreasing",
            ));
        }
        prev = off;
    }

    let n = offsets.len() as u64;
    let max = *offsets.last().unwrap();
    let universe = max.saturating_add(1);
    let avg = if n > 0 { universe / n } else { 0 };
    let l = if avg > 0 { avg.ilog2() as u8 } else { 0 };

    let lower_mask = if l == 64 { u64::MAX } else { (1u64 << l) - 1 };

    let mut l_writer = BitWriter::new();
    let mut highs: Vec<u64> = Vec::with_capacity(offsets.len());
    for &off in offsets {
        let low = off & lower_mask;
        write_bits_lsb(low, l, &mut l_writer);
        highs.push(off >> l);
    }
    let l_bits = l_writer.finish();

    let high_last = *highs.last().unwrap_or(&0);
    let u_bits_len = high_last + n;
    let u_bytes_len = ((u_bits_len + 7) / 8) as usize;
    let mut u_bits = vec![0u8; u_bytes_len];
    for (i, &h) in highs.iter().enumerate() {
        let pos = h + i as u64;
        let byte = (pos / 8) as usize;
        let bit = (pos % 8) as u8;
        if byte < u_bits.len() {
            u_bits[byte] |= 1 << bit;
        }
    }

    Ok((l, u_bits_len, u_bits, l_bits))
}

fn decode_doc_offsets_ef(
    count: usize,
    l: u8,
    u_bits_len: u64,
    u_bits: &[u8],
    l_bits: &[u8],
) -> io::Result<Vec<u64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut lows = Vec::with_capacity(count);
    let mut lr = BitReader::new(l_bits);
    for _ in 0..count {
        lows.push(read_bits_lsb(&mut lr, l)?);
    }

    let mut highs = Vec::with_capacity(count);
    let mut ones_seen = 0usize;
    let total_bits = u_bits_len as usize;
    for pos in 0..total_bits {
        let byte = pos / 8;
        let bit = pos % 8;
        if byte >= u_bits.len() {
            break;
        }
        let is_one = (u_bits[byte] >> bit) & 1 == 1;
        if is_one {
            let high = pos as u64 - ones_seen as u64;
            highs.push(high);
            ones_seen += 1;
            if highs.len() == count {
                break;
            }
        }
    }

    if highs.len() != count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Elias-Fano upper bits",
        ));
    }

    let mut offsets = Vec::with_capacity(count);
    for i in 0..count {
        let off = (highs[i] << l) | lows[i];
        offsets.push(off);
    }
    Ok(offsets)
}
