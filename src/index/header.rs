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

    // Encoded document offsets (monotonic, delta + Elias gamma)
    pub doc_offsets_encoded: Vec<u8>,
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
        let doc_offsets_encoded =
            encode_doc_offsets(&doc_offsets).unwrap_or_else(|_| Vec::new());
        Self {
            magic: MAGIC_BYTES,
            version: CURRENT_VERSION,
            text_len,
            sa_sample_rate,
            isa_sample_rate,
            doc_offsets_count,
            c_table,
            codes,
            tree_shape,
            wt_start_offset: 0, // Placeholder, filled during write
            sa_start_offset: 0, // Placeholder
            isa_start_offset: 0,
            doc_offsets_encoded,
        }
    }

    pub fn decode_doc_offsets(&self) -> io::Result<Vec<u64>> {
        decode_doc_offsets(&self.doc_offsets_encoded, self.doc_offsets_count as usize)
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

fn gamma_encode_u64(n: u64, w: &mut BitWriter) {
    debug_assert!(n >= 1);
    let len = 63 - n.leading_zeros() as u8;
    for _ in 0..len {
        w.push_bit(false);
    }
    for i in (0..=len).rev() {
        w.push_bit(((n >> i) & 1) != 0);
    }
}

fn gamma_decode_u64(r: &mut BitReader) -> io::Result<u64> {
    let mut zeros = 0u32;
    while !r.read_bit()? {
        zeros += 1;
    }
    let mut value = 1u64 << zeros;
    for i in (0..zeros).rev() {
        if r.read_bit()? {
            value |= 1u64 << i;
        }
    }
    Ok(value)
}

fn encode_doc_offsets(offsets: &[u64]) -> io::Result<Vec<u8>> {
    if offsets.is_empty() {
        return Ok(Vec::new());
    }
    let mut writer = BitWriter::new();
    let mut prev = 0u64;
    for &off in offsets {
        if off < prev {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc_offsets must be non-decreasing",
            ));
        }
        let delta = off - prev;
        let code = delta + 1; // gamma requires >= 1
        gamma_encode_u64(code, &mut writer);
        prev = off;
    }
    Ok(writer.finish())
}

fn decode_doc_offsets(encoded: &[u8], count: usize) -> io::Result<Vec<u64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(encoded);
    let mut offsets = Vec::with_capacity(count);
    let mut prev = 0u64;
    for _ in 0..count {
        let code = gamma_decode_u64(&mut reader)?;
        if code == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid gamma code",
            ));
        }
        let delta = code - 1;
        let off = prev + delta;
        offsets.push(off);
        prev = off;
    }
    Ok(offsets)
}
