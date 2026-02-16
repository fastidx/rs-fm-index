use crate::index::wavelet::{HuffmanCode, WaveletNodeShape};
use serde::{Deserialize, Serialize};

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
}

impl ShardHeader {
    pub fn new(
        text_len: u64,
        sa_sample_rate: u32,
        c_table: [u64; 256],
        codes: [Option<HuffmanCode>; 256],
        tree_shape: Vec<WaveletNodeShape>,
    ) -> Self {
        Self {
            magic: MAGIC_BYTES,
            version: CURRENT_VERSION,
            text_len,
            sa_sample_rate,
            c_table,
            codes,
            tree_shape,
            wt_start_offset: 0, // Placeholder, filled during write
            sa_start_offset: 0, // Placeholder
        }
    }
}
