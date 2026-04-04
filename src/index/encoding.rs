use serde::{Deserialize, Serialize};
use std::io;

pub const ALPHABET_SIZE: usize = 257;
pub const SENTINEL: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodingMode {
    Text,
    /// Legacy-only marker kept for backward header decoding.
    /// New builds do not use this mode.
    Binary,
}

impl Default for EncodingMode {
    fn default() -> Self {
        EncodingMode::Text
    }
}

pub fn encode_pattern_text(pattern: &[u8]) -> io::Result<Vec<u16>> {
    if pattern.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pattern contains 0 byte; 0 is reserved for the sentinel",
        ));
    }
    Ok(pattern.iter().map(|&b| b as u16).collect())
}

pub fn decode_symbol_for_extract_text(symbol: u16) -> io::Result<u8> {
    if symbol == SENTINEL {
        return Ok(0);
    }
    u8::try_from(symbol).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded symbol out of range for text mode",
        )
    })
}
