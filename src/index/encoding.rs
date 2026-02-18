use serde::{Deserialize, Serialize};
use std::io;

pub const ALPHABET_SIZE: usize = 257;
pub const SENTINEL: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodingMode {
    Text,
    Binary,
}

impl Default for EncodingMode {
    fn default() -> Self {
        EncodingMode::Text
    }
}

pub trait EncodingStrategy {
    fn mode(&self) -> EncodingMode;
    fn encode_text(&self, input: &[u8]) -> io::Result<Vec<u16>>;
    fn encode_pattern(&self, pattern: &[u8]) -> io::Result<Vec<u16>>;
    fn decode_symbol_for_extract(&self, symbol: u16) -> io::Result<u8>;
}

#[derive(Debug, Clone, Copy)]
pub struct TextEncoding;

#[derive(Debug, Clone, Copy)]
pub struct BinaryEncoding;

impl EncodingStrategy for TextEncoding {
    fn mode(&self) -> EncodingMode {
        EncodingMode::Text
    }

    fn encode_text(&self, input: &[u8]) -> io::Result<Vec<u16>> {
        if input.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "input contains 0 byte; cannot use 0 as sentinel",
            ));
        }
        let mut out = Vec::with_capacity(input.len() + 1);
        out.extend(input.iter().map(|&b| b as u16));
        out.push(SENTINEL);
        Ok(out)
    }

    fn encode_pattern(&self, pattern: &[u8]) -> io::Result<Vec<u16>> {
        if pattern.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pattern contains 0 byte; 0 is reserved for the sentinel",
            ));
        }
        Ok(pattern.iter().map(|&b| b as u16).collect())
    }

    fn decode_symbol_for_extract(&self, symbol: u16) -> io::Result<u8> {
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
}

impl EncodingStrategy for BinaryEncoding {
    fn mode(&self) -> EncodingMode {
        EncodingMode::Binary
    }

    fn encode_text(&self, input: &[u8]) -> io::Result<Vec<u16>> {
        let mut out = Vec::with_capacity(input.len() + 1);
        out.extend(input.iter().map(|&b| b as u16 + 1));
        out.push(SENTINEL);
        Ok(out)
    }

    fn encode_pattern(&self, pattern: &[u8]) -> io::Result<Vec<u16>> {
        Ok(pattern.iter().map(|&b| b as u16 + 1).collect())
    }

    fn decode_symbol_for_extract(&self, symbol: u16) -> io::Result<u8> {
        if symbol == SENTINEL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "attempted to extract sentinel byte in binary mode",
            ));
        }
        let val = symbol
            .checked_sub(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid symbol"))?;
        u8::try_from(val).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded symbol out of range for binary mode",
            )
        })
    }
}

static TEXT_ENCODING: TextEncoding = TextEncoding;
static BINARY_ENCODING: BinaryEncoding = BinaryEncoding;

pub fn strategy_for(mode: EncodingMode) -> &'static dyn EncodingStrategy {
    match mode {
        EncodingMode::Text => &TEXT_ENCODING,
        EncodingMode::Binary => &BINARY_ENCODING,
    }
}
