use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use cdivsufsort::sort as div_sort; // Using the cdivsufsort crate
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::index::bitpack;
use crate::index::encoding::{ALPHABET_SIZE, EncodingMode, SENTINEL, strategy_for};
use crate::index::external_sa;
use crate::index::header::{ShardHeader, ShardHeaderParams};
use crate::index::scratch;
use crate::index::wavelet::{
    WaveletBuildMode, canonical_codes, huffman_lengths, make_wavelet_build_strategy,
};

pub struct ShardBuilder {
    sample_rate: u32,
    encoding_mode: EncodingMode,
    wavelet_mode: WaveletBuildMode,
    scratch_dir: Option<PathBuf>,
}

const EXTERNAL_SA_THRESHOLD_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_EXTERNAL_SA_MEM_BYTES: usize = 256 * 1024 * 1024;

enum SaSource {
    InMemory(Vec<i32>),
    External(external_sa::SaStream),
}

impl ShardBuilder {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            encoding_mode: EncodingMode::Text,
            wavelet_mode: WaveletBuildMode::default(),
            scratch_dir: None,
        }
    }

    pub fn new_with_mode(sample_rate: u32, encoding_mode: EncodingMode) -> Self {
        Self {
            sample_rate,
            encoding_mode,
            wavelet_mode: WaveletBuildMode::default(),
            scratch_dir: None,
        }
    }

    pub fn new_with_modes(
        sample_rate: u32,
        encoding_mode: EncodingMode,
        wavelet_mode: WaveletBuildMode,
    ) -> Self {
        Self {
            sample_rate,
            encoding_mode,
            wavelet_mode,
            scratch_dir: None,
        }
    }

    pub fn with_scratch_dir<P: AsRef<Path>>(mut self, scratch_dir: P) -> Self {
        self.scratch_dir = Some(scratch_dir.as_ref().to_path_buf());
        self
    }

    /// Consumes a chunk of text and writes a complete .idx file
    pub fn build<P: AsRef<Path>>(&self, text: &[u8], output_path: P) -> io::Result<()> {
        let file = File::create(output_path)?;
        self.build_to_writer(text, file)
    }

    /// Consumes a chunk of text and writes a complete .idx stream.
    pub fn build_to_writer<W: Write>(&self, text: &[u8], writer: W) -> io::Result<()> {
        self.build_with_offsets_to_writer(text, vec![0], writer)
    }

    /// Consumes concatenated text + document offsets and writes a complete .idx file
    pub fn build_with_offsets<P: AsRef<Path>>(
        &self,
        text: &[u8],
        doc_offsets: Vec<u64>,
        output_path: P,
    ) -> io::Result<()> {
        let file = File::create(output_path)?;
        self.build_with_offsets_to_writer(text, doc_offsets, file)
    }

    /// Consumes concatenated text + document offsets and writes a complete .idx stream.
    pub fn build_with_offsets_to_writer<W: Write>(
        &self,
        text: &[u8],
        doc_offsets: Vec<u64>,
        writer: W,
    ) -> io::Result<()> {
        let encoder = strategy_for(self.encoding_mode);
        if self.sample_rate == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sample_rate must be > 0",
            ));
        }
        if doc_offsets.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc_offsets must contain at least one entry",
            ));
        }
        if doc_offsets[0] != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "doc_offsets must start at 0",
            ));
        }
        let mut prev = 0u64;
        for &off in &doc_offsets {
            if off < prev || off as usize > text.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "doc_offsets must be sorted and within text length",
                ));
            }
            prev = off;
        }
        if text.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "text must be non-empty",
            ));
        }
        let encoded = encoder.encode_text(text)?;

        self.build_encoded_to_writer(&encoded, doc_offsets, writer)
    }

    fn build_encoded_to_writer<W: Write>(
        &self,
        text: &[u16],
        doc_offsets: Vec<u64>,
        writer: W,
    ) -> io::Result<()> {
        if text.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "text must be non-empty",
            ));
        }
        if *text.last().unwrap() != SENTINEL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "text must end with a 0 sentinel",
            ));
        }
        if text[..text.len() - 1].contains(&SENTINEL) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "text contains 0 symbol before sentinel; separators are not supported",
            ));
        }

        let mut writer = std::io::BufWriter::new(writer);

        // 1. Compute Suffix Array (Heavy Computation)
        let sa_source = build_sa_source(text, self.encoding_mode, self.scratch_dir.as_deref())?;

        // 2. Build BWT + samples using an external-memory pipeline
        // BWT[i] = Text[SA[i] - 1] (cyclic)
        // We stream BWT into a temp file and avoid materializing SA/BWT/ISA in memory.
        let len = text.len();

        let sample_rate = self.sample_rate as usize;
        let sa_len = len.div_ceil(sample_rate);
        let isa_len = len.div_ceil(sample_rate);

        let mut sa_samples: Vec<u64> = Vec::with_capacity(sa_len);
        let mut isa_samples: Vec<u64> = vec![0u64; isa_len];

        let mut counts = [0u64; ALPHABET_SIZE];
        let mut bwt_file = scratch::named_temp_file(self.scratch_dir.as_deref())?;
        {
            let mut writer = BufWriter::new(bwt_file.as_file_mut());
            let mut buffer: Vec<u16> = Vec::with_capacity(4 * 1024 * 1024);

            match sa_source {
                SaSource::InMemory(ref sa_i32) => {
                    for (row_idx, &sa_val) in sa_i32.iter().enumerate() {
                        let pos = sa_val as usize;

                        if row_idx % sample_rate == 0 {
                            sa_samples.push(pos as u64);
                        }
                        if pos % sample_rate == 0 {
                            let idx = pos / sample_rate;
                            if idx < isa_samples.len() {
                                isa_samples[idx] = row_idx as u64;
                            }
                        }

                        let bwt_sym = if pos == 0 {
                            text[len - 1]
                        } else {
                            text[pos - 1]
                        };
                        counts[bwt_sym as usize] += 1;
                        buffer.push(bwt_sym);

                        if buffer.len() >= 4 * 1024 * 1024 {
                            for sym in buffer.drain(..) {
                                writer.write_u16::<LittleEndian>(sym)?;
                            }
                        }
                    }
                }
                SaSource::External(ref stream) => {
                    let iter = stream.iter()?;
                    for (row_idx, sa_val) in iter.enumerate() {
                        let pos = sa_val? as usize;

                        if row_idx % sample_rate == 0 {
                            sa_samples.push(pos as u64);
                        }
                        if pos % sample_rate == 0 {
                            let idx = pos / sample_rate;
                            if idx < isa_samples.len() {
                                isa_samples[idx] = row_idx as u64;
                            }
                        }

                        let bwt_sym = if pos == 0 {
                            text[len - 1]
                        } else {
                            text[pos - 1]
                        };
                        counts[bwt_sym as usize] += 1;
                        buffer.push(bwt_sym);

                        if buffer.len() >= 4 * 1024 * 1024 {
                            for sym in buffer.drain(..) {
                                writer.write_u16::<LittleEndian>(sym)?;
                            }
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                for sym in buffer.drain(..) {
                    writer.write_u16::<LittleEndian>(sym)?;
                }
            }
            writer.flush()?;
        }

        if sa_samples.len() != sa_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SA sample length mismatch",
            ));
        }

        // 3. Compute C-Table
        // C[x] = total count of characters lexicographically smaller than x
        let mut c_table = [0u64; ALPHABET_SIZE];
        let mut sum = 0;
        for i in 0..ALPHABET_SIZE {
            c_table[i] = sum;
            sum += counts[i];
        }

        // 4. Build Wavelet Tree using selected strategy
        let lens = huffman_lengths(&counts);
        let codes = canonical_codes(&lens);
        let codes_for_header = codes;
        let wavelet_strategy = make_wavelet_build_strategy(
            self.wavelet_mode,
            codes,
            &counts,
            &bwt_file,
            self.scratch_dir.as_deref(),
        )?;
        let tree_shape = wavelet_strategy.tree_shape().to_vec();
        let wavelet_bytes = wavelet_strategy.wavelet_bytes();

        let can_pack_u32 = len <= u32::MAX as usize;

        let mut sa_bits: u8 = 0;
        let mut isa_bits: u8 = 0;
        let mut sa_packed: Option<Vec<u32>> = None;
        let mut isa_packed: Option<Vec<u32>> = None;
        let mut sa_packed_u64: Option<Vec<u64>> = None;
        let mut isa_packed_u64: Option<Vec<u64>> = None;

        if sa_len > 0 && can_pack_u32 {
            let mut samples = Vec::with_capacity(sa_len);
            for &sa_val in &sa_samples {
                samples.push(sa_val as u32);
            }

            let w = bitpack::required_bits_u32(&samples) as u8;
            let words = (samples.len() * w as usize).div_ceil(32);
            let mut packed = vec![0u32; words.max(1)];
            let (packed_w, written) = bitpack::pack_u32_dynamic(&samples, &mut packed);
            packed.truncate(written);
            sa_bits = packed_w as u8;
            sa_packed = Some(packed);
        }

        if isa_len > 0 && can_pack_u32 {
            let mut samples = Vec::with_capacity(isa_len);
            for &isa_val in &isa_samples {
                samples.push(isa_val as u32);
            }

            let w = bitpack::required_bits_u32(&samples) as u8;
            let words = (samples.len() * w as usize).div_ceil(32);
            let mut packed = vec![0u32; words.max(1)];
            let (packed_w, written) = bitpack::pack_u32_dynamic(&samples, &mut packed);
            packed.truncate(written);
            isa_bits = packed_w as u8;
            isa_packed = Some(packed);
        }

        if sa_len > 0 && !can_pack_u32 {
            let w = bitpack::required_bits_u64(&sa_samples);
            if w < 64 {
                let words = (sa_samples.len() * w).div_ceil(64);
                let mut packed = vec![0u64; words.max(1)];
                let (packed_w, written) = bitpack::pack_u64_dynamic(&sa_samples, &mut packed);
                packed.truncate(written);
                sa_bits = packed_w as u8;
                sa_packed_u64 = Some(packed);
            } else {
                sa_bits = 0;
            }
        }

        if isa_len > 0 && !can_pack_u32 {
            let w = bitpack::required_bits_u64(&isa_samples);
            if w < 64 {
                let words = (isa_samples.len() * w).div_ceil(64);
                let mut packed = vec![0u64; words.max(1)];
                let (packed_w, written) = bitpack::pack_u64_dynamic(&isa_samples, &mut packed);
                packed.truncate(written);
                isa_bits = packed_w as u8;
                isa_packed_u64 = Some(packed);
            } else {
                isa_bits = 0;
            }
        }

        // Prepare Header (with placeholder offsets)
        let mut header = ShardHeader::new(ShardHeaderParams {
            encoding_mode: self.encoding_mode,
            text_len: len as u64,
            sa_sample_rate: self.sample_rate,
            isa_sample_rate: self.sample_rate, // Use same rate for ISA
            sa_bits,
            isa_bits,
            c_table,
            codes: codes_for_header,
            tree_shape: tree_shape.clone(),
            doc_offsets,
        });

        let config = bincode::config::legacy();
        let header_bytes = bincode::serde::encode_to_vec(&header, config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let header_size = header_bytes.len() as u64;

        // Compute offsets for SA/ISA
        let sa_bytes_len = if sa_bits == 0 {
            sa_len as u64 * 8
        } else if sa_bits <= 32 {
            sa_packed.as_ref().map(|v| v.len() as u64 * 4).unwrap_or(0)
        } else {
            sa_packed_u64
                .as_ref()
                .map(|v| v.len() as u64 * 8)
                .unwrap_or(0)
        };

        let _isa_bytes_len = if isa_bits == 0 {
            isa_len as u64 * 8
        } else if isa_bits <= 32 {
            isa_packed.as_ref().map(|v| v.len() as u64 * 4).unwrap_or(0)
        } else {
            isa_packed_u64
                .as_ref()
                .map(|v| v.len() as u64 * 8)
                .unwrap_or(0)
        };

        header.tree_shape = tree_shape;
        header.wt_start_offset = header_size;
        header.sa_start_offset = header_size + wavelet_bytes;
        header.isa_start_offset = header.sa_start_offset + sa_bytes_len;

        let final_header_bytes = bincode::serde::encode_to_vec(&header, config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if final_header_bytes.len() != header_bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header size changed while finalizing",
            ));
        }

        // 6. Write Header + Wavelet Tree
        writer.write_all(&final_header_bytes)?;
        wavelet_strategy.write_to(&bwt_file, &mut writer)?;

        // 7. Write Sampled Suffix Array (SA)
        if sa_bits == 0 {
            let mut int_buffer = [0u8; 8]; // Buffer for u64
            for &sa_val in &sa_samples {
                LittleEndian::write_u64(&mut int_buffer, sa_val);
                writer.write_all(&int_buffer)?;
            }
        } else if sa_bits <= 32 {
            if let Some(packed) = sa_packed.as_ref() {
                let mut buf = [0u8; 4];
                for &word in packed {
                    LittleEndian::write_u32(&mut buf, word);
                    writer.write_all(&buf)?;
                }
            }
        } else if let Some(packed) = sa_packed_u64.as_ref() {
            let mut buf = [0u8; 8];
            for &word in packed {
                LittleEndian::write_u64(&mut buf, word);
                writer.write_all(&buf)?;
            }
        }

        // 8. Write Sampled Inverse Suffix Array (ISA)
        if isa_bits == 0 {
            let mut int_buffer = [0u8; 8]; // Buffer for u64
            for &isa_val in &isa_samples {
                LittleEndian::write_u64(&mut int_buffer, isa_val);
                writer.write_all(&int_buffer)?;
            }
        } else if isa_bits <= 32 {
            if let Some(packed) = isa_packed.as_ref() {
                let mut buf = [0u8; 4];
                for &word in packed {
                    LittleEndian::write_u32(&mut buf, word);
                    writer.write_all(&buf)?;
                }
            }
        } else if let Some(packed) = isa_packed_u64.as_ref() {
            let mut buf = [0u8; 8];
            for &word in packed {
                LittleEndian::write_u64(&mut buf, word);
                writer.write_all(&buf)?;
            }
        }

        writer.flush()?;
        Ok(())
    }
}

fn build_sa_source(
    text: &[u16],
    encoding_mode: EncodingMode,
    scratch_dir: Option<&Path>,
) -> io::Result<SaSource> {
    if encoding_mode == EncodingMode::Binary || should_use_external_sa(text.len()) {
        let mem_limit = external_sa_mem_limit();
        let stream = external_sa::build_sa_external_with_scratch(text, mem_limit, scratch_dir)?;
        return Ok(SaSource::External(stream));
    }

    let mut text_u8 = Vec::with_capacity(text.len());
    for &sym in text {
        let b = u8::try_from(sym).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "text symbol out of range for byte-based SA",
            )
        })?;
        text_u8.push(b);
    }

    // cdivsufsort returns Vec<i32>
    let (_, sa_i32) = div_sort(&text_u8).into_parts();
    Ok(SaSource::InMemory(sa_i32))
}

fn should_use_external_sa(len: usize) -> bool {
    if let Ok(value) = env::var("FM_INDEX_EXTERNAL_SA") {
        return value == "1" || value.eq_ignore_ascii_case("true");
    }
    len >= EXTERNAL_SA_THRESHOLD_BYTES
}

fn external_sa_mem_limit() -> usize {
    if let Ok(value) = env::var("FM_INDEX_EXTERNAL_SA_MEM_BYTES") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed.max(1);
        }
    }
    DEFAULT_EXTERNAL_SA_MEM_BYTES
}
