use byteorder::{ByteOrder, LittleEndian};
use cdivsufsort::sort as div_sort; // Using the cdivsufsort crate
use std::fs::File;
use std::io::{self, Cursor, Write};
use std::path::Path;

use crate::index::bitpack;
use crate::index::header::ShardHeader;
use crate::index::wavelet::WaveletTreeBuilder;

pub struct ShardBuilder {
    sample_rate: u32,
}

impl ShardBuilder {
    pub fn new(sample_rate: u32) -> Self {
        Self { sample_rate }
    }

    /// Consumes a chunk of text and writes a complete .idx file
    pub fn build<P: AsRef<Path>>(&self, text: &[u8], output_path: P) -> io::Result<()> {
        self.build_with_offsets(text, vec![0], output_path)
    }

    /// Consumes concatenated text + document offsets and writes a complete .idx file
    pub fn build_with_offsets<P: AsRef<Path>>(
        &self,
        text: &[u8],
        doc_offsets: Vec<u64>,
        output_path: P,
    ) -> io::Result<()> {
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

        let mut writer = std::io::BufWriter::new(File::create(output_path)?);

        // 1. Compute Suffix Array (Heavy Computation)
        // cdivsufsort returns Vec<i32>
        let (_, sa_i32) = div_sort(text).into_parts();

        // 2. Build BWT
        // BWT[i] = Text[SA[i] - 1] (cyclic)
        // We build this in memory. For 1GB text, this is 1GB BWT.
        let len = text.len();
        let mut bwt = Vec::with_capacity(len);
        let mut sa_u64 = Vec::with_capacity(len); // Keep SA for sampling

        // NEW: ISA Vector
        // We need the full ISA in memory to sample it, or we can construct it
        // sparsely if we iterate carefuly. But for 1GB chunks, efficient
        // random access construction is needed.
        // ISA[SA[i]] = i.
        let mut isa_u64 = vec![0u64; len];

        for (row_idx, &sa_val) in sa_i32.iter().enumerate() {
            let pos = sa_val as usize; // Cast i32 -> usize
            sa_u64.push(pos as u64); // Store as u64 for index

            // Build ISA: Map "Text Position" -> "BWT Row Index"
            isa_u64[pos] = row_idx as u64;

            if pos == 0 {
                // In BWT, the char "before" the start is the last char
                // But typically BWT algorithms append a sentinel $.
                // If we assume the input text HAS a sentinel (0 bytes), we use cyclic logic.
                // Text[len-1]
                bwt.push(text[len - 1]);
            } else {
                bwt.push(text[pos - 1]);
            }
        }

        // 3. Compute C-Table
        // C[x] = total count of characters lexicographically smaller than x
        let mut counts = [0u64; 256];
        for &b in &bwt {
            counts[b as usize] += 1;
        }
        let mut c_table = [0u64; 256];
        let mut sum = 0;
        for i in 0..256 {
            c_table[i] = sum;
            sum += counts[i];
        }

        // 4. Initialize Wavelet Tree Builder
        let mut wt_builder = WaveletTreeBuilder::new(&bwt);
        wt_builder.process_text(&bwt);
        let codes = wt_builder.codes; // Save codes for header

        // 5. Write Wavelet Tree to a buffer first so we can size the header correctly
        let mut wt_buf = Cursor::new(Vec::new());
        let (_wt_offset, tree_shape) = wt_builder.write_to_file(&mut wt_buf)?;
        let wt_bytes = wt_buf.into_inner();

        // Compute sample lengths
        let sa_len = (len + self.sample_rate as usize - 1) / self.sample_rate as usize;
        let isa_len = (len + self.sample_rate as usize - 1) / self.sample_rate as usize;

        let can_pack_u32 = len <= u32::MAX as usize;

        let mut sa_bits: u8 = 0;
        let mut isa_bits: u8 = 0;
        let mut sa_packed: Option<Vec<u32>> = None;
        let mut isa_packed: Option<Vec<u32>> = None;
        let mut sa_packed_u64: Option<Vec<u64>> = None;
        let mut isa_packed_u64: Option<Vec<u64>> = None;

        if sa_len > 0 && can_pack_u32 {
            let mut samples = Vec::with_capacity(sa_len);
            for (i, &sa_val) in sa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    samples.push(sa_val as u32);
                }
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
            for (i, &isa_val) in isa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    samples.push(isa_val as u32);
                }
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
            let mut samples = Vec::with_capacity(sa_len);
            for (i, &sa_val) in sa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    samples.push(sa_val);
                }
            }

            let w = bitpack::required_bits_u64(&samples);
            if w < 64 {
                let words = (samples.len() * w).div_ceil(64);
                let mut packed = vec![0u64; words.max(1)];
                let (packed_w, written) = bitpack::pack_u64_dynamic(&samples, &mut packed);
                packed.truncate(written);
                sa_bits = packed_w as u8;
                sa_packed_u64 = Some(packed);
            } else {
                sa_bits = 0;
            }
        }

        if isa_len > 0 && !can_pack_u32 {
            let mut samples = Vec::with_capacity(isa_len);
            for (i, &isa_val) in isa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    samples.push(isa_val);
                }
            }

            let w = bitpack::required_bits_u64(&samples);
            if w < 64 {
                let words = (samples.len() * w).div_ceil(64);
                let mut packed = vec![0u64; words.max(1)];
                let (packed_w, written) = bitpack::pack_u64_dynamic(&samples, &mut packed);
                packed.truncate(written);
                isa_bits = packed_w as u8;
                isa_packed_u64 = Some(packed);
            } else {
                isa_bits = 0;
            }
        }

        // Prepare Header (with placeholder offsets)
        let mut header = ShardHeader::new(
            len as u64,
            self.sample_rate,
            self.sample_rate, // Use same rate for ISA
            sa_bits,
            isa_bits,
            c_table,
            codes,
            tree_shape.clone(),
            doc_offsets,
        );

        let config = bincode::config::legacy();
        let header_bytes = bincode::serde::encode_to_vec(&header, config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let header_size = header_bytes.len() as u64;

        // Compute offsets for SA/ISA
        let sa_bytes_len = if sa_bits == 0 {
            sa_len as u64 * 8
        } else if sa_bits <= 32 {
            sa_packed
                .as_ref()
                .map(|v| v.len() as u64 * 4)
                .unwrap_or(0)
        } else {
            sa_packed_u64
                .as_ref()
                .map(|v| v.len() as u64 * 8)
                .unwrap_or(0)
        };

        let _isa_bytes_len = if isa_bits == 0 {
            isa_len as u64 * 8
        } else if isa_bits <= 32 {
            isa_packed
                .as_ref()
                .map(|v| v.len() as u64 * 4)
                .unwrap_or(0)
        } else {
            isa_packed_u64
                .as_ref()
                .map(|v| v.len() as u64 * 8)
                .unwrap_or(0)
        };

        header.tree_shape = tree_shape;
        header.wt_start_offset = header_size;
        header.sa_start_offset = header_size + wt_bytes.len() as u64;
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
        writer.write_all(&wt_bytes)?;

        // 7. Write Sampled Suffix Array (SA)
        if sa_bits == 0 {
            let mut int_buffer = [0u8; 8]; // Buffer for u64
            for (i, &sa_val) in sa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    LittleEndian::write_u64(&mut int_buffer, sa_val);
                    writer.write_all(&int_buffer)?;
                }
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
            for (i, &isa_val) in isa_u64.iter().enumerate() {
                if i % (self.sample_rate as usize) == 0 {
                    LittleEndian::write_u64(&mut int_buffer, isa_val);
                    writer.write_all(&int_buffer)?;
                }
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
