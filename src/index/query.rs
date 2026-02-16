use crate::index::header::ShardHeader;
use crate::index::sampled_sa::PagedSampledSA;
use crate::index::wavelet::PagedWaveletTree;
use crate::iolib::paged_reader::PagedReader;
use std::io;

pub struct QueryEngine {
    header: ShardHeader,
    wt: PagedWaveletTree,
    sa: PagedSampledSA,
    isa: PagedSampledSA,
    doc_offsets: Vec<u64>,
    text_len: usize,
}

impl QueryEngine {
    pub fn new(header: ShardHeader, reader: PagedReader) -> Self {
        let wt = PagedWaveletTree::new(
            reader.clone(),
            header.tree_shape.clone(),
            header.codes,
            header.text_len as usize,
            header.wt_start_offset,
        );

        // Initialize Sampled SA Reader
        let sa_len = (header.text_len as usize + header.sa_sample_rate as usize - 1)
            / header.sa_sample_rate as usize;
        let sa = PagedSampledSA::new(reader.clone(), sa_len, header.sa_start_offset);

        // Initialize Sampled ISA Reader
        let isa_len = (header.text_len as usize + header.isa_sample_rate as usize - 1)
            / header.isa_sample_rate as usize;
        let isa = PagedSampledSA::new(reader.clone(), isa_len, header.isa_start_offset);

        let mut doc_offsets = header.decode_doc_offsets().unwrap_or_else(|_| Vec::new());
        if doc_offsets.is_empty() {
            doc_offsets.push(0);
        }

        Self {
            text_len: header.text_len as usize,
            header,
            wt,
            sa,
            isa,
            doc_offsets,
        }
    }

    /// Count occurrences of a pattern.
    /// Returns the range [sp, ep] in the suffix array rows.
    /// If sp > ep, pattern is not found.
    pub fn count(&self, pattern: &[u8]) -> io::Result<(usize, usize)> {
        let mut sp = 0;
        let mut ep = self.text_len - 1;

        // Backward search
        for &char_byte in pattern.iter().rev() {
            let c = char_byte as usize;

            // Get start of char range in F-column
            let c_start = self.header.c_table[c] as usize;

            // Check if char exists in text
            let c_next = if c < 255 {
                self.header.c_table[c + 1] as usize
            } else {
                self.text_len
            };
            if c_start >= c_next {
                return Ok((1, 0)); // Not found
            }

            // LF Mapping steps
            let rank_start = if sp == 0 {
                0
            } else {
                self.wt.rank(char_byte, sp)?
            };
            let rank_end = self.wt.rank(char_byte, ep + 1)?;

            sp = c_start + rank_start;
            ep = c_start + rank_end - 1;

            if sp > ep {
                return Ok((1, 0));
            }
        }

        Ok((sp, ep))
    }

    /// Locate all occurrences of a pattern.
    /// Returns a list of byte offsets in the original text.
    pub fn locate(&self, pattern: &[u8]) -> io::Result<Vec<usize>> {
        let (sp, ep) = self.count(pattern)?;
        if sp > ep {
            return Ok(Vec::new());
        }

        let mut locations = Vec::with_capacity(ep - sp + 1);
        for row in sp..=ep {
            locations.push(self.resolve_sa(row)?);
        }
        Ok(locations)
    }

    /// Extract a snippet of text from the index.
    /// Reconstructs original text[start .. start+len]
    pub fn extract(&self, start: usize, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if start + len > self.text_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Range out of bounds",
            ));
        }

        let mut result = vec![0u8; len];

        // Start extracting from the END of the requested segment.
        // We need the BWT row corresponding to text position `start + len`.
        // Because BWT[row] gives us T[start + len - 1].

        let mut curr_row = self.get_isa(start + len)?;

        for i in (0..len).rev() {
            // Get char at this row (which is the preceding char in text)
            let c = self.wt.access(curr_row)?;
            result[i] = c;

            // Walk LF to move to the row for this char
            curr_row = self.lf_step(curr_row)?;
        }

        Ok(result)
    }

    // --- Helpers ---

    /// Resolves the Suffix Array value for a specific row in the BWT matrix.
    fn resolve_sa(&self, mut row: usize) -> io::Result<usize> {
        let sample_rate = self.header.sa_sample_rate as usize;
        let mut steps = 0;

        while row % sample_rate != 0 {
            // LF Step: Go backwards in text
            let c = self.wt.access(row)?;
            let c_idx = c as usize;
            let c_start = self.header.c_table[c_idx] as usize;
            let rank = self.wt.rank(c, row)?;

            row = c_start + rank;
            steps += 1;
        }

        let sample_idx = row / sample_rate;
        let sa_val = self.sa.get(sample_idx)?;

        Ok((sa_val as usize + steps) % self.text_len)
    }

    /// Helper: Get ISA[i] (The BWT row corresponding to text position i)
    fn get_isa(&self, i: usize) -> io::Result<usize> {
        let rate = self.header.isa_sample_rate as usize;
        if rate == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ISA sample rate is zero",
            ));
        }
        let mut steps = 0;
        let mut curr = if i >= self.text_len { 0 } else { i };

        // 1. Find nearest sampled future position
        // Since we walk backwards easily with LF, we need a future sampled ISA value
        // to walk back TO our target.
        // Wait: LF(ISA[i+1]) = ISA[i].
        // So if we find ISA[i+k], we can apply LF k times to get ISA[i].

        while curr % rate != 0 {
            curr += 1;
            steps += 1;
            if curr >= self.text_len {
                curr = 0; // Wrap to sentinel
            }
        }

        // 2. Read sampled value
        let sample_idx = curr / rate;

        // Safety: sample_idx might be out of bounds if curr wrapped to 0 and rate=N?
        // sample_idx should be valid.
        let mut row = self.isa.get(sample_idx)? as usize;

        // 3. Walk LF mapping `steps` times to get back to ISA[i]
        for _ in 0..steps {
            row = self.lf_step(row)?;
        }

        Ok(row)
    }

    fn lf_step(&self, row: usize) -> io::Result<usize> {
        let c = self.wt.access(row)?;
        let c_idx = c as usize;
        let c_start = self.header.c_table[c_idx] as usize;
        let rank = self.wt.rank(c, row)?;
        Ok(c_start + rank)
    }

    /// Convert a global byte offset to a document id and offset within that doc.
    pub fn pos_to_doc_id(&self, pos: usize) -> Option<(usize, usize)> {
        if pos >= self.text_len {
            return None;
        }
        let pos_u64 = pos as u64;
        let idx = match self.doc_offsets.binary_search(&pos_u64) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        let doc_start = *self.doc_offsets.get(idx)?;
        Some((idx, pos - doc_start as usize))
    }

    /// Reconstruct an entire document by id.
    pub fn get_document(&self, doc_id: usize) -> io::Result<Vec<u8>> {
        if doc_id >= self.doc_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DocID out of range",
            ));
        }

        let start = self.doc_offsets[doc_id] as usize;
        let end = if doc_id + 1 < self.doc_offsets.len() {
            self.doc_offsets[doc_id + 1] as usize
        } else {
            self.text_len
        };

        let mut bytes = self.extract(start, end - start)?;
        if let Some(&0) = bytes.last() {
            bytes.pop();
        }
        Ok(bytes)
    }
}
