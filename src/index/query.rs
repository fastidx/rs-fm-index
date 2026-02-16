use crate::index::header::ShardHeader;
use crate::index::sampled_sa::PagedSampledSA;
use crate::index::wavelet::PagedWaveletTree;
use crate::iolib::paged_reader::PagedReader;
use std::io;

pub struct QueryEngine {
    header: ShardHeader,
    wt: PagedWaveletTree,
    sa: PagedSampledSA,
    text_len: usize,
}

impl QueryEngine {
    pub fn new(header: ShardHeader, reader: PagedReader) -> Self {
        let wt = PagedWaveletTree::new(
            reader.clone(),
            header.tree_shape.clone(),
            header.codes, // This is Copy
            header.text_len as usize,
        );

        let sa_len = (header.text_len as usize + header.sa_sample_rate as usize - 1)
            / header.sa_sample_rate as usize;

        let sa = PagedSampledSA::new(reader.clone(), sa_len, header.sa_start_offset);

        Self {
            text_len: header.text_len as usize,
            header,
            wt,
            sa,
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

            // Check if char exists in C-table
            // We need to know the start of the 'c' range in the F-column.
            // c_table[c] gives the count of chars strictly smaller than c.
            // This is exactly the start index.
            let c_start = self.header.c_table[c] as usize;

            // If the range for this char is empty in F-column (next char starts at same spot)
            let c_next = if c < 255 {
                self.header.c_table[c + 1] as usize
            } else {
                self.text_len
            };
            if c_start >= c_next {
                return Ok((1, 0)); // Not found
            }

            // LF Mapping steps:
            // sp = C[c] + Rank(c, sp - 1)
            // ep = C[c] + Rank(c, ep) - 1

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

    /// Resolves the Suffix Array value for a specific row in the BWT matrix.
    /// If the row is not sampled, we walk the LF-mapping until we hit a sampled row.
    fn resolve_sa(&self, mut row: usize) -> io::Result<usize> {
        let sample_rate = self.header.sa_sample_rate as usize;
        let mut steps = 0;

        // Walk LF-mapping until we hit a sampled index
        // or we hit the Sentinel ($).
        // Since we don't explicitly store $, we assume row 0 is sentinel if using sorted cyclic shift?
        // Wait, cdivsufsort output: row 0 is usually the sentinel if it was in input.
        // Let's rely on the sampling logic.

        while row % sample_rate != 0 {
            // LF(row) = C[BWT[row]] + Rank(BWT[row], row)

            // 1. Get char at BWT[row]
            let c = self.wt.access(row)?;

            // 2. Perform LF Map
            let c_idx = c as usize;
            let c_start = self.header.c_table[c_idx] as usize;
            let rank = self.wt.rank(c, row)?;

            // If we hit the sentinel logic (depending on rotation), we might need special handling.
            // But mathematically: SA[row] = SA[LF(row)] + 1
            row = c_start + rank;
            steps += 1;
        }

        // We are at a sampled row
        let sample_idx = row / sample_rate;
        let sa_val = self.sa.get(sample_idx)?;

        // The result is the sampled value + number of steps we walked "backwards"
        // (conceptually forward in text)
        // Wait, LF mapping walks *backwards* in text.
        // BWT[i] is the character *preceding* the suffix at i.
        // So LF(i) is the row starting with that preceding character.
        // This means LF step goes BACKWARDS in text.
        // SA[i] = SA[LF(i)] + 1.
        // Therefore SA[original] = SA[current] + steps.

        // However, we handle the cyclic wrap-around if steps wrap.
        // For standard text search, we usually ignore wrap logic if we just want offsets.
        // Correct logic:
        Ok((sa_val as usize + steps) % self.text_len)
    }
}
