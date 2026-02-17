use crate::iolib::paged_reader::PagedReader;
use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::mem::size_of;

/// A read-only view of a Sampled Suffix Array stored on disk.
/// Accesses are cached via the underlying PagedReader.
pub struct PagedSampledSA {
    reader: PagedReader,
    len: usize,        // Number of elements (u64 integers)
    start_offset: u64, // Byte offset in the file where the SA begins
    bits: u8,          // 0 = plain u64, 1..=32 = packed u32 width
    byte_len: u64,     // Total bytes for the packed representation
}

impl PagedSampledSA {
    /// Initialize the view.
    /// `len` is the number of items in the SA (not bytes).
    /// `start_offset` allows this to live inside a larger .idx container file.
    pub fn new(reader: PagedReader, len: usize, start_offset: u64, bits: u8) -> Self {
        let byte_len = if bits == 0 {
            len as u64 * 8
        } else if bits <= 32 {
            let words = (len as u64 * bits as u64).div_ceil(32);
            words * 4
        } else {
            let words = (len as u64 * bits as u64).div_ceil(64);
            words * 8
        };
        Self {
            reader,
            len,
            start_offset,
            bits,
            byte_len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the length of the array (number of elements).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the value at index `i`.
    /// Returns io::Result in case of disk error.
    pub fn get(&self, i: usize) -> io::Result<u64> {
        if i >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }
        if self.bits == 0 {
            // 1. Calculate Byte Offset
            let element_size = size_of::<u64>() as u64;
            let abs_offset = self.start_offset + (i as u64 * element_size);

            // 2. Read from Paged Reader
            let bytes = self.reader.read_at(abs_offset, 8)?;

            // 3. Deserialize
            Ok(LittleEndian::read_u64(&bytes))
        } else {
            let bit_offset = i as u64 * self.bits as u64;
            let byte_offset = bit_offset / 8;
            let bit_in_byte = (bit_offset % 8) as u32;
            let bytes_needed = (bit_in_byte as u64 + self.bits as u64).div_ceil(8) as usize;

            if byte_offset + bytes_needed as u64 > self.byte_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Packed read beyond buffer",
                ));
            }

            let bytes = self
                .reader
                .read_at(self.start_offset + byte_offset, bytes_needed)?;

            if self.bits <= 32 {
                let mut buf = [0u8; 8];
                buf[..bytes.len()].copy_from_slice(&bytes);
                let raw = u64::from_le_bytes(buf);
                let mask = if self.bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << self.bits) - 1
                };
                Ok((raw >> bit_in_byte) & mask)
            } else {
                let mut buf = [0u8; 16];
                buf[..bytes.len()].copy_from_slice(&bytes);
                let raw = u128::from_le_bytes(buf);
                let mask = if self.bits == 128 {
                    u128::MAX
                } else {
                    (1u128 << self.bits) - 1
                };
                Ok(((raw >> bit_in_byte) & mask) as u64)
            }
        }
    }

    /// Bulk read for range queries (optimization).
    /// useful when we narrow down a range [L, R] and need to fetch values.
    pub fn get_range(&self, start: usize, end: usize) -> io::Result<Vec<u64>> {
        if end > self.len || start > end {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid range"));
        }
        let count = end - start;
        if self.bits == 0 {
            let element_size = size_of::<u64>();
            let abs_offset = self.start_offset + (start as u64 * element_size as u64);
            let total_bytes = count * element_size;

            let raw_bytes = self.reader.read_at(abs_offset, total_bytes)?;

            // Convert [u8] -> [u64]
            let mut result = Vec::with_capacity(count);
            for chunk in raw_bytes.chunks_exact(8) {
                result.push(LittleEndian::read_u64(chunk));
            }

            Ok(result)
        } else {
            let mut result = Vec::with_capacity(count);
            for i in start..end {
                result.push(self.get(i)?);
            }
            Ok(result)
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::iolib::paged_reader::GlobalPageCache;
//     use std::io::Write;
//     use std::sync::Arc;
//     use tempfile::NamedTempFile;

//     #[test]
//     fn test_sampled_sa_access() {
//         // 1. Create a dummy SA file [0, 1, 2, ... 999]
//         let mut file = NamedTempFile::new().unwrap();
//         let mut data = Vec::with_capacity(1000 * 4);
//         for i in 0..1000u32 {
//             data.extend_from_slice(&i.to_le_bytes());
//         }
//         file.write_all(&data).unwrap();
//         file.as_file().sync_all().unwrap();

//         // 2. Setup Reader
//         let cache = Arc::new(GlobalPageCache::new(1024 * 1024, 1));
//         let reader = PagedReader::new(file.path(), 99, cache).unwrap();

//         // 3. Setup SA View
//         let sa = PagedSampledSA::new(reader, 1000, 0);

//         // 4. Verification
//         assert_eq!(sa.get(0).unwrap(), 0);
//         assert_eq!(sa.get(500).unwrap(), 500);
//         assert_eq!(sa.get(999).unwrap(), 999);
//         assert!(sa.get(1000).is_err()); // Out of bounds
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bitpack;
    use crate::iolib::paged_reader::{GlobalPageCache, PagedReader};
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    // Helper to generate a deterministic SA pattern
    fn generate_sa_data(count: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(count * 8);
        for i in 0..count {
            // Use a pattern that isn't just 0,1,2 to detect read errors
            // e.g., i * 10
            let val = (i as u64).wrapping_mul(10);
            data.extend_from_slice(&val.to_le_bytes());
        }
        data
    }

    #[test]
    fn test_sa_offset_and_alignment() {
        // Scenario: The SA starts 100 bytes into the file (header space).
        // It has enough elements to cross a 4KB page boundary.
        // 4096 bytes / 8 bytes per int = 512 ints per page.
        // Let's write 2000 integers.
        let sa_len = 2000;
        let start_offset = 100;

        let mut file = NamedTempFile::new().unwrap();
        let mut file_content = vec![0u8; start_offset]; // Header padding
        let sa_bytes = generate_sa_data(sa_len);
        file_content.extend_from_slice(&sa_bytes);

        file.write_all(&file_content).unwrap();
        file.as_file().sync_all().unwrap();

        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 555, cache).unwrap();

        // Initialize SA at the offset
        let sa = PagedSampledSA::new(reader, sa_len, start_offset as u64, 0);

        // 1. Verify Length
        assert_eq!(sa.len(), sa_len);

        // 2. Verify First Element
        assert_eq!(sa.get(0).unwrap(), 0);

        // 3. Verify Element crossing page boundary
        // Page 0 ends at 4096.
        // Our data starts at 100.
        // Space in Page 0 for data: 4096 - 100 = 3996 bytes.
        // Ints in Page 0: 3996 / 8 = 499 integers.
        // Index 498 is fully in Page 0.
        // Index 499 is fully in Page 1 (starts at byte 100 + 499*8 = 4092).

        assert_eq!(sa.get(498).unwrap(), 498 * 10);
        assert_eq!(sa.get(499).unwrap(), 499 * 10); // The critical boundary check
        assert_eq!(sa.get(500).unwrap(), 500 * 10);

        // 4. Verify Last Element
        assert_eq!(sa.get(sa_len - 1).unwrap(), (sa_len as u64 - 1) * 10);
    }

    #[test]
    fn test_sa_range_queries() {
        let sa_len = 5000;
        let start_offset = 0;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&generate_sa_data(sa_len)).unwrap();
        file.as_file().sync_all().unwrap();

        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 777, cache).unwrap();
        let sa = PagedSampledSA::new(reader, sa_len, start_offset, 0);

        // Fetch a range that spans multiple pages.
        // 3000 ints * 8 = 24000 bytes.
        let range = sa.get_range(100, 3100).unwrap();

        assert_eq!(range.len(), 3000);
        assert_eq!(range[0], 100 * 10);
        assert_eq!(range[2999], 3099 * 10);
    }

    #[test]
    fn test_sa_empty_and_oob() {
        let file = NamedTempFile::new().unwrap(); // Empty file
        let cache = Arc::new(GlobalPageCache::new(1024, 1));
        let reader = PagedReader::new(file.path(), 888, cache).unwrap();

        let sa = PagedSampledSA::new(reader, 0, 0, 0);
        assert_eq!(sa.len(), 0);
        assert!(sa.get(0).is_err());
        assert!(sa.get_range(0, 1).is_err());
    }

    #[test]
    fn test_sa_u64_values() {
        let start_offset = 0;
        let mut file = NamedTempFile::new().unwrap();
        let count = 16;
        let base = u32::MAX as u64 + 123;
        let mut data = Vec::with_capacity(count * 8);
        for i in 0..count {
            let val = base + i as u64;
            data.extend_from_slice(&val.to_le_bytes());
        }
        file.write_all(&data).unwrap();
        file.as_file().sync_all().unwrap();

        let cache = Arc::new(GlobalPageCache::new(1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 9999, cache).unwrap();
        let sa = PagedSampledSA::new(reader, count, start_offset, 0);

        assert_eq!(sa.get(0).unwrap(), base);
        assert_eq!(sa.get(count - 1).unwrap(), base + (count as u64 - 1));
    }

    #[test]
    fn test_sa_packed_u32_access() {
        let count = 2048;
        let values: Vec<u32> = (0..count).map(|i| (i * 3) as u32).collect();
        let w = bitpack::required_bits_u32(&values);
        let words = (values.len() * w).div_ceil(32);
        let mut packed = vec![0u32; words.max(1)];
        let (packed_w, written) = bitpack::pack_u32_dynamic(&values, &mut packed);
        packed.truncate(written);

        let mut file = NamedTempFile::new().unwrap();
        for word in &packed {
            file.write_all(&word.to_le_bytes()).unwrap();
        }
        file.as_file().sync_all().unwrap();

        let cache = Arc::new(GlobalPageCache::new(1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 4242, cache).unwrap();
        let sa = PagedSampledSA::new(reader, values.len(), 0, packed_w as u8);

        assert_eq!(sa.get(0).unwrap(), values[0] as u64);
        assert_eq!(sa.get(1).unwrap(), values[1] as u64);
        assert_eq!(sa.get(128).unwrap(), values[128] as u64);
        assert_eq!(sa.get(1023).unwrap(), values[1023] as u64);
        assert_eq!(sa.get(count - 1).unwrap(), values[count - 1] as u64);
    }

    #[test]
    fn test_sa_packed_u64_access() {
        let count = 1024;
        let base = u32::MAX as u64 + 1000;
        let values: Vec<u64> = (0..count).map(|i| base + (i as u64 * 17)).collect();
        let w = bitpack::required_bits_u64(&values);
        let words = (values.len() * w).div_ceil(64);
        let mut packed = vec![0u64; words.max(1)];
        let (packed_w, written) = bitpack::pack_u64_dynamic(&values, &mut packed);
        packed.truncate(written);

        let mut file = NamedTempFile::new().unwrap();
        for word in &packed {
            file.write_all(&word.to_le_bytes()).unwrap();
        }
        file.as_file().sync_all().unwrap();

        let cache = Arc::new(GlobalPageCache::new(1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 4243, cache).unwrap();
        let sa = PagedSampledSA::new(reader, values.len(), 0, packed_w as u8);

        assert_eq!(sa.get(0).unwrap(), values[0]);
        assert_eq!(sa.get(1).unwrap(), values[1]);
        assert_eq!(sa.get(127).unwrap(), values[127]);
        assert_eq!(sa.get(512).unwrap(), values[512]);
        assert_eq!(sa.get(count - 1).unwrap(), values[count - 1]);
    }
}
