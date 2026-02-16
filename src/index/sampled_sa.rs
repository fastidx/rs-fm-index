use crate::iolib::paged_reader::PagedReader;
use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::mem::size_of;

/// A read-only view of a Sampled Suffix Array stored on disk.
/// Accesses are cached via the underlying PagedReader.
pub struct PagedSampledSA {
    reader: PagedReader,
    len: usize,        // Number of elements (u32 integers)
    start_offset: u64, // Byte offset in the file where the SA begins
}

impl PagedSampledSA {
    /// Initialize the view.
    /// `len` is the number of items in the SA (not bytes).
    /// `start_offset` allows this to live inside a larger .idx container file.
    pub fn new(reader: PagedReader, len: usize, start_offset: u64) -> Self {
        Self {
            reader,
            len,
            start_offset,
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
    pub fn get(&self, i: usize) -> io::Result<u32> {
        if i >= self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }

        // 1. Calculate Byte Offset
        // We assume u32 for now. For >4GB files, we will need u40 or u48 packing later,
        // but u32 covers up to 4GB shards nicely.
        let element_size = size_of::<u32>() as u64;
        let abs_offset = self.start_offset + (i as u64 * element_size);

        // 2. Read from Paged Reader
        // This handles the page alignment logic internally.
        let bytes = self.reader.read_at(abs_offset, 4)?;

        // 3. Deserialize
        Ok(LittleEndian::read_u32(&bytes))
    }

    /// Bulk read for range queries (optimization).
    /// useful when we narrow down a range [L, R] and need to fetch values.
    pub fn get_range(&self, start: usize, end: usize) -> io::Result<Vec<u32>> {
        if end > self.len || start > end {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid range"));
        }

        let count = end - start;
        let element_size = size_of::<u32>();
        let abs_offset = self.start_offset + (start as u64 * element_size as u64);
        let total_bytes = count * element_size;

        let raw_bytes = self.reader.read_at(abs_offset, total_bytes)?;

        // Convert [u8] -> [u32]
        let mut result = Vec::with_capacity(count);
        for chunk in raw_bytes.chunks_exact(4) {
            result.push(LittleEndian::read_u32(chunk));
        }

        Ok(result)
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
    use crate::iolib::paged_reader::{GlobalPageCache, PagedReader};
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    // Helper to generate a deterministic SA pattern
    fn generate_sa_data(count: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(count * 4);
        for i in 0..count {
            // Use a pattern that isn't just 0,1,2 to detect read errors
            // e.g., i * 10
            let val = (i as u32).wrapping_mul(10);
            data.extend_from_slice(&val.to_le_bytes());
        }
        data
    }

    #[test]
    fn test_sa_offset_and_alignment() {
        // Scenario: The SA starts 100 bytes into the file (header space).
        // It has enough elements to cross a 4KB page boundary.
        // 4096 bytes / 4 bytes per int = 1024 ints per page.
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
        let sa = PagedSampledSA::new(reader, sa_len, start_offset as u64);

        // 1. Verify Length
        assert_eq!(sa.len(), sa_len);

        // 2. Verify First Element
        assert_eq!(sa.get(0).unwrap(), 0);

        // 3. Verify Element crossing page boundary
        // Page 0 ends at 4096.
        // Our data starts at 100.
        // Space in Page 0 for data: 4096 - 100 = 3996 bytes.
        // Ints in Page 0: 3996 / 4 = 999 integers.
        // Index 998 is fully in Page 0.
        // Index 999 is fully in Page 1 (starts at byte 100 + 999*4 = 4096).

        assert_eq!(sa.get(998).unwrap(), 998 * 10);
        assert_eq!(sa.get(999).unwrap(), 999 * 10); // The critical boundary check
        assert_eq!(sa.get(1000).unwrap(), 1000 * 10);

        // 4. Verify Last Element
        assert_eq!(sa.get(sa_len - 1).unwrap(), (sa_len as u32 - 1) * 10);
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
        let sa = PagedSampledSA::new(reader, sa_len, start_offset);

        // Fetch a range that spans 3 pages (4KB * 3 approx 12KB)
        // 3000 ints * 4 = 12000 bytes.
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

        let sa = PagedSampledSA::new(reader, 0, 0);
        assert_eq!(sa.len(), 0);
        assert!(sa.get(0).is_err());
        assert!(sa.get_range(0, 1).is_err());
    }
}
