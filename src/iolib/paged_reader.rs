use std::fs::File;
use std::io::{self};
use std::os::unix::fs::FileExt; // For pread
use std::path::Path;
use std::sync::Arc;

use crate::cache::sharded_fifo::ShardedFastS3Fifo;

const PAGE_SIZE: usize = 4096;

/// A global cache shared across all readers.
/// Key: (FileID, PageIndex), Value: 4KB Page Data
pub type GlobalPageCache = ShardedFastS3Fifo<(u64, u64), Vec<u8>>;

#[derive(Clone)]
pub struct PagedReader {
    file: Arc<File>,
    file_id: u64, // Unique ID for this file (e.g., hash of path or inode)
    file_len: u64,
    cache: Arc<GlobalPageCache>,
}

impl PagedReader {
    pub fn new<P: AsRef<Path>>(
        path: P,
        file_id: u64,
        cache: Arc<GlobalPageCache>,
    ) -> io::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();

        Ok(Self {
            file: Arc::new(file),
            file_id,
            file_len,
            cache,
        })
    }

    /// Read bytes at a specific offset using the S3-FIFO cache.
    /// Handles reads that span across multiple pages.
    pub fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if offset + len as u64 > self.file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read past EOF",
            ));
        }

        let mut result = vec![0u8; len];
        let mut bytes_copied = 0;
        let mut current_offset = offset;

        while bytes_copied < len {
            // 1. Identify the Page
            let page_idx = current_offset / PAGE_SIZE as u64;
            let offset_in_page = (current_offset % PAGE_SIZE as u64) as usize;

            // 2. Fetch Page (Hit or Miss)
            let page_data = self.get_page(page_idx)?;

            // 3. Copy relevant data
            let available_in_page = PAGE_SIZE - offset_in_page;
            let needed = len - bytes_copied;
            let to_copy = available_in_page.min(needed);

            result[bytes_copied..bytes_copied + to_copy]
                .copy_from_slice(&page_data[offset_in_page..offset_in_page + to_copy]);

            bytes_copied += to_copy;
            current_offset += to_copy as u64;
        }

        Ok(result)
    }

    /// Fetch a page from S3-FIFO cache or load from disk.
    fn get_page(&self, page_idx: u64) -> io::Result<Arc<Vec<u8>>> {
        let key = (self.file_id, page_idx);

        // 1. Try Cache Hit
        if let Some(page) = self.cache.get(&key) {
            return Ok(page);
        }

        // 2. Cache Miss: Read from Disk
        // Note: In a highly concurrent environment, two threads might read the same page
        // simultaneously here (thundering herd). For now, we accept the redundant I/O
        // to avoid complex lock sharding on "pending reads".

        let mut buffer = vec![0u8; PAGE_SIZE];
        let page_start = page_idx * PAGE_SIZE as u64;

        // Handle partial last page
        let read_len = if page_start + PAGE_SIZE as u64 > self.file_len {
            (self.file_len - page_start) as usize
        } else {
            PAGE_SIZE
        };

        // pread is thread-safe and atomic
        self.file.read_at(&mut buffer[0..read_len], page_start)?;

        let page_arc = Arc::new(buffer);

        // 3. Insert into Cache
        self.cache.put(key, page_arc.clone());

        Ok(page_arc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::NamedTempFile;

    fn create_test_file(size: usize) -> (NamedTempFile, Vec<u8>) {
        let mut file = NamedTempFile::new().unwrap();
        let data: Vec<u8> = (0..size).map(|i| (i % 255) as u8).collect();
        file.write_all(&data).unwrap();
        file.as_file().sync_all().unwrap(); // Ensure flush to disk
        (file, data)
    }

    #[test]
    fn test_read_single_page() {
        let (file, data) = create_test_file(PAGE_SIZE * 2);
        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1)); // 10MB cache
        let reader = PagedReader::new(file.path(), 1, cache).unwrap();

        // Read middle of first page
        let read_data = reader.read_at(100, 50).unwrap();
        assert_eq!(read_data, &data[100..150]);
    }

    #[test]
    fn test_read_cross_page_boundary() {
        let (file, data) = create_test_file(PAGE_SIZE * 3);
        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 2, cache).unwrap();

        // Read across page 0 and page 1
        // Start: PAGE_SIZE - 10
        // End:   PAGE_SIZE + 10
        let start = (PAGE_SIZE - 10) as u64;
        let len = 20;
        let read_data = reader.read_at(start, len).unwrap();

        assert_eq!(read_data.len(), len);
        assert_eq!(read_data, &data[(start as usize)..(start as usize + len)]);
    }

    #[test]
    fn test_read_past_eof() {
        let (file, _) = create_test_file(100);
        let cache = Arc::new(GlobalPageCache::new(1024, 1));
        let reader = PagedReader::new(file.path(), 3, cache).unwrap();

        let res = reader.read_at(90, 20); // 10 bytes valid, 10 past EOF
        assert!(res.is_err()); // Should verify ErrorKind::UnexpectedEof in robust implementations
    }

    #[test]
    fn test_concurrent_access_same_file() {
        let (file, data) = create_test_file(PAGE_SIZE * 10);
        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 16));
        let path = file.path().to_path_buf();
        let data = Arc::new(data);

        let mut handles = vec![];
        let barrier = Arc::new(Barrier::new(10));

        for i in 0..10 {
            let path_clone = path.clone();
            let cache_clone = cache.clone();
            let data_clone = data.clone();
            let barrier_clone = barrier.clone();

            handles.push(thread::spawn(move || {
                let reader = PagedReader::new(path_clone, 100, cache_clone).unwrap();
                barrier_clone.wait(); // Synchronize start

                // Read a unique page based on thread ID to avoid trivial cache hits everywhere
                // but also some overlap to test the cache logic.
                let offset = (i * PAGE_SIZE / 2) as u64;
                let read_data = reader.read_at(offset, 100).unwrap();
                assert_eq!(
                    read_data,
                    &data_clone[(offset as usize)..(offset as usize + 100)]
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
