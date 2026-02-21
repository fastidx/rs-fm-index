use std::fs::File;
use std::io::{self};
use std::os::unix::fs::FileExt; // For pread
use std::path::Path;
use std::sync::Arc;

use crate::cache::sharded_fifo::ShardedFastS3Fifo;

pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// A global cache shared across all readers.
/// Key: (FileID, PageSize, PageIndex), Value: Page Data
pub type GlobalPageCache = ShardedFastS3Fifo<(u64, u32, u64), Vec<u8>>;

#[derive(Debug, Clone, Copy)]
pub struct PagedReaderConfig {
    pub page_size: usize,
    pub prefetch_pages: usize,
}

impl Default for PagedReaderConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            prefetch_pages: 0,
        }
    }
}

#[derive(Clone)]
pub struct PagedReader {
    file: Arc<File>,
    file_id: u64, // Unique ID for this file (e.g., hash of path or inode)
    file_len: u64,
    cache: Arc<GlobalPageCache>,
    page_size: usize,
    prefetch_pages: usize,
}

impl PagedReader {
    pub fn new_with_config<P: AsRef<Path>>(
        path: P,
        file_id: u64,
        cache: Arc<GlobalPageCache>,
        config: PagedReaderConfig,
    ) -> io::Result<Self> {
        if config.page_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "page_size must be > 0",
            ));
        }

        let file = File::open(path)?;
        let file_len = file.metadata()?.len();

        Ok(Self {
            file: Arc::new(file),
            file_id,
            file_len,
            cache,
            page_size: config.page_size,
            prefetch_pages: config.prefetch_pages,
        })
    }

    pub fn new<P: AsRef<Path>>(
        path: P,
        file_id: u64,
        cache: Arc<GlobalPageCache>,
    ) -> io::Result<Self> {
        Self::new_with_config(path, file_id, cache, PagedReaderConfig::default())
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
            let page_idx = current_offset / self.page_size as u64;
            let offset_in_page = (current_offset % self.page_size as u64) as usize;

            // 2. Fetch Page (Hit or Miss)
            let page_data = self.get_page(page_idx)?;

            // 3. Copy relevant data
            let available_in_page = self.page_size - offset_in_page;
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
        let key = self.cache_key(page_idx);

        // 1. Try Cache Hit
        if let Some(page) = self.cache.get(&key) {
            return Ok(page);
        }

        // 2. Cache Miss: Read from Disk + Prefetch
        // Note: In a highly concurrent environment, two threads might read the same page
        // simultaneously here (thundering herd). For now, we accept the redundant I/O
        // to avoid complex lock sharding on "pending reads".

        let pages = 1usize.saturating_add(self.prefetch_pages);
        self.read_pages_into_cache(page_idx, pages)?;

        // 3. Return the requested page
        if let Some(page) = self.cache.get(&key) {
            return Ok(page);
        }

        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Failed to load page",
        ))
    }

    fn cache_key(&self, page_idx: u64) -> (u64, u32, u64) {
        (self.file_id, self.page_size as u32, page_idx)
    }

    fn read_at_fully(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut read = 0usize;
        while read < buf.len() {
            let n = self.file.read_at(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Read past EOF",
                ));
            }
            read += n;
        }
        Ok(())
    }

    fn read_pages_into_cache(&self, page_idx: u64, pages: usize) -> io::Result<()> {
        if pages == 0 {
            return Ok(());
        }

        let page_size = self.page_size as u64;
        let page_start = page_idx * page_size;
        if page_start >= self.file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read past EOF",
            ));
        }

        let remaining_bytes = self.file_len - page_start;
        let max_pages = (remaining_bytes + page_size - 1) / page_size;
        let pages_to_read = std::cmp::min(pages as u64, max_pages) as usize;
        let total_bytes = std::cmp::min(
            remaining_bytes,
            pages_to_read as u64 * page_size,
        ) as usize;

        let mut buffer = vec![0u8; total_bytes];
        self.read_at_fully(page_start, &mut buffer)?;

        for i in 0..pages_to_read {
            let offset = i * self.page_size;
            if offset >= buffer.len() {
                break;
            }

            let end = std::cmp::min(offset + self.page_size, buffer.len());
            let mut page_buf = vec![0u8; self.page_size];
            page_buf[..end - offset].copy_from_slice(&buffer[offset..end]);
            self.cache
                .put(self.cache_key(page_idx + i as u64), Arc::new(page_buf));
        }

        Ok(())
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
        let (file, data) = create_test_file(DEFAULT_PAGE_SIZE * 2);
        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1)); // 10MB cache
        let reader = PagedReader::new(file.path(), 1, cache).unwrap();

        // Read middle of first page
        let read_data = reader.read_at(100, 50).unwrap();
        assert_eq!(read_data, &data[100..150]);
    }

    #[test]
    fn test_read_cross_page_boundary() {
        let (file, data) = create_test_file(DEFAULT_PAGE_SIZE * 3);
        let cache = Arc::new(GlobalPageCache::new(10 * 1024 * 1024, 1));
        let reader = PagedReader::new(file.path(), 2, cache).unwrap();

        // Read across page 0 and page 1
        // Start: PAGE_SIZE - 10
        // End:   PAGE_SIZE + 10
        let start = (DEFAULT_PAGE_SIZE - 10) as u64;
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
        let (file, data) = create_test_file(DEFAULT_PAGE_SIZE * 10);
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
                let offset = (i * DEFAULT_PAGE_SIZE / 2) as u64;
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

    #[test]
    fn test_custom_page_size() {
        let page_size = 1024;
        let (file, data) = create_test_file(page_size * 4);
        let cache = Arc::new(GlobalPageCache::new(2 * 1024 * 1024, 2));
        let config = PagedReaderConfig {
            page_size,
            prefetch_pages: 2,
        };
        let reader = PagedReader::new_with_config(file.path(), 4, cache, config).unwrap();

        let start = (page_size - 5) as u64;
        let len = 20;
        let read_data = reader.read_at(start, len).unwrap();
        assert_eq!(
            read_data,
            &data[(start as usize)..(start as usize + len)]
        );
    }
}
