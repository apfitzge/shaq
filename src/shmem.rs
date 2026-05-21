use crate::error::Error;
use std::{fs::File, num::NonZeroUsize, ptr::NonNull, sync::Arc};

pub(crate) const MINIMUM_REGION_ALIGNMENT: usize = 4096;

pub(crate) struct Region {
    addr: NonNull<u8>,
    size: usize,
    backing: RegionBacking,
}

impl Region {
    pub(crate) fn map_file(file: &File, size: usize) -> Result<Arc<Self>, Error> {
        let addr = map_file(file, size)?;
        validate_region_alignment(addr)?;
        Ok(Arc::new(Self {
            addr,
            size,
            backing: RegionBacking::MappedFile,
        }))
    }

    pub(crate) fn alloc(size: NonZeroUsize) -> Result<Arc<Self>, Error> {
        let layout = std::alloc::Layout::from_size_align(size.get(), MINIMUM_REGION_ALIGNMENT)
            .map_err(|_| Error::InvalidBufferSize)?;
        let addr = {
            // SAFETY: layout is valid and non-zero.
            let addr = unsafe { std::alloc::alloc_zeroed(layout) };
            NonNull::new(addr).ok_or(Error::Allocation(layout))?
        };

        assert_eq!(addr.as_ptr().align_offset(MINIMUM_REGION_ALIGNMENT), 0);

        Ok(Arc::new(Self {
            addr,
            size: size.get(),
            backing: RegionBacking::Heap(layout),
        }))
    }

    pub(crate) fn addr(&self) -> NonNull<u8> {
        self.addr
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        match self.backing {
            RegionBacking::MappedFile => {
                // SAFETY: addr and size were produced by a successful map_file call.
                unsafe { unmap_file(self.addr, self.size) };
            }
            RegionBacking::Heap(layout) => {
                // SAFETY: addr was allocated with this exact layout in `alloc`.
                unsafe { std::alloc::dealloc(self.addr.as_ptr(), layout) };
            }
        }
    }
}

enum RegionBacking {
    MappedFile,
    Heap(std::alloc::Layout),
}

// SAFETY: The mapped memory is shared (MAP_SHARED / file-backed) and access
// is synchronized by the queue protocol built on top of it.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

fn validate_region_alignment(addr: NonNull<u8>) -> Result<(), Error> {
    let actual = addr.as_ptr().align_offset(MINIMUM_REGION_ALIGNMENT);
    if actual != 0 {
        return Err(Error::InvalidRegionAlignment {
            minimum: MINIMUM_REGION_ALIGNMENT,
            actual,
        });
    }

    Ok(())
}

/// Maps a file into memory.
fn map_file(file: &File, size: usize) -> Result<NonNull<u8>, Error> {
    use std::os::fd::AsRawFd;

    let addr = unsafe {
        libc::mmap(
            core::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    Ok(NonNull::new(addr.cast()).expect("already checked for null"))
}

/// Unmaps a previously mapped file view.
unsafe fn unmap_file(addr: NonNull<u8>, size: usize) {
    let _ = unsafe { libc::munmap(addr.as_ptr().cast(), size) };
}

#[cfg(test)]
pub(crate) fn create_temp_shmem_file() -> Result<File, Error> {
    use std::fs::OpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let temp_dir = std::env::temp_dir();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = temp_dir.join(format!("rts-alloc-{n}.tmp"));

    let mut open_options = OpenOptions::new();
    open_options.read(true).write(true).create_new(true);

    let open_result = open_options.open(&path);

    match open_result {
        Ok(file) => {
            std::fs::remove_file(&path)?;
            Ok(file)
        }
        Err(err) => Err(Error::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_is_minimum_region_aligned() {
        let file = create_temp_shmem_file().expect("temp file");
        file.set_len(MINIMUM_REGION_ALIGNMENT as u64)
            .expect("set len");

        let region = Region::map_file(&file, MINIMUM_REGION_ALIGNMENT).expect("map file");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
    }

    #[test]
    fn test_alloc_region_is_4096_aligned() {
        let region = Region::alloc(NonZeroUsize::new(MINIMUM_REGION_ALIGNMENT * 2).unwrap())
            .expect("allocation failed");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
        assert_eq!(region.size(), MINIMUM_REGION_ALIGNMENT * 2);
    }

    #[test]
    fn test_alloc_region_accepts_non_4096_multiple() {
        let region = Region::alloc(NonZeroUsize::new(MINIMUM_REGION_ALIGNMENT + 1).unwrap())
            .expect("allocation failed");
        assert_eq!(
            region
                .addr()
                .as_ptr()
                .align_offset(MINIMUM_REGION_ALIGNMENT),
            0
        );
        assert_eq!(region.size(), MINIMUM_REGION_ALIGNMENT + 1);
    }
}
