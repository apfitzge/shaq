use crate::error::Error;
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
};

pub fn create_and_map_file(
    path: impl AsRef<Path>,
    size: usize,
    hugepages: bool,
    mirror: bool,
) -> Result<*mut u8, Error> {
    let file = create_file(path.as_ref(), size)?;
    if mirror {
        mirror_map(&file, size, hugepages)
    } else {
        map(&file, size, hugepages)
    }
}

pub fn open_and_map_file(
    path: impl AsRef<Path>,
    hugepages: bool,
    mirror: bool,
) -> Result<(*mut u8, usize), Error> {
    let file = open_file(path.as_ref())?;
    let file_size = file.metadata()?.len() as usize;
    let mmap = if mirror {
        mirror_map(&file, file_size, hugepages)
    } else {
        map(&file, file_size, hugepages)
    }?;

    Ok((mmap, file_size))
}

fn create_file(path: impl AsRef<Path>, size: usize) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.as_ref())?;

    file.set_len(size as u64)?;
    Ok(file)
}

fn open_file(path: impl AsRef<Path>) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())?;
    Ok(file)
}

fn map(file: &File, size: usize, hugepages: bool) -> Result<*mut u8, Error> {
    let addr = unsafe {
        nix::libc::mmap(
            core::ptr::null_mut(),
            size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            nix::libc::MAP_SHARED | if hugepages { nix::libc::MAP_HUGETLB } else { 0 },
            file.as_raw_fd(),
            0,
        )
    };

    if addr == nix::libc::MAP_FAILED {
        Err(Error::Mmap(std::io::Error::last_os_error()))
    } else {
        Ok(addr.cast())
    }
}

fn mirror_map(file: &File, size: usize, hugepages: bool) -> Result<*mut u8, Error> {
    let addr = unsafe {
        nix::libc::mmap(
            core::ptr::null_mut(),
            size * 2,
            nix::libc::PROT_NONE,
            nix::libc::MAP_PRIVATE
                | nix::libc::MAP_ANONYMOUS
                | if hugepages { nix::libc::MAP_HUGETLB } else { 0 },
            -1,
            0,
        )
    };

    if addr == nix::libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    let addr1 = addr;
    let addr2 = (addr as usize + size) as *mut nix::libc::c_void;
    let flags = nix::libc::MAP_SHARED
        | nix::libc::MAP_FIXED
        | nix::libc::MAP_POPULATE
        | if hugepages { nix::libc::MAP_HUGETLB } else { 0 };

    let first = unsafe {
        nix::libc::mmap(
            addr1,
            size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            flags,
            file.as_raw_fd(),
            0,
        )
    };

    if first == nix::libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    let second = unsafe {
        nix::libc::mmap(
            addr2,
            size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            flags,
            file.as_raw_fd(),
            0,
        )
    };

    if second == nix::libc::MAP_FAILED {
        return Err(Error::Mmap(std::io::Error::last_os_error()));
    }

    Ok(first.cast())
}

pub const STANDARD_PAGE_SIZE: usize = 4096;
pub const HUGE_PAGE_SIZE: usize = 1 << 21;

/// Round up to the nearest multiple of `ROUNDING_SIZE`.
/// This is a constant function that can be used in `const` contexts.
///
/// Safety:
///     - `ROUNDING_SIZE` must be a power of 2.
pub const unsafe fn pow2_round_size<const ROUNDING_SIZE: usize>(size: usize) -> usize {
    (size + ROUNDING_SIZE - 1) & !(ROUNDING_SIZE - 1)
}

pub fn use_hugepages(path: impl AsRef<Path>) -> bool {
    path.as_ref().starts_with("/mnt/hugepages")
}

pub fn get_rounded_file_size(path: impl AsRef<Path>, size: usize) -> usize {
    let hugepages = use_hugepages(path.as_ref());
    if hugepages {
        // Safety: `HUGE_PAGE_SIZE` is a power of 2.
        unsafe { pow2_round_size::<HUGE_PAGE_SIZE>(size) }
    } else {
        // Safety: `STANDARD_PAGE_SIZE` is a power of 2.
        unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(size) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pow2_round_size() {
        assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(1234) }, 4096);
        assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(4096) }, 4096);
        assert_eq!(unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(8192) }, 8192);
        assert_eq!(
            unsafe { pow2_round_size::<STANDARD_PAGE_SIZE>(8193) },
            12288
        );
    }
}
