#![allow(dead_code)]

use aligned_alloc::{aligned_alloc, aligned_free};
use bitmaps::*;
use std::cell::RefCell;
use std::io::IoSlice;
use std::rc::Rc;
use typenum::*;

#[derive(Debug)]
pub struct UringSlab {
    data: *mut u8,
    size: usize,
    blksize: usize,
    slabidx: usize,
    free: RefCell<Bitmap<U128>>,
}

#[derive(Debug)]
pub struct UringDmaBuffer {
    data: *mut u8,
    size: usize,
    slab: Rc<UringSlab>,
    pub(crate) slabidx: usize,
}

impl Drop for UringDmaBuffer {
    fn drop(&mut self) {
        self.slab.free_buffer(self.data);
    }
}

impl UringDmaBuffer {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.size) }
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.size) }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn trim_to_size(&mut self, new_size: usize) {
        self.size = std::cmp::min(new_size, self.slab.blksize);
    }
}

impl UringSlab {
    // FIXME: Result
    pub fn new(blksize: usize, slabidx: usize) -> UringSlab {
        let size = blksize * std::mem::size_of::<u128>();
        let data: *mut u8 = aligned_alloc(size, 4 << 10) as *mut u8;
        // FIXME: if null.... Result
        let mut free = Bitmap::new(); // inits to all false
        free.invert(); // all true now
        UringSlab {
            data,
            size,
            free: RefCell::new(free),
            slabidx,
            blksize,
        }
    }

    pub fn as_io_slice(&mut self) -> IoSlice<'_> {
        let slice = unsafe { std::slice::from_raw_parts(self.data, self.size) };
        IoSlice::new(slice)
    }

    pub fn free_buffer(&self, buffer: *mut u8) {
        let buffer = buffer as u64;
        let data = self.data as u64;
        let diff = (buffer - data) as usize;
        assert_eq!(diff % self.blksize, 0);
        let bit = diff / self.blksize;
        println!(
            "Freeing buffer at {:x} from {:x}, calculated bit {}",
            buffer, data, bit
        );
        let mut free = self.free.borrow_mut();
        let was = free.set(bit, true);
        assert_eq!(was, false);
    }

    pub fn alloc_buffer(self: Rc<Self>, size: usize) -> Option<UringDmaBuffer> {
        let mut free = self.free.borrow_mut();
        let bit = free.first_index();
        match bit {
            Some(bit) => {
                free.set(bit, false);
                drop(free);

                let ptr = unsafe { self.data.add(self.blksize * bit) };
                println!("Allocated buffer {:p} from {:p}", ptr, self.data);
                Some(UringDmaBuffer {
                    data: ptr,
                    size, // may be different than allocated size, this is sans any padding
                    slabidx: self.slabidx,
                    slab: self.clone(),
                })
            }
            None => None,
        }
    }
}

impl Drop for UringSlab {
    fn drop(&mut self) {
        unsafe {
            aligned_free(self.data as _);
        }
    }
}
