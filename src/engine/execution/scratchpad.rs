use std::borrow::BorrowMut;
use std::cell::*;
use std::collections::HashMap;
use std::mem;

use engine::*;

pub struct Scratchpad<'a> {
    buffers: Vec<RefCell<BoxedVec<'a>>>,
    columns: HashMap<String, Vec<&'a AnyVec<'a>>>,
    pinned: Vec<bool>,
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize, columns: HashMap<String, Vec<&'a AnyVec<'a>>>) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(AnyVec::empty(0)));
        }
        Scratchpad {
            buffers,
            columns,
            pinned: vec![false; count],
        }
    }

    pub fn get_any(&self, index: BufferRef<Any>) -> Ref<AnyVec<'a>> {
        Ref::map(self.buffers[index.i].borrow(), |x| x.as_ref())
    }

    pub fn get_any_mut(&self, index: BufferRef<Any>) -> RefMut<AnyVec<'a> + 'a> {
        assert!(!self.pinned[index.i], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[index.i].borrow_mut(), |x| x.borrow_mut())
    }

    pub fn get_column_data(&self, name: &str, section_index: usize) -> &'a AnyVec<'a> {
        match self.columns.get(name) {
            Some(ref col) => col[section_index],
            None => panic!("No column of name {} ({:?})", name, self.columns.keys()),
        }
    }

    pub fn get<T: GenericVec<T> + 'a>(&self, index: BufferRef<T>) -> Ref<[T]> {
        Ref::map(self.buffers[index.i].borrow(), |x| T::unwrap(x.as_ref()))
    }

    pub fn get_pinned<T: GenericVec<T> + 'a>(&mut self, index: BufferRef<T>) -> &'a [T] {
        self.pinned[index.i] = true;
        let buffer = self.get(index);
        unsafe {
            mem::transmute::<&[T], &'a [T]>(&*buffer)
        }
    }

    pub fn get_mut<T: GenericVec<T> + 'a>(&self, index: BufferRef<T>) -> RefMut<Vec<T>> {
        assert!(!self.pinned[index.i], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[index.i].borrow_mut(), |x| {
            let a: &mut AnyVec<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_const<T: ConstType<T>>(&self, index: &BufferRef<T>) -> T {
        T::unwrap(&*self.get_any(index.any()))
    }

    pub fn collect(&mut self, index: BufferRef<Any>) -> BoxedVec<'a> {
        let owned = mem::replace(&mut self.buffers[index.i], RefCell::new(AnyVec::empty(0)));
        owned.into_inner()
    }

    pub fn set_any(&mut self, index: BufferRef<Any>, vec: BoxedVec<'a>) {
        assert!(!self.pinned[index.i], "Trying to set pinned buffer {}", index);
        self.buffers[index.i] = RefCell::new(vec);
    }

    pub fn set<T: GenericVec<T> + 'a>(&mut self, index: BufferRef<T>, vec: Vec<T>) {
        assert!(!self.pinned[index.i], "Trying to set pinned buffer {}", index);
        self.buffers[index.i] = RefCell::new(AnyVec::owned(vec));
    }

    pub fn pin(&mut self, index: BufferRef<Any>) {
        self.pinned[index.i] = true;
    }

    pub unsafe fn unpin(&mut self, index: BufferRef<Any>) {
        self.pinned[index.i] = false;
    }

    pub fn collect_pinned(self) -> Vec<BoxedVec<'a>> {
        self.buffers
            .into_iter()
            .zip(self.pinned.iter())
            .filter_map(|(d, pinned)|
                if *pinned {
                    Some(d.into_inner())
                } else {
                    None
                })
            .collect()
    }
}

