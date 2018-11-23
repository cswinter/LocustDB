use std::borrow::BorrowMut;
use std::cell::*;
use std::collections::HashMap;
use std::mem;

use engine::*;

pub struct Scratchpad<'a> {
    buffers: Vec<RefCell<BoxedData<'a>>>,
    aliases: Vec<Option<usize>>,
    columns: HashMap<String, Vec<&'a Data<'a>>>,
    pinned: Vec<bool>,
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize, columns: HashMap<String, Vec<&'a Data<'a>>>) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(Data::empty(0)));
        }
        Scratchpad {
            buffers,
            aliases: vec![None; count],
            columns,
            pinned: vec![false; count],
        }
    }

    pub fn get_any(&self, index: BufferRef<Any>) -> Ref<Data<'a>> {
        Ref::map(self.buffer(index).borrow(), |x| x.as_ref())
    }

    pub fn get_any_mut(&self, index: BufferRef<Any>) -> RefMut<Data<'a> + 'a> {
        assert!(!self.pinned[self.resolve(&index)], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffer(index).borrow_mut(), |x| x.borrow_mut())
    }

    pub fn get_column_data(&self, name: &str, section_index: usize) -> &'a Data<'a> {
        match self.columns.get(name) {
            Some(ref col) => col[section_index],
            None => panic!("No column of name {} ({:?})", name, self.columns.keys()),
        }
    }

    pub fn get<T: VecData<T> + 'a>(&self, index: BufferRef<T>) -> Ref<[T]> {
        Ref::map(self.buffer(index).borrow(), |x| T::unwrap(x.as_ref()))
    }

    pub fn get_pinned<T: VecData<T> + 'a>(&mut self, index: BufferRef<T>) -> &'a [T] {
        let i = self.resolve(&index);
        self.pinned[i] = true;
        let buffer = self.get(index);
        unsafe {
            mem::transmute::<&[T], &'a [T]>(&*buffer)
        }
    }

    pub fn get_scalar_string_pinned(&mut self, index: &BufferRef<Scalar<String>>) -> &'a str {
        let i = self.resolve(index);
        self.pinned[i] = true;
        let any = self.get_any(index.any());
        unsafe {
            mem::transmute::<&str, &'a str>(any.cast_ref_scalar_string())
        }
    }

    pub fn get_mut<T: VecData<T> + 'a>(&self, index: BufferRef<T>) -> RefMut<Vec<T>> {
        assert!(!self.pinned[self.resolve(&index)], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[self.resolve(&index)].borrow_mut(), |x| {
            let a: &mut Data<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_scalar<T: ScalarData<T>>(&self, index: &BufferRef<Scalar<T>>) -> T {
        T::unwrap(&*self.get_any(index.any()))
    }

    pub fn collect_aliased(&mut self,
                           projections: &[BufferRef<Any>],
                           aggregations: &[(BufferRef<Any>, Aggregator)],
                           rankings: &[(BufferRef<Any>, bool)])
                           -> (Vec<BoxedData<'a>>, Vec<usize>, Vec<(usize, Aggregator)>, Vec<(usize, bool)>) {
        let mut collected_buffers = HashMap::<usize, usize>::default();
        let mut columns = Vec::new();
        let mut projection_indices = Vec::new();
        for &projection in projections {
            let i = self.resolve(&projection);
            if collected_buffers.contains_key(&i) {
                projection_indices.push(collected_buffers[&i]);
            } else {
                collected_buffers.insert(i, columns.len());
                projection_indices.push(columns.len());
                let data = mem::replace(self.buffer_mut(projection), RefCell::new(Data::empty(0)));
                columns.push(data.into_inner());
            }
        }
        let mut aggregation_indices = Vec::new();
        for &(aggregation, aggregator) in aggregations {
            let i = self.resolve(&aggregation);
            if collected_buffers.contains_key(&i) {
                aggregation_indices.push((collected_buffers[&i], aggregator));
            } else {
                collected_buffers.insert(i, columns.len());
                aggregation_indices.push((columns.len(), aggregator));
                let data = mem::replace(self.buffer_mut(aggregation), RefCell::new(Data::empty(0)));
                columns.push(data.into_inner());
            }
        }
        let mut ranking_indices = Vec::new();
        for &(ranking, desc) in rankings {
            let i = self.resolve(&ranking);
            if collected_buffers.contains_key(&i) {
                ranking_indices.push((collected_buffers[&i], desc));
            } else {
                collected_buffers.insert(i, columns.len());
                ranking_indices.push((columns.len(), desc));
                let data = mem::replace(self.buffer_mut(ranking), RefCell::new(Data::empty(0)));
                columns.push(data.into_inner());
            }
        }
        (columns, projection_indices, aggregation_indices, ranking_indices)
    }

    pub fn set_any(&mut self, index: BufferRef<Any>, vec: BoxedData<'a>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(vec);
    }

    pub fn set<T: VecData<T> + 'a>(&mut self, index: BufferRef<T>, vec: Vec<T>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(Data::owned(vec));
    }

    pub fn set_const<T: ScalarData<T> + 'a>(&mut self, index: BufferRef<Scalar<T>>, val: T) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(Data::scalar(val));
    }

    pub fn alias<T>(&mut self, original: BufferRef<T>, alias: BufferRef<T>) {
        // TODO(clemens): cycle check
        self.aliases[alias.i] = Some(original.i)
    }

    fn buffer<T>(&self, buffer: BufferRef<T>) -> &RefCell<BoxedData<'a>> {
        &self.buffers[self.resolve(&buffer)]
    }

    fn buffer_mut<T>(&mut self, buffer: BufferRef<T>) -> &mut RefCell<BoxedData<'a>> {
        let i = self.resolve(&buffer);
        assert!(!self.pinned[i], "Trying to mutably borrow pinned buffer {}", buffer);
        &mut self.buffers[i]
    }

    fn resolve<T>(&self, buffer: &BufferRef<T>) -> usize {
        let mut index = buffer.i;
        while let Some(i) = self.aliases[index] {
            index = i;
        }
        index
    }

    pub fn pin(&mut self, index: &BufferRef<Any>) {
        let i = self.resolve(index);
        self.pinned[i] = true;
    }

    pub unsafe fn unpin(&mut self, index: BufferRef<Any>) {
        let i = self.resolve(&index);
        self.pinned[i] = false;
    }

    pub fn collect_pinned(self) -> Vec<BoxedData<'a>> {
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

