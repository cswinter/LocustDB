use engine::*;
use std::borrow::BorrowMut;
use std::cell::*;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;

pub struct Scratchpad<'a> {
    buffers: Vec<RefCell<BoxedData<'a>>>,
    aliases: Vec<Option<usize>>,
    null_maps: Vec<Option<usize>>,
    columns: HashMap<String, Vec<&'a dyn Data<'a>>>,
    pinned: Vec<bool>,
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize, columns: HashMap<String, Vec<&'a dyn Data<'a>>>) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(Data::empty(0)));
        }
        Scratchpad {
            buffers,
            aliases: vec![None; count],
            null_maps: vec![None; count],
            columns,
            pinned: vec![false; count],
        }
    }

    pub fn get_any(&self, index: BufferRef<Any>) -> Ref<dyn Data<'a>> {
        Ref::map(self.buffer(index).borrow(), |x| x.as_ref())
    }

    pub fn get_any_mut(&self, index: BufferRef<Any>) -> RefMut<dyn Data<'a> + 'a> {
        assert!(!self.pinned[self.resolve(&index)], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffer(index).borrow_mut(), |x| x.borrow_mut())
    }

    pub fn get_column_data(&self, name: &str, section_index: usize) -> &'a dyn Data<'a> {
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
            #[allow(clippy::transmute_ptr_to_ptr)]
            mem::transmute::<&str, &'a str>(any.cast_ref_scalar_string())
        }
    }

    pub fn get_mut<T: VecData<T> + 'a>(&self, index: BufferRef<T>) -> RefMut<Vec<T>> {
        assert!(!self.pinned[self.resolve(&index)], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[self.resolve(&index)].borrow_mut(), |x| {
            let a: &mut dyn Data<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_mut_val_rows(&self, index: BufferRef<ValRows<'a>>) -> RefMut<ValRows<'a>> {
        RefMut::map(self.get_any_mut(index.any()), |x| x.cast_ref_mut_val_rows())
    }

    pub fn get_mut_nullable<T: VecData<T> + 'a>(&self, index: BufferRef<Nullable<T>>) -> (RefMut<Vec<T>>, RefMut<Vec<u8>>) {
        (self.get_mut(index.cast_non_nullable()),
         self.get_mut(BufferRef {
             i: self.null_maps[index.i].unwrap(),
             name: "null_map",
             t: PhantomData::<u8>,
         }))
    }

    pub fn get_data_mut<T: VecData<T> + 'a>(&self, index: BufferRef<Nullable<T>>) -> RefMut<Vec<T>> {
        self.get_mut(index.cast_non_nullable())
    }

    pub fn get_scalar<T: ScalarData<T>>(&self, index: &BufferRef<Scalar<T>>) -> T {
        T::unwrap(&*self.get_any(index.any()))
    }

    pub fn get_nullable<T: VecData<T> + 'a>(&self, index: BufferRef<Nullable<T>>) -> (Ref<[T]>, Ref<[u8]>) {
        let data = self.get(index.cast_non_nullable());
        let present = self.get_null_map(index.nullable_any());
        (data, present)
    }

    pub fn get_null_map(&self, index: BufferRef<Nullable<Any>>) -> Ref<[u8]> {
        match self.null_maps[index.i] {
            Some(null_map_index) => {
                let present_index = BufferRef {
                    i: null_map_index,
                    name: "null_map",
                    t: PhantomData::<u8>,
                };
                self.get(present_index)
            }
            None => Ref::map(self.get_any(index.any()), |x| x.cast_ref_null_map()),
        }
    }

    pub fn alias_null_map(&mut self, index: BufferRef<Nullable<Any>>, target: BufferRef<u8>) {
        match self.null_maps[index.i] {
            Some(null_map_index) => self.aliases[target.i] = Some(null_map_index),
            None => panic!("No null map"),
        }
    }

    pub fn try_get_null_map(&self, index: BufferRef<Any>) -> Option<Ref<[u8]>> {
        match self.null_maps[index.i] {
            Some(null_map_index) => {
                let present_index = BufferRef {
                    i: null_map_index,
                    name: "null_map",
                    t: PhantomData::<u8>,
                };
                Some(self.get(present_index))
            }
            None => None,
        }
    }

    // TODO: return struct
    #[allow(clippy::type_complexity, clippy::map_entry)]
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
                columns.push(self.collect_one(projection));
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
                columns.push(self.collect_one(aggregation));
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
                columns.push(self.collect_one(ranking));
            }
        }
        (columns, projection_indices, aggregation_indices, ranking_indices)
    }

    fn collect_one(&mut self, buffer: BufferRef<Any>) -> BoxedData<'a> {
        let mut data = mem::replace(self.buffer_mut(buffer), RefCell::new(Data::empty(0))).into_inner();
        match self.null_maps[buffer.i] {
            Some(index) => data.make_nullable(&*self.get(BufferRef { i: index, name: "present", t: PhantomData::<u8> })),
            None => data,
        }
    }

    pub fn set_any(&mut self, index: BufferRef<Any>, vec: BoxedData<'a>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(vec);
    }

    pub fn set<T: VecData<T> + 'a>(&mut self, index: BufferRef<T>, vec: Vec<T>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(Data::owned(vec));
    }

    pub fn set_nullable<T: VecData<T> + 'a>(&mut self, index: BufferRef<Nullable<T>>, data: Vec<T>, present: Vec<u8>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        match self.null_maps[index.i] {
            Some(nm_index) => *self.buffer_mut(BufferRef {
                i: nm_index,
                name: "present",
                t: PhantomData::<u8>,
            }) = RefCell::new(Box::new(present)),
            None => {
                self.buffers.push(RefCell::new(Box::new(present)));
                self.aliases.push(None);
                self.pinned.push(false);
                self.null_maps.push(None);
                self.null_maps[index.i] = Some(self.buffers.len() - 1);
            }
        }
        *self.buffer_mut(index) = RefCell::new(Box::new(data));
    }

    pub fn set_data<T: VecData<T> + 'a>(&mut self, index: BufferRef<Nullable<T>>, vec: Vec<T>) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(Data::owned(vec));
    }

    pub fn set_const<T: ScalarData<T> + 'a>(&mut self, index: BufferRef<Scalar<T>>, val: T) {
        assert!(!self.pinned[self.resolve(&index)], "Trying to set pinned buffer {}", index);
        *self.buffer_mut(index) = RefCell::new(Data::scalar(val));
    }

    pub fn alias<T>(&mut self, original: BufferRef<T>, alias: BufferRef<T>) {
        // should probably do cycle check
        self.aliases[alias.i] = Some(original.i);
        self.null_maps[alias.i] = self.null_maps[original.i];
    }

    pub fn assemble_nullable<T>(&mut self,
                                original: BufferRef<T>,
                                null_map: BufferRef<u8>,
                                nullable: BufferRef<Nullable<T>>) {
        // should probably do cycle check
        self.aliases[nullable.i] = Some(original.i);
        self.null_maps[nullable.i] = Some(null_map.i);
    }

    pub fn propagate_null_map<T, U>(&mut self,
                                    from: BufferRef<Nullable<T>>,
                                    to: BufferRef<Nullable<U>>) {
        self.null_maps[to.i] = self.null_maps[from.i];
    }

    pub fn set_null_map<T>(&mut self,
                           nullable: BufferRef<Nullable<T>>,
                           null_map: BufferRef<u8>) {
        self.null_maps[nullable.i] = Some(null_map.i);
    }

    pub fn reassemble_nullable<T>(&mut self,
                                  nullable_in: BufferRef<Nullable<Any>>,
                                  data: BufferRef<T>,
                                  nullable: BufferRef<Nullable<T>>) {
        self.aliases[nullable.i] = Some(data.i);
        self.null_maps[nullable.i] = self.null_maps[nullable_in.i];
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

