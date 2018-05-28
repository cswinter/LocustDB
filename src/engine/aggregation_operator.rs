use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;
use ingest::raw_val::RawVal;
use seahash::SeaHasher;

type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;


#[derive(Debug)]
pub struct Unique<T> {
    input: BufferRef,
    output: BufferRef,
    max_index: usize,
    seen_before: Vec<bool>,
    t: PhantomData<T>,
}

impl<T: IntVecType<T> + IntoUsize> Unique<T> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef, max_index: usize) -> BoxedOperator<'a> {
        Box::new(Unique::<T> { input, output, max_index, seen_before: Vec::new(), t: PhantomData })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for Unique<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let mut unique = scratchpad.get_mut::<T>(self.output);
        for &i in data.iter() {
            let index = i.cast_usize();
            if !self.seen_before[index] {
                unique.push(i);
                self.seen_before[index] = true;
            }
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let mut unique = scratchpad.get_mut::<T>(self.output);
        unique.sort();
        trace!("unique: {:?}", &unique);
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        self.seen_before = vec![false; self.max_index + 1];
        // TODO(clemens): output size estimate?
        scratchpad.set(self.output, TypedVec::owned(Vec::<T>::new()));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}

#[derive(Debug)]
pub struct HashMapGrouping<T: IntVecType<T>> {
    input: BufferRef,
    unique_out: BufferRef,
    grouping_key_out: BufferRef,
    cardinality_out: BufferRef,
    map: HashMapSea<T, T>,
}

impl<T: IntVecType<T> + IntoUsize> HashMapGrouping<T> {
    pub fn boxed<'a>(input: BufferRef,
                     unique_out: BufferRef,
                     grouping_key_out: BufferRef,
                     cardinality_out: BufferRef,
                     _max_index: usize) -> BoxedOperator<'a> {
        Box::new(HashMapGrouping::<T> {
            input,
            unique_out,
            grouping_key_out,
            cardinality_out,
            map: HashMapSea::default(),
        })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for HashMapGrouping<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let raw_grouping_key = scratchpad.get::<T>(self.input);
        let mut grouping = scratchpad.get_mut::<T>(self.grouping_key_out);
        let mut unique = scratchpad.get_mut::<T>(self.unique_out);
        for i in raw_grouping_key.iter() {
            grouping.push(*self.map.entry(*i).or_insert_with(|| {
                unique.push(*i);
                T::from(unique.len()).unwrap() - T::one()
            }));
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let count = scratchpad.get::<T>(self.unique_out).len();
        scratchpad.set(self.cardinality_out, TypedVec::constant(RawVal::Int(count as i64)));
    }
    fn init(&mut self, total_count: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): Estimate capacities for unique + map?
        scratchpad.set(self.unique_out, TypedVec::owned(Vec::<T>::new()));
        scratchpad.set(self.grouping_key_out, TypedVec::owned(Vec::<T>::with_capacity(total_count)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.unique_out, self.grouping_key_out, self.cardinality_out] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}

#[derive(Debug)]
pub struct VecCount<T> {
    grouping: BufferRef,
    output: BufferRef,
    max_index: usize,
    dense_grouping: bool,
    t: PhantomData<T>,
}

impl<T> VecCount<T> {
    pub fn new(grouping: BufferRef, output: BufferRef, max_index: usize, dense_grouping: bool) -> VecCount<T> {
        VecCount {
            grouping,
            output,
            max_index,
            dense_grouping,
            t: PhantomData,
        }
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for VecCount<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut::<u32>(self.output);
        let grouping = scratchpad.get::<T>(self.grouping);
        for i in grouping.iter() {
            result[i.cast_usize()] += 1;
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        if !self.dense_grouping {
            let mut result = scratchpad.get_mut::<u32>(self.output);
            // Remove 0 counts for all entries that weren't present in grouping
            let mut j = 0;
            for i in 0..result.len() {
                if result[i] > 0 {
                    result[j] = result[i];
                    j += 1;
                }
            }
            result.truncate(j);
            trace!("Vec count: {:?}", &result);
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, TypedVec::owned(vec![0u32; self.max_index + 1]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}

#[derive(Debug)]
pub struct VecSum<T, U> {
    input: BufferRef,
    grouping: BufferRef,
    output: BufferRef,
    max_index: usize,
    dense_grouping: bool,
    modified: Vec<bool>,
    t: PhantomData<T>,
    u: PhantomData<U>,
}

impl<T, U> VecSum<T, U> where
    T: IntVecType<T>, U: IntVecType<U> + IntoUsize {
    pub fn boxed<'a>(input: BufferRef, grouping: BufferRef, output: BufferRef, max_index: usize, dense_grouping: bool) -> BoxedOperator<'a> {
        Box::new(VecSum::<T, U> {
            input,
            grouping,
            output,
            max_index,
            dense_grouping,
            modified: Vec::with_capacity(0),
            t: PhantomData,
            u: PhantomData,
        })
    }
}

impl<'a, T, U> VecOperator<'a> for VecSum<T, U> where
    T: IntVecType<T>, U: IntVecType<U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): this is already computed in unique function, we should just reuse
        let nums = scratchpad.get::<T>(self.input);
        let grouping = scratchpad.get::<U>(self.grouping);
        let mut sums = scratchpad.get_mut::<i64>(self.output);
        for (i, n) in grouping.iter().zip(nums.iter()) {
            sums[i.cast_usize()] += Into::<i64>::into(*n);
            self.modified[i.cast_usize()] = true;
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        if !self.dense_grouping {
            let mut sums = scratchpad.get_mut::<i64>(self.output);
            // Remove all unmodified entries
            let mut j = 0;
            for i in 0..sums.len() {
                if self.modified[i] {
                    sums[j] = sums[i];
                    j += 1;
                }
            }
            sums.truncate(j);
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        self.modified = vec![false; self.max_index + 1];
        scratchpad.set(self.output, TypedVec::owned(vec![0 as i64; self.max_index + 1]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping, self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}
