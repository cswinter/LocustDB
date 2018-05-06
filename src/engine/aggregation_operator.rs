use std::cell::Ref;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::vector_op::types::*;
use ingest::raw_val::RawVal;
use seahash::SeaHasher;

type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;


#[derive(Debug)]
pub struct Unique<T> {
    input: BufferRef,
    output: BufferRef,
    max_index: usize,
    t: PhantomData<T>,
}

impl<T: IntVecType<T> + IntoUsize> Unique<T> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef, max_index: usize) -> BoxedOperator<'a> {
        Box::new(Unique::<T> { input, output, max_index, t: PhantomData })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for Unique<T> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let mut seen_before = vec![false; self.max_index + 1];
            let mut result = Vec::new();
            let data = Ref::map(scratchpad.get(self.input), T::unwrap);
            for &i in data.iter() {
                let index = i.cast_usize();
                if !seen_before[index] {
                    result.push(i);
                    seen_before[index] = true;
                }
            }
            result.sort();
            T::wrap(result)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct HashMapGrouping<T> {
    input: BufferRef,
    unique_out: BufferRef,
    grouping_key_out: BufferRef,
    cardinality_out: BufferRef,
    t: PhantomData<T>,
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
            t: PhantomData
        })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for HashMapGrouping<T> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let (unique, grouping_key, cardinality) = {
            let mut count = T::zero();
            let grouping_key = Ref::map(scratchpad.get(self.input), T::unwrap);
            let mut grouping = Vec::with_capacity(grouping_key.len());
            let mut groups = Vec::new();
            let mut map = HashMapSea::default();
            for i in grouping_key.iter() {
                grouping.push(*map.entry(i).or_insert_with(|| {
                    groups.push(*i);
                    let old = count;
                    count = count + T::one();
                    old
                }));
            }
            (grouping, groups, count.to_usize().unwrap())
        };
        scratchpad.set(self.unique_out, T::wrap(unique));
        scratchpad.set(self.grouping_key_out, T::wrap(grouping_key));
        scratchpad.set(self.cardinality_out, TypedVec::Constant(RawVal::Int(cardinality as i64)));
    }
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
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let mut result = vec![0; self.max_index + 1];
            let g = scratchpad.get(self.grouping);
            let grouping = T::unwrap(&g);
            for i in grouping {
                result[i.cast_usize()] += 1;
            }
            if !self.dense_grouping {
                // Remove 0 counts for all entries that weren't present in grouping
                let mut j = 0;
                for i in 0..result.len() {
                    if result[i] > 0 {
                        result[j] = result[i];
                        j += 1;
                    }
                }
                result.truncate(j);
            }
            TypedVec::Integer(result)
        };
        scratchpad.set(self.output, result)
    }
}

#[derive(Debug)]
pub struct VecSum<T, U> {
    input: BufferRef,
    grouping: BufferRef,
    output: BufferRef,
    max_index: usize,
    dense_grouping: bool,
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
            t: PhantomData,
            u: PhantomData,
        })
    }
}

impl<'a, T, U> VecOperator<'a> for VecSum<T, U> where
    T: IntVecType<T>, U: VecType<U> + IntoUsize {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            // TODO(clemens): this is already computed in unique function, we should just reuse
            let mut modified = vec![false; self.max_index + 1];
            let input = scratchpad.get(self.input);
            let g = scratchpad.get(self.grouping);
            let nums = T::unwrap(&input);
            let grouping = U::unwrap(&g);
            let mut result = vec![0; self.max_index + 1];
            for (i, n) in grouping.iter().zip(nums) {
                result[i.cast_usize()] += Into::<i64>::into(*n);
                modified[i.cast_usize()] = true;
            }
            if !self.dense_grouping {
                // Remove 0 counts for all entries that weren't present in grouping
                let mut j = 0;
                for i in 0..result.len() {
                    if modified[i] {
                        result[j] = result[i];
                        j += 1;
                    }
                }
                result.truncate(j);
            }

            TypedVec::Integer(result)
        };
        scratchpad.set(self.output, result);
    }
}
