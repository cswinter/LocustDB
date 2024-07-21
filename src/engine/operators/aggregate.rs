use ordered_float::OrderedFloat;

use crate::bitvec::{BitVec, BitVecMut};
use crate::engine::*;
use std::i64;
use std::marker::PhantomData;

pub trait Aggregator<T, Acc> {
    fn unit() -> Acc;
    fn accumulate(accumulator: Acc, value: T) ->Acc;
    #[allow(dead_code)]
    fn combine(accumulator1: Acc, accumulator2: Acc) -> Acc;
}

pub trait CheckedAggregator<T, Acc>: Aggregator<T, Acc> {
    fn accumulate_checked(accumulator: Acc, value: T) -> (Acc, bool);
    #[allow(dead_code)]
    fn combine_checked(accumulator1: Acc, accumulator2: Acc) -> (Acc, bool);
}

pub struct SumI64;

impl<T> Aggregator<T, i64> for SumI64 where T: Into<i64> {
    fn unit() -> i64 { 0 }
    #[inline]
    fn accumulate(accumulator: i64, value: T) -> i64 { accumulator + value.into() }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { accumulator1 + accumulator2 }
}

impl<T> CheckedAggregator<T, i64> for SumI64 where T: Into<i64> {
    #[inline]
    fn accumulate_checked(acc: i64, value: T) -> (i64, bool) { acc.overflowing_add(value.into()) }
    #[inline]
    fn combine_checked(acc1: i64, acc2: i64) -> (i64, bool) { acc1.overflowing_add(acc2) }
}

pub struct SumF64;

impl<T> Aggregator<T, OrderedFloat<f64>> for SumF64 where T: Into<OrderedFloat<f64>> {
    fn unit() -> OrderedFloat<f64> { OrderedFloat(0.0) }
    #[inline]
    fn accumulate(accumulator: OrderedFloat<f64>, value: T) -> OrderedFloat<f64> { accumulator + value.into() }
    #[inline]
    fn combine(accumulator1: OrderedFloat<f64>, accumulator2: OrderedFloat<f64>) -> OrderedFloat<f64> { accumulator1 + accumulator2 }
}

pub struct Count;

impl<V> Aggregator<V, u32> for Count {
    fn unit() -> u32 { 0 }
    #[inline]
    fn accumulate(accumulator: u32, _: V) -> u32 { accumulator + 1 }
    #[inline]
    fn combine(accumulator1: u32, accumulator2: u32) -> u32 { accumulator1 + accumulator2 }
}

pub struct MaxI64;

impl<V> Aggregator<V, i64> for MaxI64 where V: Into<i64> {
    fn unit() -> i64 { i64::MIN }
    #[inline]
    fn accumulate(accumulator: i64, value: V) -> i64 { std::cmp::max(accumulator, value.into()) }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { std::cmp::max(accumulator1, accumulator2) }
}

pub struct MaxF64;

impl<V> Aggregator<V, OrderedFloat<f64>> for MaxF64 where V: Into<OrderedFloat<f64>> {
    fn unit() -> OrderedFloat<f64> { OrderedFloat(f64::MIN) }
    #[inline]
    fn accumulate(accumulator: OrderedFloat<f64>, value: V) -> OrderedFloat<f64> { std::cmp::max(accumulator, value.into()) }
    #[inline]
    fn combine(accumulator1: OrderedFloat<f64>, accumulator2: OrderedFloat<f64>) -> OrderedFloat<f64> { std::cmp::max(accumulator1, accumulator2) }
}

pub struct MinI64;

impl<V> Aggregator<V, i64> for MinI64 where V: Into<i64> {
    fn unit() -> i64 { i64::MAX }
    #[inline]
    fn accumulate(accumulator: i64, value: V) -> i64 { std::cmp::min(accumulator, value.into()) }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { std::cmp::min(accumulator1, accumulator2) }
}

pub struct MinF64;

impl<V> Aggregator<V, OrderedFloat<f64>> for MinF64 where V: Into<OrderedFloat<f64>> {
    fn unit() -> OrderedFloat<f64> { OrderedFloat(f64::MAX) }
    #[inline]
    fn accumulate(accumulator: OrderedFloat<f64>, value: V) -> OrderedFloat<f64> { std::cmp::min(accumulator, value.into()) }
    #[inline]
    fn combine(accumulator1: OrderedFloat<f64>, accumulator2: OrderedFloat<f64>) -> OrderedFloat<f64> { std::cmp::min(accumulator1, accumulator2) }
}


pub struct Aggregate<T, U, V, A> {
    pub input: BufferRef<T>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<V>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: Aggregator<T, V>> VecOperator<'a> for Aggregate<T, U, V, A> where
    T: VecData<T> + 'static, U: GenericIntVec<U>, V: VecData<V> + 'static {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let nums = scratchpad.get(self.input);
        let grouping = scratchpad.get(self.grouping);
        let mut accumulators = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
        }

        for (i, n) in grouping.iter().zip(nums.iter()) {
            let i = i.cast_usize();
            accumulators[i] = A::accumulate(accumulators[i], *n);
        }

        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.grouping.i, &mut self.input.i, &mut self.max_index.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}

pub struct AggregateNullable<T, U, V, A> where A: Aggregator<T, V> {
    pub input: BufferRef<Nullable<T>>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<Nullable<V>>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: Aggregator<T, V>> VecOperator<'a> for AggregateNullable<T, U, V, A> where
    T: VecData<T> + 'static, U: GenericIntVec<U>, V: VecData<V> + 'static {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (nums, present) = scratchpad.get_nullable(self.input);
        let grouping = scratchpad.get(self.grouping);
        let (mut accumulators, mut accumulators_present) = scratchpad.get_mut_nullable(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
            accumulators_present.resize((len + 7) / 8, 0);
        }

        for i in 0..nums.len() {
            if (&*present).is_set(i) {
                let g = grouping[i].cast_usize();
                accumulators[g] = A::accumulate(accumulators[g], nums[i]);
                accumulators_present.set(g);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(0), Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.grouping.i, &mut self.input.i, &mut self.max_index.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}

pub struct CheckedAggregate<T, U, V, A> {
    pub input: BufferRef<T>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<V>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: CheckedAggregator<T, V>> VecOperator<'a> for CheckedAggregate<T, U, V, A> where
    T: GenericIntVec<T> + Into<V>, U: GenericIntVec<U>, V: GenericIntVec<V> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let nums = scratchpad.get(self.input);
        let grouping = scratchpad.get(self.grouping);
        let mut accumulators = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
        }

        let mut any_overflow = false;
        for (i, n) in grouping.iter().zip(nums.iter()) {
            let i = i.cast_usize();
            let (result, overflow) = A::accumulate_checked(accumulators[i], *n);
            any_overflow |= overflow;
            accumulators[i] = result;
        }

        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.grouping.i, &mut self.input.i, &mut self.max_index.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}

pub struct CheckedAggregateNullable<T, U, V, A> {
    pub input: BufferRef<Nullable<T>>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<Nullable<V>>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: CheckedAggregator<T, V>> VecOperator<'a> for CheckedAggregateNullable<T, U, V, A> where
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U>, V: GenericIntVec<V> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (nums, present) = scratchpad.get_nullable(self.input);
        let grouping = scratchpad.get(self.grouping);
        let (mut accumulators, mut accumulators_present) = scratchpad.get_mut_nullable(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
        }

        let mut any_overflow = false;
        for i in 0..nums.len() {
            if (&*present).is_set(i) {
                let g = grouping[i].cast_usize();
                let (result, overflow) = A::accumulate_checked(accumulators[g], nums[i]);
                any_overflow |= overflow;
                accumulators[g] = result;
                accumulators_present.set(g);
            }
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(0), Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.grouping.i, &mut self.input.i, &mut self.max_index.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}
