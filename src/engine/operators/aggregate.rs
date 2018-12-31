use engine::*;
use bitvec::BitVec;
use std::marker::PhantomData;
use std::i64;

pub trait Aggregator<T> {
    fn unit() -> T;
    #[inline]
    fn accumulate(accumulator: T, value: i64) -> T;
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64;
}


pub struct Sum;

impl Aggregator<i64> for Sum {
    fn unit() -> i64 { 0 }
    #[inline]
    fn accumulate(accumulator: i64, value: i64) -> i64 { accumulator + value }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { accumulator1 + accumulator2 }
}


pub struct Count;

impl Aggregator<u32> for Count {
    fn unit() -> u32 { 0 }
    #[inline]
    fn accumulate(accumulator: u32, _: i64) -> u32 { accumulator + 1 }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { accumulator1 + accumulator2 }
}

pub struct Max;

impl Aggregator<i64> for Max {
    fn unit() -> i64 { i64::MIN }
    #[inline]
    fn accumulate(accumulator: i64, value: i64) -> i64 { std::cmp::max(accumulator, value) }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { std::cmp::max(accumulator1, accumulator2) }
}

pub struct Min;

impl Aggregator<i64> for Min {
    fn unit() -> i64 { i64::MAX }
    #[inline]
    fn accumulate(accumulator: i64, value: i64) -> i64 { std::cmp::min(accumulator, value) }
    #[inline]
    fn combine(accumulator1: i64, accumulator2: i64) -> i64 { std::cmp::min(accumulator1, accumulator2) }
}


pub struct Aggregate<T, U, V, A> {
    pub input: BufferRef<T>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<V>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: Aggregator<V>> VecOperator<'a> for Aggregate<T, U, V, A> where
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U>, V: GenericIntVec<V> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let nums = scratchpad.get(self.input);
        let grouping = scratchpad.get(self.grouping);
        let mut accumulators = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
        }

        for (i, n) in grouping.iter().zip(nums.iter()) {
            let i = i.cast_usize();
            accumulators[i] = A::accumulate(accumulators[i], (*n).into());
        }

        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}

pub struct AggregateNullable<T, U, V, A> {
    pub input: BufferRef<Nullable<T>>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<V>,
    pub max_index: BufferRef<Scalar<i64>>,
    pub a: PhantomData<A>,
}

impl<'a, T, U, V, A: Aggregator<V>> VecOperator<'a> for AggregateNullable<T, U, V, A> where
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U>, V: GenericIntVec<V> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let (nums, present) = scratchpad.get_nullable(self.input);
        let grouping = scratchpad.get(self.grouping);
        let mut accumulators = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > accumulators.len() {
            accumulators.resize(len, A::unit());
        }

        for i in 0..nums.len() {
            if (&*present).is_set(i) {
                let g = grouping[i].cast_usize();
                accumulators[g] = A::accumulate(accumulators[g], nums[i].into());
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}
