use std::marker::PhantomData;

use engine::*;


pub struct BinaryOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<RHS>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for BinaryOperator<LHS, RHS, Out, Op>
    where LHS: GenericVec<LHS> + 'a,
          RHS: GenericVec<RHS> + 'a,
          Out: GenericVec<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for (l, r) in lhs.iter().zip(rhs.iter()) {
            output.push(Op::perform(*l, *r));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct BinaryVSOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<Scalar<RHS>>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for BinaryVSOperator<LHS, RHS, Out, Op>
    where LHS: GenericVec<LHS> + 'a,
          RHS: ConstType<RHS> + Copy + 'a,
          Out: GenericVec<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get_scalar(&self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for &l in lhs.iter() {
            output.push(Op::perform(l, rhs));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct BinarySVOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<Scalar<LHS>>,
    pub rhs: BufferRef<RHS>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for BinarySVOperator<LHS, RHS, Out, Op>
    where LHS: ConstType<LHS> + Copy + 'a,
          RHS: GenericVec<RHS> + 'a,
          Out: GenericVec<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let lhs = scratchpad.get_scalar(&self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for &r in rhs.iter() {
            output.push(Op::perform(lhs, r));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}


pub trait BinaryOp<LHS, RHS, Out> {
    fn perform(lhs: LHS, rhs: RHS) -> Out;
    fn symbol() -> &'static str;
}
