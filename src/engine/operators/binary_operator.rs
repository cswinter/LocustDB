use crate::bitvec::BitVec;
use crate::engine::*;
use std::marker::PhantomData;

pub struct BinaryOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<RHS>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for BinaryOperator<LHS, RHS, Out, Op>
    where LHS: VecData<LHS> + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for (l, r) in lhs.iter().zip(rhs.iter()) {
            output.push(Op::perform(*l, *r));
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
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
    where LHS: VecData<LHS> + 'a,
          RHS: ScalarData<RHS> + Copy + 'a,
          Out: VecData<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get_scalar(&self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for &l in lhs.iter() {
            output.push(Op::perform(l, rhs));
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
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
    where LHS: ScalarData<LHS> + Copy + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: BinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get_scalar(&self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for &r in rhs.iter() {
            output.push(Op::perform(lhs, r));
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}


pub struct CheckedBinaryOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<RHS>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for CheckedBinaryOperator<LHS, RHS, Out, Op>
    where LHS: VecData<LHS> + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for (l, r) in lhs.iter().zip(rhs.iter()) {
            let (result, overflow) = Op::perform_checked(*l, *r);
            any_overflow |= overflow;
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct CheckedBinaryVSOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<Scalar<RHS>>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for CheckedBinaryVSOperator<LHS, RHS, Out, Op>
    where LHS: VecData<LHS> + 'a,
          RHS: ScalarData<RHS> + Copy + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get_scalar(&self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for &l in lhs.iter() {
            let (result, overflow) = Op::perform_checked(l, rhs);
            any_overflow |= overflow;
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct CheckedBinarySVOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<Scalar<LHS>>,
    pub rhs: BufferRef<RHS>,
    pub output: BufferRef<Out>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for CheckedBinarySVOperator<LHS, RHS, Out, Op>
    where LHS: ScalarData<LHS> + Copy + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get_scalar(&self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for &r in rhs.iter() {
            let (result, overflow) = Op::perform_checked(lhs, r);
            any_overflow |= overflow;
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}


pub struct NullableCheckedBinaryOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<RHS>,
    pub present: BufferRef<u8>,
    pub output: BufferRef<Nullable<Out>>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for NullableCheckedBinaryOperator<LHS, RHS, Out, Op>
    where LHS: VecData<LHS> + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let present = scratchpad.get(self.present);
        let mut output = scratchpad.get_data_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for (i, (l, r)) in lhs.iter().zip(rhs.iter()).enumerate() {
            let (result, overflow) = Op::perform_checked(*l, *r);
            any_overflow |= overflow && (&*present).is_set(i);
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_data(self.output, Vec::with_capacity(batch_size));
        scratchpad.set_null_map(self.output, self.present);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any(), self.present.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i, &mut self.present.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct NullableCheckedBinaryVSOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<LHS>,
    pub rhs: BufferRef<Scalar<RHS>>,
    pub present: BufferRef<u8>,
    pub output: BufferRef<Nullable<Out>>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for NullableCheckedBinaryVSOperator<LHS, RHS, Out, Op>
    where LHS: VecData<LHS> + 'a,
          RHS: ScalarData<RHS> + Copy + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get_scalar(&self.rhs);
        let present = scratchpad.get(self.present);
        let mut output = scratchpad.get_data_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for (i, &l) in lhs.iter().enumerate() {
            let (result, overflow) = Op::perform_checked(l, rhs);
            any_overflow |= overflow && (&*present).is_set(i);
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_data(self.output, Vec::with_capacity(batch_size));
        scratchpad.set_null_map(self.output, self.present);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any(), self.present.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i, &mut self.present.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub struct NullableCheckedBinarySVOperator<LHS, RHS, Out, Op> {
    pub lhs: BufferRef<Scalar<LHS>>,
    pub rhs: BufferRef<RHS>,
    pub present: BufferRef<u8>,
    pub output: BufferRef<Nullable<Out>>,
    pub op: PhantomData<Op>,
}

impl<'a, LHS, RHS, Out, Op> VecOperator<'a> for NullableCheckedBinarySVOperator<LHS, RHS, Out, Op>
    where LHS: ScalarData<LHS> + Copy + 'a,
          RHS: VecData<RHS> + 'a,
          Out: VecData<Out> + 'a,
          Op: CheckedBinaryOp<LHS, RHS, Out> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let lhs = scratchpad.get_scalar(&self.lhs);
        let rhs = scratchpad.get(self.rhs);
        let present = scratchpad.get(self.present);
        let mut output = scratchpad.get_data_mut(self.output);
        if stream { output.clear(); }
        let mut any_overflow = false;
        for (i, &r) in rhs.iter().enumerate() {
            let (result, overflow) = Op::perform_checked(lhs, r);
            any_overflow |= overflow && (&*present).is_set(i);
            output.push(result);
        }
        if any_overflow { Err(QueryError::Overflow) } else { Ok(()) }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_data(self.output, Vec::with_capacity(batch_size));
        scratchpad.set_null_map(self.output, self.present);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any(), self.present.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i, &mut self.present.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}


pub trait BinaryOp<LHS, RHS, Out> {
    fn perform(lhs: LHS, rhs: RHS) -> Out;
    fn symbol() -> &'static str;
}

pub trait CheckedBinaryOp<LHS, RHS, Out>: BinaryOp<LHS, RHS, Out> {
    fn perform_checked(lhs: LHS, rhs: RHS) -> (Out, bool);
}
