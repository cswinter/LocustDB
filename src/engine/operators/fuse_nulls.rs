use bitvec::*;
use engine::*;
use std::i64;

pub struct FuseNullsI64 {
    pub input: BufferRef<Nullable<i64>>,
    pub fused: BufferRef<i64>,
}

impl<'a> VecOperator<'a> for FuseNullsI64 {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut fused = scratchpad.get_mut(self.fused);
        if stream { fused.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                fused.push(input[i]);
            } else {
                fused.push(i64::MIN);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.fused, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("FuseNullsI64({})", self.input)
    }
}

pub struct UnfuseNullsI64 {
    pub fused: BufferRef<i64>,
    pub present: BufferRef<u8>,
    pub unfused: BufferRef<Nullable<i64>>,
}

impl<'a> VecOperator<'a> for UnfuseNullsI64 {
    fn execute(&mut self, _stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let present = {
            let fused = scratchpad.get(self.fused);
            let mut present = vec![0u8; fused.len() / 8 + 1];
            for i in 0..fused.len() {
                if fused[i] != i64::MIN {
                    present.set(i);
                }
            }
            present
        };
        scratchpad.set(self.present, present);
    }

    fn init(&mut self, _: usize, _batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.assemble_nullable(self.fused, self.present, self.unfused);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unfused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("UnfuseNullsI64({})", self.fused)
    }
}

pub struct FuseNullsStr<'a> {
    pub input: BufferRef<Nullable<&'a str>>,
    pub fused: BufferRef<Option<&'a str>>,
}

impl<'a> VecOperator<'a> for FuseNullsStr<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut fused = scratchpad.get_mut(self.fused);
        if stream { fused.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                fused.push(Some(input[i]));
            } else {
                fused.push(None);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.fused, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("FuseNullsStr({})", self.fused)
    }
}

pub struct UnfuseNullsStr<'a> {
    pub fused: BufferRef<Option<&'a str>>,
    pub data: BufferRef<&'a str>,
    pub present: BufferRef<u8>,
    pub unfused: BufferRef<Nullable<&'a str>>,
}

impl<'a> VecOperator<'a> for UnfuseNullsStr<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let present = {
            let fused = scratchpad.get(self.fused);
            let mut data = scratchpad.get_mut(self.data);
            let mut present = vec![0; fused.len() / 8 + 1];
            if stream { data.clear() }
            for i in 0..fused.len() {
                data.push(fused[i].unwrap_or(""));
                if fused[i].is_some() {
                    present.set(i);
                }
            }
            present
        };
        scratchpad.set(self.present, present);
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.assemble_nullable(self.data, self.present, self.unfused);
        scratchpad.set(self.data, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unfused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("UnfuseNullsStr({})", self.fused)
    }
}

pub struct FuseIntNulls<T> {
    pub offset: T,
    pub input: BufferRef<Nullable<T>>,
    pub fused: BufferRef<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for FuseIntNulls<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut fused = scratchpad.get_mut(self.fused);
        if stream { fused.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                fused.push(input[i] + self.offset);
            } else {
                fused.push(T::zero());
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.fused, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, full: bool) -> String {
        if full {
            format!("FuseIntNulls<{:?}>({}, {:?})", T::t(), self.input, self.offset)
        } else {
            format!("FuseIntNulls<{:?}>({})", T::t(), self.input)
        }
    }
}

pub struct UnfuseIntNulls<T> {
    pub offset: T,
    pub fused: BufferRef<T>,
    pub data: BufferRef<T>,
    pub present: BufferRef<u8>,
    pub unfused: BufferRef<Nullable<T>>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for UnfuseIntNulls<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let present = {
            let fused = scratchpad.get(self.fused);
            let mut data = scratchpad.get_mut(self.data);
            let mut present = vec![0; fused.len() / 8 + 1];
            if stream { data.clear() }
            for i in 0..fused.len() {
                if fused[i] == T::zero() {
                    data.push(T::zero());
                } else {
                    data.push(fused[i] - self.offset);
                    present.set(i);
                }
            }
            present
        };
        scratchpad.set(self.present, present);
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.assemble_nullable(self.data, self.present, self.unfused);
        scratchpad.set(self.data, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unfused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("UnfuseNulls<{:?}>({})", T::t(), self.fused)
    }
}

