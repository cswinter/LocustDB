use bitvec::*;
use engine::*;
use mem_store::Val;

pub struct NullableStrToVal<'a> {
    pub input: BufferRef<Nullable<&'a str>>,
    pub vals: BufferRef<Val<'a>>,
}

impl<'a> VecOperator<'a> for NullableStrToVal<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut vals = scratchpad.get_mut(self.vals);
        if stream { vals.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                vals.push(Val::Str(input[i]));
            } else {
                vals.push(Val::Null);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.vals, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.vals.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("NullableStrToVal({})", self.vals)
    }
}

pub struct ValToNullableStr<'a> {
    pub vals: BufferRef<Val<'a>>,
    pub nullable: BufferRef<Nullable<&'a str>>,
}

impl<'a> VecOperator<'a> for ValToNullableStr<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let vals = scratchpad.get(self.vals);
        let (mut data, mut present) = scratchpad.get_mut_nullable(self.nullable);
        if stream {
            data.clear();
            present.clear();
        }
        for (i, &val) in vals.iter().enumerate() {
            match val {
                Val::Str(s) => {
                    data.push(s);
                    present.set(i);
                }
                Val::Null => data.push(""),
                _ => panic!("Trying to cast {:?} to NullableStr!", val),
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.nullable, Vec::with_capacity(batch_size), Vec::with_capacity(batch_size / 8 + 1));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.vals.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.nullable.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("ValToNullableStr({})", self.vals)
    }
}

pub struct NullableIntToVal<'a, T> {
    pub input: BufferRef<Nullable<T>>,
    pub vals: BufferRef<Val<'a>>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for NullableIntToVal<'a, T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut vals = scratchpad.get_mut(self.vals);
        if stream { vals.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                vals.push(Val::Integer(num::cast(input[i]).unwrap()));
            } else {
                vals.push(Val::Null);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.vals, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.vals.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("NullableIntToVal<{:?}>({})", T::t(), self.vals)
    }
}

pub struct ValToNullableInt<'a, T> {
    pub vals: BufferRef<Val<'a>>,
    pub nullable: BufferRef<Nullable<T>>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for ValToNullableInt<'a, T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let vals = scratchpad.get(self.vals);
        let (mut data, mut present) = scratchpad.get_mut_nullable(self.nullable);
        if stream {
            data.clear();
            present.clear();
        }
        for (i, &val) in vals.iter().enumerate() {
            match val {
                Val::Integer(x) => {
                    data.push(num::cast(x).unwrap());
                    present.set(i);
                }
                Val::Null => {
                    data.push(T::zero());
                }
                _ => panic!("Trying to convert {:?} to {:?}!", val, T::t()),
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.nullable, Vec::with_capacity(batch_size), Vec::with_capacity(batch_size / 8 + 1));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.vals.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.nullable.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("UnfuseNulls<{:?}>({})", T::t(), self.vals)
    }
}

