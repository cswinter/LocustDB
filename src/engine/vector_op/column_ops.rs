use engine::vector_op::vector_operator::*;
use mem_store::column::{ColumnData, ColumnCodec};


#[derive(Debug)]
pub struct GetDecode<'a> {
    pub output: BufferRef,
    pub col: &'a ColumnData
}

impl<'a> VecOperator<'a> for GetDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, self.col.collect_decoded());
    }
}

#[derive(Debug)]
pub struct FilterDecode<'a> {
    pub col: &'a ColumnData,
    pub filter: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for FilterDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let filter = scratchpad.get_mut_bit_vec(self.filter);
            self.col.filter_decode(&filter)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct IndexDecode<'a> {
    pub col: &'a ColumnData,
    pub filter: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for IndexDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let indices = scratchpad.get::<usize>(self.filter);
            self.col.index_decode(&indices)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct GetEncoded<'a> {
    pub col: &'a ColumnCodec,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = self.col.get_encoded();
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct FilterEncoded<'a> {
    pub col: &'a ColumnCodec,
    pub filter: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for FilterEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let filter = scratchpad.get_bit_vec(self.filter);
            self.col.filter_encoded(&filter)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct IndexEncoded<'a> {
    pub col: &'a ColumnCodec,
    pub filter: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for IndexEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let indices = scratchpad.get::<usize>(self.filter);
            self.col.index_encoded(&indices)
        };
        scratchpad.set(self.output, result);
    }
}

