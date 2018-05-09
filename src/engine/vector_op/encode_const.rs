use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::column::ColumnCodec;

#[derive(Debug)]
pub struct EncodeStrConstant<'a> {
    constant: BufferRef,
    output: BufferRef,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeStrConstant<'a> {
    pub fn new(constant: BufferRef, output: BufferRef, codec: &'a ColumnCodec) -> EncodeStrConstant<'a> {
        EncodeStrConstant {
            constant,
            output,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeStrConstant<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let constant = scratchpad.get_const::<String>(self.constant);
            self.codec.encode_str(&constant)
        };
        scratchpad.set(self.output, TypedVec::constant(result));
    }
}


#[derive(Debug)]
pub struct EncodeIntConstant<'a> {
    constant: BufferRef,
    output: BufferRef,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeIntConstant<'a> {
    pub fn new(constant: BufferRef, output: BufferRef, codec: &'a ColumnCodec) -> EncodeIntConstant<'a> {
        EncodeIntConstant {
            constant,
            output,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeIntConstant<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let constant = scratchpad.get_const::<i64>(self.constant);
        let result = self.codec.encode_int(constant);
        scratchpad.set(self.output, TypedVec::constant(result));
    }
}

