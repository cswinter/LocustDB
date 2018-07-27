use engine::query_plan::QueryPlan;
use engine::types::*;
use ingest::raw_val::RawVal;


#[derive(Debug, Serialize, Deserialize, Clone, HeapSizeOf)]
pub struct Codec {
    ops: Vec<CodecOp>,
    column_name: String,
    encoding_type: EncodingType,
    decoded_type: BasicType,
    is_summation_preserving: bool,
    is_order_preserving: bool,
    is_positive_integer: bool,
    is_fixed_width: bool,
}

impl Codec {
    pub fn new(ops: Vec<CodecOp>) -> Codec {
        let encoding_type = ops.iter().find(|x| match x {
            CodecOp::PushDataSection(_) => false,
            _ => true
        }).unwrap().input_type();
        let decoded_type = ops[ops.len() - 1].output_type();
        let is_summation_preserving = Codec::has_property(&ops, CodecOp::is_summation_preserving);
        let is_order_preserving = Codec::has_property(&ops, CodecOp::is_order_preserving);
        let is_positive_integer = Codec::has_property(&ops, CodecOp::is_positive_integer);
        let is_fixed_width = Codec::has_property(&ops, CodecOp::is_fixed_width);
        Codec {
            ops,
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type,
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_positive_integer,
            is_fixed_width,
        }
    }

    pub fn identity(t: BasicType) -> Codec {
        Codec {
            ops: vec![],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type: t.to_encoded(),
            decoded_type: t,
            is_summation_preserving: true,
            is_order_preserving: true,
            is_positive_integer: true,
            is_fixed_width: true,
        }
    }

    pub fn integer_offset(t: EncodingType, offset: i64) -> Codec {
        Codec::new(vec![CodecOp::Add(t, offset)])
    }

    pub fn integer_cast(t: EncodingType) -> Codec {
        Codec::new(vec![CodecOp::ToI64(t)])
    }

    pub fn lz4(t: EncodingType) -> Codec {
        Codec::new(vec![CodecOp::LZ4(t)])
    }

    pub fn opaque(encoding_type: EncodingType,
                  decoded_type: BasicType,
                  is_summation_preserving: bool,
                  is_order_preserving: bool,
                  is_positive_integer: bool,
                  is_fixed_width: bool) -> Codec {
        Codec {
            ops: vec![CodecOp::Unknown],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type,
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_positive_integer,
            is_fixed_width,
        }
    }

    pub fn with_lz4(&self) -> Codec {
        let mut ops = vec![CodecOp::LZ4(self.encoding_type)];
        for &op in &self.ops {
            /*match  op {
                CodecOp::DictLookup(t)=>{
                    let dict_data  = ops.pop().unwrap();
                    let dict_indices = ops.pop().unwrap();
                    ops.push(CodecOp::LZ4(EncodingType::U8));
                    ops.push(dict_indices);
                    ops.push(CodecOp::LZ4())
                }
            }*/
            ops.push(op);
        }
        Codec::new(ops)
    }

    pub fn decode(&self, plan: Box<QueryPlan>) -> Box<QueryPlan> {
        self.decode_ops(&self.ops, plan)
    }

    fn decode_ops(&self, ops: &[CodecOp], plan: Box<QueryPlan>) -> Box<QueryPlan> {
        let mut stack = vec![plan];
        for op in ops {
            let plan = match *op {
                CodecOp::Add(t, x) => {
                    Box::new(QueryPlan::AddVS(
                        stack.pop().unwrap(),
                        t,
                        Box::new(QueryPlan::Constant(RawVal::Int(x), true))))
                }
                CodecOp::ToI64(t) => {
                    Box::new(QueryPlan::Widen(
                        stack.pop().unwrap(),
                        t,
                        EncodingType::I64))
                }
                CodecOp::PushDataSection(section_index) => {
                    Box::new(QueryPlan::ReadColumnSection(
                        self.column_name.to_string(),
                        section_index,
                        None))
                }
                CodecOp::DictLookup(t) => {
                    let dict_data = stack.pop().unwrap();
                    let dict_indices = stack.pop().unwrap();
                    let indices = stack.pop().unwrap();
                    Box::new(QueryPlan::DictLookup(
                        indices,
                        t,
                        dict_indices,
                        dict_data))
                }
                CodecOp::LZ4(t) =>
                    Box::new(QueryPlan::LZ4Decode(
                        stack.pop().unwrap(), t)),
                CodecOp::UnpackStrings => unimplemented!(" unpack strings"),
                CodecOp::Unknown => panic!("unkown decode plan!"),
            };
            stack.push(plan);
        }
        assert_eq!(stack.len(), 1);
        stack.pop().unwrap()
    }

    pub fn ensure_fixed_width(&self, plan: Box<QueryPlan>) -> (Codec, Box<QueryPlan>) {
        let (fixed_width, rest) = self.ensure_property(CodecOp::is_fixed_width);
        let mut new_codec = if rest.is_empty() {
            Codec::identity(self.decoded_type())
        } else {
            Codec::new(rest)
        };
        new_codec.set_column_name(&self.column_name);
        (new_codec, self.decode_ops(&fixed_width, plan))
    }

    pub fn encoding_type(&self) -> EncodingType { self.encoding_type }
    pub fn decoded_type(&self) -> BasicType { self.decoded_type }
    pub fn is_summation_preserving(&self) -> bool { self.is_summation_preserving }
    pub fn is_order_preserving(&self) -> bool { self.is_order_preserving }
    pub fn is_positive_integer(&self) -> bool { self.is_positive_integer }
    pub fn is_fixed_width(&self) -> bool { self.is_fixed_width }

    pub fn encode_str(&self, string_const: Box<QueryPlan>) -> Box<QueryPlan> {
        match self.ops[..] {
            [CodecOp::PushDataSection(1), CodecOp::PushDataSection(2), CodecOp::DictLookup(_)] =>
                Box::new(QueryPlan::InverseDictLookup(
                    Box::new(QueryPlan::ReadColumnSection(
                        self.column_name.to_string(), 1, None)),
                    Box::new(QueryPlan::ReadColumnSection(
                        self.column_name.to_string(), 2, None)),
                    string_const)),
            _ => panic!("encode_str not supported for {:?}", &self.ops),
        }
    }

    pub fn encode_int(&self, x: i64) -> RawVal {
        if let CodecOp::Add(_, y) = self.ops[0] {
            assert_eq!(self.ops.len(), 1);
            RawVal::Int(x - y)
        } else if let CodecOp::ToI64(_) = self.ops[0] {
            assert_eq!(self.ops.len(), 1);
            RawVal::Int(x)
        } else {
            panic!("encode_int not supported for {:?}", &self.ops)
        }
    }

    pub(in mem_store) fn set_column_name(&mut self, name: &str) {
        self.column_name = name.to_string();
    }


    fn has_property(ops: &[CodecOp], p: fn(&CodecOp) -> bool) -> bool {
        let mut ops = ops.to_vec();
        while let Some(op) = ops.pop() {
            if !p(&op) { return false; }
            for _ in 1..op.arg_count() {
                Codec::pop(&mut ops);
            }
        }
        true
    }

    fn pop(ops: &mut Vec<CodecOp>) {
        if let Some(op) = ops.pop() {
            for _ in 0..op.arg_count() {
                Codec::pop(ops);
            }
        }
    }

    /// Splits up the CodecOps into two sequences.
    /// The first sequence is the minimum number of `CodecOp`s that have to be executed to restore property `p`.
    /// The second sequence is the remaining operations required for full decoding.
    fn ensure_property(&self, p: fn(&CodecOp) -> bool) -> (Vec<CodecOp>, Vec<CodecOp>) {
        let mut ops = self.ops.clone();
        let mut property_preserving = Vec::new();
        while let Some(op) = ops.pop() {
            if !p(&op) {
                ops.push(op);
                property_preserving.reverse();
                return (ops, property_preserving);
            }
            property_preserving.push(op);
            for _ in 1..op.arg_count() {
                Codec::pop_push(&mut ops, &mut property_preserving);
            }
        }
        (ops, property_preserving)
    }

    fn pop_push(ops: &mut Vec<CodecOp>, push: &mut Vec<CodecOp>) {
        if let Some(op) = ops.pop() {
            push.push(op);
            for _ in 0..op.arg_count() {
                Codec::pop_push(ops, push);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, HeapSizeOf)]
pub enum CodecOp {
    Add(EncodingType, i64),
    ToI64(EncodingType),
    PushDataSection(usize),
    DictLookup(EncodingType),
    LZ4(EncodingType),
    UnpackStrings,
    Unknown,
}

impl CodecOp {
    fn input_type(&self) -> EncodingType {
        match *self {
            CodecOp::Add(t, _) => t,
            CodecOp::ToI64(t) => t,
            CodecOp::DictLookup(t) => t,
            CodecOp::LZ4(_) => EncodingType::U8,
            CodecOp::UnpackStrings => EncodingType::U8,
            CodecOp::PushDataSection(_) => panic!("PushDataSection.input_type()"),
            CodecOp::Unknown => panic!("Unknown.input_type()"),
        }
    }

    fn output_type(&self) -> BasicType {
        match self {
            CodecOp::Add(_, _) => BasicType::Integer,
            CodecOp::ToI64(_) => BasicType::Integer,
            CodecOp::DictLookup(_) => BasicType::String,
            CodecOp::LZ4(_) => BasicType::Integer,
            CodecOp::UnpackStrings => BasicType::String,
            CodecOp::PushDataSection(_) => panic!("PushDataSection.input_type()"),
            CodecOp::Unknown => panic!("Unknown.output_type()"),
        }
    }

    fn is_summation_preserving(&self) -> bool {
        match self {
            CodecOp::Add(_, x) => *x == 0,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => false,
            CodecOp::LZ4(_) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_summation_preserving()"),
        }
    }

    fn is_order_preserving(&self) -> bool {
        match self {
            CodecOp::Add(_, _) => true,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::LZ4(_) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_summation_preserving()"),
        }
    }

    fn is_positive_integer(&self) -> bool {
        match self {
            CodecOp::Add(_, _) => true,
            CodecOp::ToI64(_) => true, // TODO(clemens): no it's not (hack to make grouping key work)
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::LZ4(_) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_positive_integer()"),
        }
    }

    fn is_fixed_width(&self) -> bool {
        match self {
            CodecOp::Add(_, _) => true,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::LZ4(_) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_fixed_width()"),
        }
    }

    fn arg_count(&self) -> usize {
        match self {
            CodecOp::Add(_, _) => 1,
            CodecOp::ToI64(_) => 1,
            CodecOp::PushDataSection(_) => 0,
            CodecOp::DictLookup(_) => 3,
            CodecOp::LZ4(_) => 1,
            CodecOp::UnpackStrings => 1,
            CodecOp::Unknown => panic!("Unknown.is_fixed_width()"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_property() {
        let codec = vec![
            CodecOp::LZ4(EncodingType::U16),
            CodecOp::PushDataSection(1),
            CodecOp::LZ4(EncodingType::U64),
            CodecOp::PushDataSection(2),
            CodecOp::LZ4(EncodingType::U8),
            CodecOp::DictLookup(EncodingType::U16),
        ];
        let (fixed_width, rest) = Codec::new(codec).ensure_property(CodecOp::is_fixed_width);
        assert_eq!(fixed_width, vec![
            CodecOp::LZ4(EncodingType::U16),
        ]);
        assert_eq!(rest, vec![
            CodecOp::PushDataSection(1),
            CodecOp::LZ4(EncodingType::U64),
            CodecOp::PushDataSection(2),
            CodecOp::LZ4(EncodingType::U8),
            CodecOp::DictLookup(EncodingType::U16),
        ]);
    }
}
