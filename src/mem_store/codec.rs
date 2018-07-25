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
}

impl Codec {
    pub fn new(ops: Vec<CodecOp>) -> Codec {
        let encoding_type = ops.iter().find(|x| match x {
            CodecOp::PushDataSection(_) => false,
            _ => true
        }).unwrap().input_type();
        let decoded_type = ops[ops.len() - 1].output_type();
        let is_summation_preserving = ops.iter().all(|x| x.is_summation_preserving());
        let is_order_preserving = ops.iter().all(|x| x.is_order_preserving());
        let is_positive_integer = ops.iter().all(|x| x.is_positive_integer());
        Codec {
            ops,
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type,
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_positive_integer,
        }
    }

    pub fn identity(t: EncodingType) -> Codec {
        Codec {
            ops: vec![],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type: t,
            decoded_type: t.cast_to_basic(),
            is_summation_preserving: true,
            is_order_preserving: true,
            is_positive_integer: true,
        }
    }

    pub fn opaque(encoding_type: EncodingType,
                  decoded_type: BasicType,
                  is_summation_preserving: bool,
                  is_order_preserving: bool,
                  is_positive_integer: bool) -> Codec {
        Codec {
            ops: vec![CodecOp::Unknown],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            encoding_type,
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_positive_integer,
        }
    }

    pub fn decode(&self, plan: Box<QueryPlan>) -> Box<QueryPlan> {
        let mut stack = vec![plan];
        for op in &self.ops {
            let plan = match *op {
                CodecOp::Add(t, x) => {
                    Box::new(QueryPlan::AddVS(
                        stack.pop().unwrap(),
                        t,
                        Box::new(QueryPlan::Constant(RawVal::Int(x), true))))
                }
                CodecOp::ToI64(t) => {
                    Box::new(QueryPlan::TypeConversion(
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
                CodecOp::UnpackStrings => unimplemented!(" unpack strings"),
                CodecOp::Unknown => panic!("unkown decode plan!"),
            };
            stack.push(plan);
        }
        assert_eq!(stack.len(), 1);
        stack.pop().unwrap()
    }
    pub fn encoding_type(&self) -> EncodingType { self.encoding_type }
    pub fn decoded_type(&self) -> BasicType { self.decoded_type }
    pub fn is_summation_preserving(&self) -> bool { self.is_summation_preserving }
    pub fn is_order_preserving(&self) -> bool { self.is_order_preserving }
    pub fn is_positive_integer(&self) -> bool { self.is_positive_integer }

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
}

#[derive(Debug, Serialize, Deserialize, Clone, HeapSizeOf)]
pub enum CodecOp {
    Add(EncodingType, i64),
    ToI64(EncodingType),
    PushDataSection(usize),
    DictLookup(EncodingType),
    UnpackStrings,
    Unknown,
}

impl CodecOp {
    fn input_type(&self) -> EncodingType {
        match *self {
            CodecOp::Add(t, _) => t,
            CodecOp::ToI64(t) => t,
            CodecOp::DictLookup(t) => t,
            CodecOp::UnpackStrings => EncodingType::U8,
            _ => panic!("{:?}.input_type()", self),
        }
    }

    fn output_type(&self) -> BasicType {
        match self {
            CodecOp::Add(_, _) => BasicType::Integer,
            CodecOp::ToI64(_) => BasicType::Integer,
            CodecOp::DictLookup(_) => BasicType::String,
            CodecOp::UnpackStrings => BasicType::String,
            _ => panic!("{:?}.input_type()", self),
        }
    }

    fn is_summation_preserving(&self) -> bool {
        match self {
            CodecOp::Add(_, x) => *x == 0,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => false,
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
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_summation_preserving()"),
        }
    }

    fn is_positive_integer(&self) -> bool {
        match self {
            CodecOp::Add(_, _) => true,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::UnpackStrings => false,
            CodecOp::Unknown => panic!("Unknown.is_positive_integer()"),
        }
    }
}


