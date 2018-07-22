use engine::query_plan::QueryPlan;
use engine::typed_vec::AnyVec;
use engine::types::*;
use heapsize::HeapSizeOf;
use ingest::raw_val::RawVal;

#[derive(Debug)]
pub struct Column {
    name: String,
    len: usize,
    range: Option<(i64, i64)>,
    codec: Codec,
    data: Vec<DataSection>,
}

impl Column {
    pub fn new(name: &str,
               len: usize,
               range: Option<(i64, i64)>,
               mut codec: Codec,
               data: Vec<DataSection>) -> Column {
        codec.set_column_name(name);
        Column {
            name: name.to_string(),
            len,
            range,
            codec,
            data,
        }
    }

    pub fn null(name: &str, len: usize) -> Column {
        Column {
            name: name.to_string(),
            len,
            range: None,
            codec: Codec::identity(EncodingType::Null),
            data: vec![DataSection::Null(len)],
        }
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn len(&self) -> usize { self.len }
    pub fn codec(&self) -> Codec { self.codec.clone() }
    pub fn basic_type(&self) -> BasicType { self.codec.decoded_type() }
    pub fn encoding_type(&self) -> EncodingType { self.codec.encoding_type() }
    pub fn range(&self) -> Option<(i64, i64)> { self.range }
    pub fn full_type(&self) -> Type {
        Type::new(self.basic_type(), Some(self.codec()))
    }
    pub fn data_sections(&self) -> Vec<&AnyVec> {
        self.data.iter().map(|d| d.to_any_vec()).collect()
    }
}

impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

#[derive(Debug, Clone)]
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
                        Box::new(QueryPlan::Constant(RawVal::Int(x), false))))
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
                    let dictionary = stack.pop().unwrap();
                    let indices = stack.pop().unwrap();
                    Box::new(QueryPlan::DictLookup(
                        indices,
                        t,
                        dictionary))
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
            [CodecOp::PushDataSection(1), CodecOp::DictLookup(EncodingType::U8)] =>
                Box::new(QueryPlan::InverseDictLookup(
                    Box::new(QueryPlan::ReadColumnSection(
                        self.column_name.to_string(), 1, None)),
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

    fn set_column_name(&mut self, name: &str) {
        self.column_name = name.to_string();
    }
}

#[derive(Debug, Clone)]
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


#[derive(Debug)]
pub enum DataSection {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    // TODO(clemens): remove
    String(Vec<String>),
    Null(usize),
}

impl DataSection {
    pub fn to_any_vec(&self) -> &AnyVec {
        match self {
            DataSection::U8(ref x) => x,
            DataSection::U16(ref x) => x,
            DataSection::U32(ref x) => x,
            DataSection::I64(ref x) => x,
            DataSection::String(ref x) => x,
            DataSection::Null(ref x) => x,
        }
    }
}

impl HeapSizeOf for DataSection {
    fn heap_size_of_children(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.heap_size_of_children(),
            DataSection::U16(ref x) => x.heap_size_of_children(),
            DataSection::U32(ref x) => x.heap_size_of_children(),
            DataSection::I64(ref x) => x.heap_size_of_children(),
            DataSection::Null(_) => 0,
            DataSection::String(ref x) => x.heap_size_of_children(),
        }
    }
}

