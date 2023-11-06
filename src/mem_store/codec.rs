use serde::{Deserialize, Serialize};

use crate::engine::planning::QueryPlanner;
use crate::engine::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Codec {
    ops: Vec<CodecOp>,
    column_name: String,
    section_types: Vec<EncodingType>,
    decoded_type: BasicType,
    is_summation_preserving: bool,
    is_order_preserving: bool,
    is_fixed_width: bool,
}

impl Codec {
    pub fn new(ops: Vec<CodecOp>, section_types: Vec<EncodingType>) -> Codec {
        let decoded_type = CodecOp::output_type(&ops, &section_types);
        let is_summation_preserving = Codec::has_property(&ops, CodecOp::is_summation_preserving);
        let is_order_preserving = Codec::has_property(&ops, CodecOp::is_order_preserving);
        let is_fixed_width = Codec::has_property(&ops, CodecOp::is_elementwise_decodable);
        Codec {
            ops,
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            section_types,
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_fixed_width,
        }
    }

    pub fn identity(t: BasicType) -> Codec {
        Codec {
            ops: vec![],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            section_types: vec![t.to_encoded()],
            decoded_type: t,
            is_summation_preserving: true,
            is_order_preserving: true,
            is_fixed_width: true,
        }
    }

    pub fn integer_offset(t: EncodingType, offset: i64) -> Codec {
        Codec::new(vec![CodecOp::Add(t, offset)], vec![t])
    }

    pub fn integer_cast(t: EncodingType) -> Codec {
        Codec::new(vec![CodecOp::ToI64(t)], vec![t])
    }

    pub fn lz4(t: EncodingType, decoded_length: usize) -> Codec {
        Codec::new(vec![CodecOp::LZ4(t, decoded_length)], vec![t])
    }

    pub fn opaque(
        encoding_type: EncodingType,
        decoded_type: BasicType,
        is_summation_preserving: bool,
        is_order_preserving: bool,
        is_fixed_width: bool,
    ) -> Codec {
        Codec {
            ops: vec![CodecOp::Unknown],
            column_name: "COLUMN_UNSPECIFIED".to_string(),
            section_types: vec![encoding_type],
            decoded_type,
            is_summation_preserving,
            is_order_preserving,
            is_fixed_width,
        }
    }

    pub fn with_lz4(&self, decoded_length: usize) -> Codec {
        let mut ops = vec![CodecOp::LZ4(self.section_types[0], decoded_length)];
        for &op in &self.ops {
            ops.push(op);
        }
        let mut section_types = self.section_types.clone();
        section_types[0] = EncodingType::U8;
        let mut codec = Codec::new(ops, section_types);
        codec.set_column_name(&self.column_name);
        codec
    }

    pub fn without_lz4(&self) -> Codec {
        let mut ops = Vec::with_capacity(self.ops.len() - 1);
        let mut decoded_type = None;
        for &op in &self.ops {
            if let CodecOp::LZ4(t, _) = op {
                decoded_type = Some(t);
                continue;
            }
            ops.push(op);
        }
        let mut codec = if ops.is_empty() {
            Codec::identity(self.decoded_type)
        } else {
            let mut section_types = self.section_types.clone();
            if let Some(decoded) = decoded_type {
                section_types[0] = decoded;
            }
            Codec::new(ops, section_types)
        };
        codec.set_column_name(&self.column_name);
        codec
    }

    pub fn decode(&self, plan: TypedBufferRef, planner: &mut QueryPlanner) -> TypedBufferRef {
        self.decode_ops(&self.ops, plan, planner)
    }

    fn decode_ops(
        &self,
        ops: &[CodecOp],
        plan: TypedBufferRef,
        planner: &mut QueryPlanner,
    ) -> TypedBufferRef {
        let mut stack = vec![plan];
        for op in ops {
            let plan = match *op {
                CodecOp::Nullable => {
                    let present = stack.pop().unwrap().u8().unwrap();
                    let data = stack.pop().unwrap();
                    planner.assemble_nullable(data, present)
                }
                CodecOp::Add(_, x) => {
                    let lhs = stack.pop().unwrap();
                    let rhs = planner.scalar_i64(x, true).into();
                    planner.add(lhs, rhs)
                }
                CodecOp::Delta(_) => planner.delta_decode(stack.pop().unwrap()).into(),
                CodecOp::ToI64(_) => planner.cast(stack.pop().unwrap(), EncodingType::I64),
                CodecOp::PushDataSection(section_index) => planner.column_section(
                    &self.column_name,
                    section_index,
                    None,
                    self.section_types[section_index],
                ),
                CodecOp::DictLookup(_t) => {
                    let dict_data = stack.pop().unwrap();
                    let dict_indices = stack.pop().unwrap();
                    let indices = stack.pop().unwrap();
                    planner.dict_lookup(
                        indices,
                        dict_indices.u64().unwrap(),
                        dict_data.u8().unwrap(),
                    )
                }
                CodecOp::LZ4(t, decoded_length) => {
                    planner.lz4_decode(stack.pop().unwrap().u8().unwrap(), decoded_length, t)
                }
                CodecOp::UnpackStrings => planner
                    .unpack_strings(stack.pop().unwrap().u8().unwrap())
                    .into(),
                CodecOp::UnhexpackStrings(upper, total_bytes) => planner
                    .unhexpack_strings(stack.pop().unwrap().u8().unwrap(), upper, total_bytes)
                    .into(),
                CodecOp::Unknown => panic!("unknown decode plan!"),
            };
            stack.push(plan);
        }
        assert_eq!(stack.len(), 1);
        stack.pop().unwrap()
    }

    pub fn ensure_fixed_width(
        &self,
        plan: TypedBufferRef,
        planner: &mut QueryPlanner,
    ) -> (Codec, TypedBufferRef) {
        let (fixed_width, rest) = self.ensure_property(CodecOp::is_elementwise_decodable);
        let mut new_codec = if rest.is_empty() {
            Codec::identity(self.decoded_type())
        } else {
            Codec::new(rest, self.section_types.clone())
        };
        new_codec.set_column_name(&self.column_name);
        (new_codec, self.decode_ops(&fixed_width, plan, planner))
    }

    pub fn ops(&self) -> &[CodecOp] {
        &self.ops
    }
    pub fn encoding_type(&self) -> EncodingType {
        self.section_types[0]
    }
    pub fn section_types(&self) -> &[EncodingType] {
        &self.section_types
    }
    pub fn decoded_type(&self) -> BasicType {
        self.decoded_type
    }
    pub fn is_summation_preserving(&self) -> bool {
        self.is_summation_preserving
    }
    pub fn is_order_preserving(&self) -> bool {
        self.is_order_preserving
    }
    pub fn is_elementwise_decodable(&self) -> bool {
        self.is_fixed_width
    }
    pub fn is_identity(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn encode_str(
        &self,
        string_const: BufferRef<Scalar<&'static str>>,
        planner: &mut QueryPlanner,
    ) -> BufferRef<Scalar<i64>> {
        match self.ops[..] {
            [CodecOp::PushDataSection(1), CodecOp::PushDataSection(2), CodecOp::DictLookup(_)] => {
                let offset_len = planner
                    .column_section(&self.column_name, 1, None, EncodingType::U64)
                    .u64()
                    .unwrap();
                let backing_store = planner
                    .column_section(&self.column_name, 2, None, EncodingType::U8)
                    .u8()
                    .unwrap();
                planner.inverse_dict_lookup(offset_len, backing_store, string_const)
            }
            _ => panic!("encode_str not supported for {:?}", &self.ops),
        }
    }

    pub fn encode_int(&self, x: i64) -> i64 {
        if let CodecOp::Add(_, y) = self.ops[0] {
            assert_eq!(self.ops.len(), 1);
            x - y
        } else if let CodecOp::ToI64(_) = self.ops[0] {
            assert_eq!(self.ops.len(), 1);
            x
        } else {
            panic!("encode_int not supported for {:?}", &self.ops)
        }
    }

    pub(in crate::mem_store) fn set_column_name(&mut self, name: &str) {
        self.column_name = name.to_string();
    }

    fn has_property(ops: &[CodecOp], p: fn(&CodecOp) -> bool) -> bool {
        let mut ops = ops.to_vec();
        while let Some(op) = ops.pop() {
            if !p(&op) {
                return false;
            }
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

    pub fn signature(&self, alternate: bool) -> String {
        let mut s = String::new();
        for op in &self.ops {
            s += &op.signature(alternate);
            s += " ";
        }
        s
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CodecOp {
    Nullable,
    Add(EncodingType, i64),
    Delta(EncodingType),
    ToI64(EncodingType),
    PushDataSection(usize),
    DictLookup(EncodingType),
    LZ4(EncodingType, usize),
    UnpackStrings,
    UnhexpackStrings(bool, usize),
    Unknown,
}

impl CodecOp {
    fn output_type(codec: &[CodecOp], section_types: &[EncodingType]) -> BasicType {
        let mut type_stack = vec![section_types[0]];
        for op in codec {
            let t = match op {
                CodecOp::Nullable => {
                    type_stack.pop();
                    type_stack.pop().unwrap().nullable()
                }
                CodecOp::Add(_, _) | CodecOp::Delta(_) | CodecOp::ToI64(_) => {
                    if type_stack.pop().unwrap().is_nullable() {
                        EncodingType::NullableI64
                    } else {
                        EncodingType::I64
                    }
                }
                CodecOp::DictLookup(_) => {
                    type_stack.pop();
                    type_stack.pop();
                    if type_stack.pop().unwrap().is_nullable() {
                        EncodingType::NullableStr
                    } else {
                        EncodingType::Str
                    }
                }
                CodecOp::LZ4(t, _) => *t,
                CodecOp::UnpackStrings => EncodingType::Str,
                CodecOp::UnhexpackStrings(_, _) => EncodingType::Str,
                CodecOp::PushDataSection(i) => section_types[*i],
                CodecOp::Unknown => panic!("Unknown.output_type()"),
            };
            type_stack.push(t);
        }
        type_stack.pop().unwrap().cast_to_basic()
    }

    fn is_summation_preserving(&self) -> bool {
        match self {
            CodecOp::Nullable => false,
            CodecOp::Add(_, x) => *x == 0,
            CodecOp::Delta(_) => false,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => false,
            CodecOp::LZ4(_, _) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::UnhexpackStrings(_, _) => false,
            CodecOp::Unknown => panic!("Unknown.is_summation_preserving()"),
        }
    }

    fn is_order_preserving(&self) -> bool {
        match self {
            CodecOp::Nullable => false,
            CodecOp::Add(_, _) => true,
            CodecOp::Delta(_) => false,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::LZ4(_, _) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::UnhexpackStrings(_, _) => false,
            CodecOp::Unknown => panic!("Unknown.is_order_preserving()"),
        }
    }

    fn is_elementwise_decodable(&self) -> bool {
        match self {
            CodecOp::Nullable => false,
            CodecOp::Add(_, _) => true,
            CodecOp::Delta(_) => false,
            CodecOp::ToI64(_) => true,
            CodecOp::PushDataSection(_) => true,
            CodecOp::DictLookup(_) => true,
            CodecOp::LZ4(_, _) => false,
            CodecOp::UnpackStrings => false,
            CodecOp::UnhexpackStrings(_, _) => false,
            CodecOp::Unknown => panic!("Unknown.is_fixed_width()"),
        }
    }

    fn arg_count(&self) -> usize {
        match self {
            CodecOp::Nullable => 0,
            CodecOp::Add(_, _) => 1,
            CodecOp::Delta(_) => 1,
            CodecOp::ToI64(_) => 1,
            CodecOp::PushDataSection(_) => 0,
            CodecOp::DictLookup(_) => 3,
            CodecOp::LZ4(_, _) => 1,
            CodecOp::UnpackStrings => 1,
            CodecOp::UnhexpackStrings(_, _) => 1,
            CodecOp::Unknown => panic!("Unknown.is_fixed_width()"),
        }
    }

    fn signature(&self, alternate: bool) -> String {
        match self {
            CodecOp::Nullable => "Nullable".to_string(),
            CodecOp::Add(t, amount) => {
                if alternate {
                    format!("Add({:?}, {})", t, amount)
                } else {
                    format!("Add({:?})", t)
                }
            }
            CodecOp::Delta(t) => format!("Delta({:?})", t),
            CodecOp::ToI64(t) => format!("ToI64({:?})", t),
            CodecOp::PushDataSection(i) => format!("Data({})", i),
            CodecOp::DictLookup(t) => format!("Dict({:?})", t),
            CodecOp::LZ4(t, decoded_len) => {
                if alternate {
                    format!("LZ4({:?}, {})", t, decoded_len)
                } else {
                    format!("LZ4({:?})", t)
                }
            }
            CodecOp::UnpackStrings => "StrUnpack".to_string(),
            CodecOp::UnhexpackStrings(_, _) => "StrHexUnpack".to_string(),
            CodecOp::Unknown => "Unknown".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_property() {
        let codec = vec![
            CodecOp::LZ4(EncodingType::U16, 20),
            CodecOp::PushDataSection(1),
            CodecOp::LZ4(EncodingType::U64, 1),
            CodecOp::PushDataSection(2),
            CodecOp::LZ4(EncodingType::U8, 3),
            CodecOp::DictLookup(EncodingType::U16),
        ];
        let (fixed_width, rest) = Codec::new(
            codec,
            vec![EncodingType::U8, EncodingType::U8, EncodingType::U8],
        )
        .ensure_property(CodecOp::is_elementwise_decodable);
        assert_eq!(fixed_width, vec![CodecOp::LZ4(EncodingType::U16, 20),]);
        assert_eq!(
            rest,
            vec![
                CodecOp::PushDataSection(1),
                CodecOp::LZ4(EncodingType::U64, 1),
                CodecOp::PushDataSection(2),
                CodecOp::LZ4(EncodingType::U8, 3),
                CodecOp::DictLookup(EncodingType::U16),
            ]
        );
    }
}
