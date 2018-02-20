use bit_vec::BitVec;
use mem_store::point_codec::PointCodec;
use engine::types::Type;
use value::Val;
use mem_store::ingest::RawVal;


pub enum TypedVec<'a> {
    String(Vec<&'a str>),
    Integer(Vec<i64>),
    Mixed(Vec<Val<'a>>),
    Boolean(BitVec),

    BorrowedEncodedU8(&'a [u8], &'a PointCodec<u8>),
    BorrowedEncodedU16(&'a [u16], &'a PointCodec<u16>),
    BorrowedEncodedU32(&'a [u32], &'a PointCodec<u32>),
    EncodedU8(Vec<u8>, &'a PointCodec<u8>),
    EncodedU16(Vec<u16>, &'a PointCodec<u16>),
    EncodedU32(Vec<u32>, &'a PointCodec<u32>),

    Constant(RawVal),
    Empty,
}

impl<'a> TypedVec<'a> {
    pub fn len(&self) -> usize {
        match self {
            &TypedVec::String(ref v) => v.len(),
            &TypedVec::Integer(ref v) => v.len(),
            &TypedVec::Mixed(ref v) => v.len(),
            &TypedVec::Boolean(ref v) => v.len(),
            &TypedVec::Empty => 0,
            &TypedVec::Constant(_) => panic!(" cannot get length of constant"),
            &TypedVec::BorrowedEncodedU8(v, _) => v.len(),
            &TypedVec::EncodedU8(ref v, _) => v.len(),
            &TypedVec::BorrowedEncodedU16(v, _) => v.len(),
            &TypedVec::EncodedU16(ref v, _) => v.len(),
            _ => panic!(" not implemented"),
        }
    }

    pub fn get_raw(&self, i: usize) -> RawVal {
        match self {
            &TypedVec::String(ref v) => RawVal::Str(v[i].to_string()),
            &TypedVec::Integer(ref v) => RawVal::Int(v[i]),
            &TypedVec::Mixed(ref v) => RawVal::from(&v[i]),
            &TypedVec::Boolean(_) => panic!("Boolean(BitVec).get_raw()"),
            &TypedVec::EncodedU8(ref v, codec) => codec.to_raw(v[i]),
            &TypedVec::EncodedU16(ref v, codec) => codec.to_raw(v[i]),
            &TypedVec::EncodedU32(ref v, codec) => codec.to_raw(v[i]),
            &TypedVec::BorrowedEncodedU8(v, codec) => codec.to_raw(v[i]),
            &TypedVec::BorrowedEncodedU16(v, codec) => codec.to_raw(v[i]),
            &TypedVec::BorrowedEncodedU32(v, codec) => codec.to_raw(v[i]),
            &TypedVec::Empty => RawVal::Null,
            &TypedVec::Constant(ref r) => r.clone(),
        }
    }

    pub fn get_type(&self) -> Type {
        match self {
            &TypedVec::String(_) => Type::String,
            &TypedVec::Integer(_) => Type::I64,
            &TypedVec::Mixed(_) => Type::Val,
            &TypedVec::Boolean(_) => Type::Boolean,
            &TypedVec::Empty => Type::Null,
            &TypedVec::EncodedU8(_, _) => Type::U8,
            &TypedVec::EncodedU16(_, _) => Type::U16,
            &TypedVec::EncodedU32(_, _) => Type::U32,
            &TypedVec::BorrowedEncodedU8(_, _) => Type::RefU8,
            &TypedVec::BorrowedEncodedU16(_, _) => Type::RefU16,
            &TypedVec::BorrowedEncodedU32(_, _) => Type::RefU32,
            _ => panic!("not implemented")
        }
    }

    pub fn sort_indices_desc(&self, indices: &mut Vec<usize>) {
        match self {
            &TypedVec::String(ref data) => indices.sort_unstable_by(|i, j| data[*i].cmp(data[*j]).reverse()),
            &TypedVec::Integer(ref data) => indices.sort_unstable_by_key(|i| -data[*i]),
            &TypedVec::Mixed(ref data) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::Boolean(_) => panic!("cannot sort by boolean column"),
            &TypedVec::EncodedU8(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::EncodedU16(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::EncodedU32(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::BorrowedEncodedU8(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::BorrowedEncodedU16(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::BorrowedEncodedU32(ref data, _) => indices.sort_unstable_by(|i, j| data[*i].cmp(&data[*j]).reverse()),
            &TypedVec::Empty | &TypedVec::Constant(_) => {}
        }
    }

    pub fn sort_indices_asc(&self, indices: &mut Vec<usize>) {
        match self {
            &TypedVec::String(ref data) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::Integer(ref data) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::Mixed(ref data) => indices.sort_unstable_by_key(|i| &data[*i]),
            &TypedVec::Boolean(_) => panic!("cannot sort by boolean column"),
            &TypedVec::EncodedU8(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::EncodedU16(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::EncodedU32(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::BorrowedEncodedU8(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::BorrowedEncodedU16(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::BorrowedEncodedU32(ref data, _) => indices.sort_unstable_by_key(|i| data[*i]),
            &TypedVec::Empty | &TypedVec::Constant(_) => {}
        }
    }

    pub fn order_preserving(self) -> Self {
        match self {
            TypedVec::BorrowedEncodedU8(data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            TypedVec::BorrowedEncodedU16(data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            TypedVec::BorrowedEncodedU32(data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            TypedVec::EncodedU8(ref data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            TypedVec::EncodedU16(ref data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            TypedVec::EncodedU32(ref data, codec)
            if !codec.is_order_preserving() =>
                codec.decode(data),
            x => x,
        }
    }

    pub fn cast_ref_i64(&self) -> &[i64] {
        match self {
            &TypedVec::Integer(ref x) => x,
            _ => panic!("type error"),
        }
    }


    pub fn cast_ref_u32<'b>(&'b self) -> (&'b [u32], &'a PointCodec<u32>) {
        match self {
            &TypedVec::BorrowedEncodedU32(data, codec) => (data, codec),
            &TypedVec::EncodedU32(ref data, codec) => (data, codec),
            _ => panic!("type error"),
        }
    }

    pub fn cast_ref_u16<'b>(&'b self) -> (&'b [u16], &'a PointCodec<u16>) {
        match self {
            &TypedVec::BorrowedEncodedU16(data, codec) => (data, codec),
            &TypedVec::EncodedU16(ref data, codec) => (data, codec),
            _ => panic!("type error"),
        }
    }

    pub fn cast_ref_u8<'b>(&'b self) -> (&'b [u8], &'a PointCodec<u8>) {
        match self {
            &TypedVec::BorrowedEncodedU8(data, codec) => (data, codec),
            &TypedVec::EncodedU8(ref data, codec) => (data, codec),
            _ => panic!("type error"),
        }
    }
}

impl<'a> From<(&'a [u8], &'a PointCodec<u8>)> for TypedVec<'a> {
    fn from(encoded: (&'a [u8], &'a PointCodec<u8>)) -> Self {
        TypedVec::BorrowedEncodedU8(encoded.0, encoded.1)
    }
}

impl<'a> From<(&'a [u16], &'a PointCodec<u16>)> for TypedVec<'a> {
    fn from(encoded: (&'a [u16], &'a PointCodec<u16>)) -> Self {
        TypedVec::BorrowedEncodedU16(encoded.0, encoded.1)
    }
}

impl<'a> From<(&'a [u32], &'a PointCodec<u32>)> for TypedVec<'a> {
    fn from(encoded: (&'a [u32], &'a PointCodec<u32>)) -> Self {
        TypedVec::BorrowedEncodedU32(encoded.0, encoded.1)
    }
}


impl<'a> From<(Vec<u8>, &'a PointCodec<u8>)> for TypedVec<'a> {
    fn from(encoded: (Vec<u8>, &'a PointCodec<u8>)) -> Self {
        TypedVec::EncodedU8(encoded.0, encoded.1)
    }
}

impl<'a> From<(Vec<u16>, &'a PointCodec<u16>)> for TypedVec<'a> {
    fn from(encoded: (Vec<u16>, &'a PointCodec<u16>)) -> Self {
        TypedVec::EncodedU16(encoded.0, encoded.1)
    }
}

impl<'a> From<(Vec<u32>, &'a PointCodec<u32>)> for TypedVec<'a> {
    fn from(encoded: (Vec<u32>, &'a PointCodec<u32>)) -> Self {
        TypedVec::EncodedU32(encoded.0, encoded.1)
    }
}
