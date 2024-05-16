use serde::{Deserialize, Serialize};

use crate::mem_store::*;

// WARNING: Changing this enum will break backwards compatibility with existing data
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum EncodingType {
    // Straightforward vector or slice of basic types
    Str,
    I64,
    U8,
    U16,
    U32,
    U64,
    F64,
    Val,
    USize,
    Bitvec, // this has the same representation as U8, but will have 1/8th the length

    // Nullable versions of basic types which include both a vector/slize and a Vec<u8>/&[u8] bitvec mask
    NullableStr,
    NullableI64,
    NullableU8,
    NullableU16,
    NullableU32,
    NullableU64,
    NullableF64,

    // Vector of optional basic types. Used for grouping or sorting
    OptStr,
    OptF64,

    // Represents null column as single `usize` value that is the length of the column
    Null,

    // Single scalar value
    ScalarI64,
    ScalarF64,
    ScalarStr,
    ScalarString,
    ConstVal,

    ByteSlices(usize),

    // Used as grouping key during aggregation/sorting operation when we cannot bit or byte pack the columns that make up the grouping key
    ValRows,

    Premerge,
    MergeOp,
}

impl EncodingType {
    pub fn cast_to_basic(self) -> BasicType {
        match self {
            EncodingType::Str => BasicType::String,
            EncodingType::I64 => BasicType::Integer,
            EncodingType::F64 => BasicType::Float,
            EncodingType::NullableStr => BasicType::NullableString,
            EncodingType::NullableI64 => BasicType::NullableInteger,
            EncodingType::NullableF64 => BasicType::NullableFloat,
            EncodingType::Val => BasicType::Val,
            EncodingType::Null => BasicType::Null,
            _ => panic!("{:?} does not have a corresponding BasicType", &self),
        }
    }

    /// Returns the nullable version of the encoding type.
    /// This is used when propagating nullability of type casts.
    /// Types that already represent null values will return themselves.
    pub fn nullable(&self) -> EncodingType {
        match self {
            EncodingType::Str | EncodingType::NullableStr => EncodingType::NullableStr,
            EncodingType::I64 | EncodingType::NullableI64 => EncodingType::NullableI64,
            EncodingType::U8 | EncodingType::NullableU8 => EncodingType::NullableU8,
            EncodingType::U16 | EncodingType::NullableU16 => EncodingType::NullableU16,
            EncodingType::U32 | EncodingType::NullableU32 => EncodingType::NullableU32,
            EncodingType::U64 | EncodingType::NullableU64 => EncodingType::NullableU64,
            EncodingType::F64 | EncodingType::NullableF64 => EncodingType::NullableF64,
            EncodingType::Val => EncodingType::Val,
            EncodingType::OptStr => EncodingType::OptStr,
            EncodingType::OptF64 => EncodingType::OptF64,
            _ => panic!("{:?} does not have a corresponding nullable type", &self),
        }
    }

    pub fn nullable_fused(&self) -> EncodingType {
        match self {
            EncodingType::NullableStr => EncodingType::OptStr,
            EncodingType::NullableF64 => EncodingType::OptF64,
            EncodingType::NullableI64 => EncodingType::I64,
            _ => panic!(
                "{:?} does not have a corresponding fused nullable type",
                &self
            ),
        }
    }

    /// Returns whether the encoding type is nullable, i.e., is a basic type with an associated null map.
    /// This does not apply to types like OptStr which can naturally represent null values.
    pub fn is_nullable(&self) -> bool {
        match self {
            EncodingType::NullableStr
            | EncodingType::NullableI64
            | EncodingType::NullableU8
            | EncodingType::NullableU16
            | EncodingType::NullableU32
            | EncodingType::NullableU64
            | EncodingType::NullableF64 => true,
            EncodingType::OptStr
            | EncodingType::OptF64
            | EncodingType::Str
            | EncodingType::I64
            | EncodingType::U8
            | EncodingType::U16
            | EncodingType::U32
            | EncodingType::U64
            | EncodingType::F64
            | EncodingType::USize
            | EncodingType::Bitvec
            | EncodingType::Val
            | EncodingType::Null
            | EncodingType::ScalarF64
            | EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::ConstVal
            | EncodingType::ByteSlices(_)
            | EncodingType::ValRows
            | EncodingType::Premerge
            | EncodingType::MergeOp => false,
        }
    }

    /// Returns whether the encoding type can represent null values without an associated null map.
    pub fn is_naturally_nullable(&self) -> bool {
        match self {
            EncodingType::Val | EncodingType::OptStr | EncodingType::OptF64 => true,
            EncodingType::NullableStr
            | EncodingType::NullableI64
            | EncodingType::NullableU8
            | EncodingType::NullableU16
            | EncodingType::NullableU32
            | EncodingType::NullableU64
            | EncodingType::NullableF64
            | EncodingType::Str
            | EncodingType::I64
            | EncodingType::U8
            | EncodingType::U16
            | EncodingType::U32
            | EncodingType::U64
            | EncodingType::F64
            | EncodingType::USize
            | EncodingType::Bitvec
            | EncodingType::Null
            | EncodingType::ScalarF64
            | EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::ConstVal
            | EncodingType::ByteSlices(_)
            | EncodingType::ValRows
            | EncodingType::Premerge
            | EncodingType::MergeOp => false,
        }
    }

    pub fn non_nullable(&self) -> EncodingType {
        match self {
            EncodingType::NullableStr | EncodingType::OptStr => EncodingType::Str,
            EncodingType::NullableI64 => EncodingType::I64,
            EncodingType::NullableU8 => EncodingType::U8,
            EncodingType::NullableU16 => EncodingType::U16,
            EncodingType::NullableU32 => EncodingType::U32,
            EncodingType::NullableU64 => EncodingType::U64,
            EncodingType::OptF64 | EncodingType::NullableF64 => EncodingType::F64,
            EncodingType::Str
            | EncodingType::I64
            | EncodingType::U8
            | EncodingType::U16
            | EncodingType::U32
            | EncodingType::U64
            | EncodingType::F64
            | EncodingType::USize
            | EncodingType::Bitvec
            | EncodingType::Val
            | EncodingType::Null
            | EncodingType::ScalarF64
            | EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::ConstVal
            | EncodingType::ByteSlices(_)
            | EncodingType::ValRows
            | EncodingType::Premerge
            | EncodingType::MergeOp => *self,
        }
    }

    pub fn is_constant(&self) -> bool {
        match *self {
            EncodingType::NullableStr
            | EncodingType::NullableI64
            | EncodingType::NullableU8
            | EncodingType::NullableU16
            | EncodingType::NullableU32
            | EncodingType::NullableU64
            | EncodingType::NullableF64
            | EncodingType::OptStr
            | EncodingType::OptF64
            | EncodingType::Str
            | EncodingType::I64
            | EncodingType::U8
            | EncodingType::U16
            | EncodingType::U32
            | EncodingType::U64
            | EncodingType::F64
            | EncodingType::USize
            | EncodingType::Val
            | EncodingType::Bitvec
            | EncodingType::ByteSlices(_)
            | EncodingType::ValRows
            | EncodingType::Premerge
            | EncodingType::MergeOp => false,
            EncodingType::ScalarF64
            | EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::Null
            | EncodingType::ConstVal => true,
        }
    }

    pub fn is_scalar(&self) -> bool {
        match *self {
            EncodingType::NullableStr
            | EncodingType::NullableI64
            | EncodingType::NullableU8
            | EncodingType::NullableU16
            | EncodingType::NullableU32
            | EncodingType::NullableU64
            | EncodingType::NullableF64
            | EncodingType::OptStr
            | EncodingType::OptF64
            | EncodingType::Str
            | EncodingType::I64
            | EncodingType::U8
            | EncodingType::U16
            | EncodingType::U32
            | EncodingType::U64
            | EncodingType::F64
            | EncodingType::USize
            | EncodingType::Val
            | EncodingType::Bitvec
            | EncodingType::ByteSlices(_)
            | EncodingType::ValRows
            | EncodingType::Premerge
            | EncodingType::Null
            | EncodingType::MergeOp => false,
            EncodingType::ScalarF64
            | EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::ConstVal => true,
        }
    }

    pub fn least_upper_bound(&self, other: EncodingType) -> EncodingType {
        if *self == other {
            *self
        } else {
            match (self, other) {
                (EncodingType::Val, _) => EncodingType::Val,
                (_, EncodingType::Val) => EncodingType::Val,
                (EncodingType::F64, EncodingType::I64) => EncodingType::Val,
                (EncodingType::I64, EncodingType::F64) => EncodingType::Val,
                (EncodingType::OptStr, EncodingType::Str) => EncodingType::OptStr,
                (EncodingType::Str, EncodingType::OptStr) => EncodingType::OptStr,
                (EncodingType::OptF64, EncodingType::F64) => EncodingType::OptF64,
                (EncodingType::F64, EncodingType::OptF64) => EncodingType::OptF64,
                _ => unimplemented!("lub not implemented for {:?} and {:?}", self, other),
            }
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            EncodingType::Str => 0u8,
            EncodingType::I64 => 1,
            EncodingType::U8 => 2,
            EncodingType::U16 => 3,
            EncodingType::U32 => 4,
            EncodingType::U64 => 5,
            EncodingType::F64 => 6,
            EncodingType::Val => 7,
            EncodingType::USize => 8,
            EncodingType::Bitvec => 9,
            EncodingType::NullableStr => 10,
            EncodingType::NullableI64 => 11,
            EncodingType::NullableU8 => 12,
            EncodingType::NullableU16 => 13,
            EncodingType::NullableU32 => 14,
            EncodingType::NullableU64 => 15,
            EncodingType::NullableF64 => 16,
            EncodingType::OptStr => 17,
            EncodingType::OptF64 => 18,
            EncodingType::Null => 19,
            EncodingType::ScalarF64 => 20,
            EncodingType::ScalarI64 => 21,
            EncodingType::ScalarStr => 22,
            EncodingType::ScalarString => 23,
            EncodingType::ConstVal => 24,
            EncodingType::ValRows => 25,
            EncodingType::Premerge => 26,
            EncodingType::MergeOp => 27,
            EncodingType::ByteSlices(x) => 64 + u8::try_from(x).unwrap(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BasicType {
    String,
    Integer,
    Float,
    NullableString,
    NullableInteger,
    NullableFloat,

    Val,
    Null,
    Boolean,
}

impl BasicType {
    pub fn to_encoded(self) -> EncodingType {
        match self {
            BasicType::String => EncodingType::Str,
            BasicType::Integer => EncodingType::I64,
            BasicType::Float => EncodingType::F64,
            BasicType::NullableString => EncodingType::NullableStr,
            BasicType::NullableInteger => EncodingType::NullableI64,
            BasicType::NullableFloat => EncodingType::NullableF64,
            BasicType::Val => EncodingType::Val,
            BasicType::Null => EncodingType::Null,
            BasicType::Boolean => EncodingType::U8,
        }
    }

    pub fn is_nullable(self) -> bool {
        matches!(self, BasicType::NullableInteger | BasicType::NullableString)
    }

    pub fn non_nullable(self) -> BasicType {
        match self {
            BasicType::NullableInteger => BasicType::Integer,
            BasicType::NullableString => BasicType::String,
            _ => self,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Type {
    pub decoded: BasicType,
    // this could just be of type `Codec`, using the identity codec instead of None
    pub codec: Codec,
    pub is_scalar: bool,
    pub is_borrowed: bool,
}

impl Type {
    pub fn new(basic: BasicType, codec: Codec) -> Type {
        Type {
            decoded: basic,
            codec,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn encoded(codec: Codec) -> Type {
        Type {
            decoded: codec.decoded_type(),
            codec,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn unencoded(basic: BasicType) -> Type {
        Type {
            decoded: basic,
            codec: Codec::identity(basic),
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn bit_vec() -> Type {
        Type::unencoded(BasicType::Boolean).mutable()
    }

    pub fn integer() -> Type {
        Type::unencoded(BasicType::Integer)
    }

    pub fn is_encoded(&self) -> bool {
        !self.codec.is_identity()
    }

    pub fn is_summation_preserving(&self) -> bool {
        self.codec.is_summation_preserving()
    }

    pub fn is_elementwise_decodable(&self) -> bool {
        self.codec.is_elementwise_decodable()
    }

    pub fn is_order_preserving(&self) -> bool {
        self.codec.is_order_preserving()
    }

    pub fn is_nullable(&self) -> bool {
        self.decoded.is_nullable()
    }

    pub fn scalar(basic: BasicType) -> Type {
        Type {
            decoded: basic,
            codec: Codec::identity(basic),
            is_scalar: true,
            is_borrowed: false,
        }
    }

    pub fn encoding_type(&self) -> EncodingType {
        self.codec.encoding_type()
    }

    pub fn decoded(&self) -> Type {
        let mut result = (*self).clone();
        result.is_borrowed = !self.is_encoded();
        result.codec = Codec::identity(self.decoded);
        result
    }

    pub fn mutable(mut self) -> Type {
        self.is_borrowed = true;
        self
    }
}
