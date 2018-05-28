use std::fmt;

use mem_store::*;
use engine::*;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EncodingType {
    Str,
    I64,
    USize,
    Val,
    Null,
    BitVec,
    Constant,

    U8,
    U16,
    U32,
}

impl EncodingType {
    pub fn cast_to_basic(&self) -> BasicType {
        match self {
            EncodingType::Str => BasicType::String,
            EncodingType::I64 => BasicType::Integer,
            EncodingType::Val => BasicType::Val,
            EncodingType::Null => BasicType::Null,
            EncodingType::BitVec => BasicType::Boolean,
            _ => panic!("{:?} does not have a corresponding BasicType", &self)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BasicType {
    String,
    Integer,
    Val,
    Null,
    Boolean,
}

impl BasicType {
    pub fn to_encoded(&self) -> EncodingType {
        match *self {
            BasicType::String => EncodingType::Str,
            BasicType::Integer => EncodingType::I64,
            BasicType::Val => EncodingType::Val,
            BasicType::Null => EncodingType::Null,
            BasicType::Boolean => EncodingType::BitVec,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Type<'a> {
    pub decoded: BasicType,
    pub codec: Option<Codec<'a>>,
    pub is_scalar: bool,
    pub is_borrowed: bool,
}

impl<'a> Type<'a> {
    pub fn new(basic: BasicType, codec: Option<Codec>) -> Type {
        Type {
            decoded: basic,
            codec,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn encoded(codec: Codec<'a>) -> Type {
        Type {
            decoded: codec.decoded_type(),
            codec: Some(codec),
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn unencoded(basic: BasicType) -> Type<'a> {
        Type {
            decoded: basic,
            codec: None,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn bit_vec() -> Type<'a> {
        Type::new(BasicType::Boolean, None).mutable()
    }

    pub fn is_encoded(&self) -> bool {
        self.codec.is_some()
    }

    pub fn is_summation_preserving(&self) -> bool {
        self.codec.as_ref().map_or(true, |c| c.is_summation_preserving())
    }

    pub fn is_order_preserving(&self) -> bool {
        self.codec.as_ref().map_or(true, |c| c.is_order_preserving())
    }

    pub fn is_positive_integer(&self) -> bool {
        // TODO(clemens): this is wrong
        self.codec.as_ref().map_or(self.decoded == BasicType::Integer, |c| c.is_positive_integer())
    }

    pub fn scalar(basic: BasicType) -> Type<'a> {
        Type {
            decoded: basic,
            codec: None,
            is_scalar: true,
            is_borrowed: false,
        }
    }

    pub fn encoding_type(&self) -> EncodingType {
        self.codec.as_ref().map_or(self.decoded.to_encoded(), |x| x.encoding_type())
    }

    pub fn decoded(&self) -> Type<'a> {
        let mut result = (*self).clone();
        result.is_borrowed = !self.is_encoded();
        result.codec = None;
        result
    }

    pub fn mutable(mut self) -> Type<'a> {
        self.is_borrowed = true;
        self
    }
}

pub struct OpaqueCodec {
    encoding_type: EncodingType,
    decoded_type: BasicType,
    is_summation_preserving: bool,
    is_order_preserving: bool,
    is_positive_integer: bool,
}

impl OpaqueCodec {
    pub fn new(decoded_type: BasicType, encoding_type: EncodingType) -> OpaqueCodec {
        OpaqueCodec {
            encoding_type,
            decoded_type,
            is_summation_preserving: false,
            is_order_preserving: false,
            is_positive_integer: false,
        }
    }

    pub fn set_positive_integer(mut self, positive_integer: bool) -> OpaqueCodec {
        self.is_positive_integer = positive_integer;
        self
    }

    pub fn set_order_preserving(mut self, order_preserving: bool) -> OpaqueCodec {
        self.is_order_preserving = order_preserving;
        self
    }
}

impl<'a> ColumnCodec<'a> for OpaqueCodec {
    fn unwrap_decode<'b>(&self, _data: &TypedVec<'b>, _buffer: &mut TypedVec<'b>) where 'a: 'b { panic!("OpaqueCodec.unwrap_decode()") }
    fn encoding_type(&self) -> EncodingType { self.encoding_type }
    fn decoded_type(&self) -> BasicType { self.decoded_type }
    fn is_summation_preserving(&self) -> bool { self.is_summation_preserving }
    fn is_order_preserving(&self) -> bool { self.is_order_preserving }
    fn is_positive_integer(&self) -> bool { self.is_positive_integer }
    fn decode_range(&self, _range: (i64, i64)) -> Option<(i64, i64)> { None }
}

impl fmt::Debug for OpaqueCodec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Opaque<{:?},{:?}>[", self.encoding_type, self.decoded_type)?;
        if self.is_summation_preserving {
            write!(f, "SummationPreserving;")?;
        }
        if self.is_order_preserving {
            write!(f, "OrderPreserving;")?;
        }
        if self.is_positive_integer {
            write!(f, "PositiveInteger;")?;
        }
        write!(f, "]")
    }
}
