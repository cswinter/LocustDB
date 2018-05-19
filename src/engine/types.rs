use mem_store::*;


#[derive(Debug, Copy, Clone)]
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

    pub fn encoded(basic: BasicType, codec: Codec<'a>) -> Type {
        Type {
            decoded: basic,
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

