use mem_store::column::ColumnCodec;


#[derive(Debug, Copy, Clone)]
pub enum EncodingType {
    Str,
    I64,
    Val,
    Null,
    BitVec,
    Constant,

    U8,
    U16,
    U32,
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
    fn to_encoded(&self) -> EncodingType {
        match *self {
            BasicType::String => EncodingType::Str,
            BasicType::Integer => EncodingType::I64,
            BasicType::Val => EncodingType::Val,
            BasicType::Null => EncodingType::Null,
            BasicType::Boolean => EncodingType::BitVec,
        }
    }
}

#[derive(Debug)]
pub struct Type<'a> {
    pub decoded: BasicType,
    pub codec: Option<&'a ColumnCodec>,
    pub is_scalar: bool,
    pub is_borrowed: bool,
}

impl<'a> Type<'a> {
    pub fn new(basic: BasicType, codec: Option<&'a ColumnCodec>) -> Type {
        Type {
            decoded: basic,
            codec,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn bit_vec() -> Type<'static> {
        Type::new(BasicType::Boolean, None).mutable()
    }

    pub fn is_encoded(&self) -> bool {
        self.codec.is_some()
    }

    pub fn is_summation_preserving(&self) -> bool {
        self.codec.map_or(true, |c| c.is_summation_preserving())
    }

    pub fn scalar(basic: BasicType) -> Type<'static> {
        Type {
            decoded: basic,
            codec: None,
            is_scalar: true,
            is_borrowed: false,
        }
    }

    pub fn encoding_type(&self) -> EncodingType {
        self.codec.map_or(self.decoded.to_encoded(), |x| x.encoding_type())
    }

    pub fn decoded(mut self) -> Type<'a> {
        self.is_borrowed = !self.is_encoded();
        self.codec = None;
        self
    }

    pub fn mutable(mut self) -> Type<'a> {
        self.is_borrowed = true;
        self
    }
}

