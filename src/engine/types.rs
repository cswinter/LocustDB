use mem_store::*;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, HeapSizeOf)]
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
    U64,

    Premerge,
    MergeOp,
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

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, HeapSizeOf)]
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
pub struct Type {
    pub decoded: BasicType,
    // TODO(clemens): make this required, can use identity codec
    pub codec: Option<Codec>,
    pub is_scalar: bool,
    pub is_borrowed: bool,
}

impl Type {
    pub fn new(basic: BasicType, codec: Option<Codec>) -> Type {
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
            codec: Some(codec),
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn unencoded(basic: BasicType) -> Type {
        Type {
            decoded: basic,
            codec: None,
            is_scalar: false,
            is_borrowed: false,
        }
    }

    pub fn bit_vec() -> Type {
        Type::new(BasicType::Boolean, None).mutable()
    }

    pub fn is_encoded(&self) -> bool {
        self.codec.as_ref().map_or(false, |c| !c.is_identity())
    }

    pub fn is_summation_preserving(&self) -> bool {
        self.codec.as_ref().map_or(true, |c| c.is_summation_preserving())
    }

    pub fn is_elementwise_decodable(&self) -> bool {
        self.codec.as_ref().map_or(true, |c| c.is_elementwise_decodable())
    }

    pub fn is_order_preserving(&self) -> bool {
        self.codec.as_ref().map_or(true, |c| c.is_order_preserving())
    }

    pub fn is_positive_integer(&self) -> bool {
        // TODO(clemens): this is wrong
        self.codec.as_ref().map_or(self.decoded == BasicType::Integer, |c| c.is_positive_integer())
    }

    pub fn scalar(basic: BasicType) -> Type {
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

    pub fn decoded(&self) -> Type {
        let mut result = (*self).clone();
        result.is_borrowed = !self.is_encoded();
        result.codec = None;
        result
    }

    pub fn mutable(mut self) -> Type {
        self.is_borrowed = true;
        self
    }
}

