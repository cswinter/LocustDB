#[derive(Debug, Copy, Clone)]
pub enum Type {
    String,
    I64,
    Val,
    Null,
    Boolean,
    Scalar,

    U8,
    U16,
    U32,

    RefU8,
    RefU16,
    RefU32,
}