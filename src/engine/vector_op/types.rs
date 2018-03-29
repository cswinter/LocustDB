use engine::typed_vec::TypedVec;

pub trait VecType<'a, T: 'a> {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [T];
}

impl<'a> VecType<'a, u8> for u8 {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [u8] { vec.cast_ref_u8().0 }
}

impl<'a> VecType<'a, u16> for u16 {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [u16] { vec.cast_ref_u16().0 }
}

impl<'a> VecType<'a, u32> for u32 {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [u32] { vec.cast_ref_u32().0 }
}

impl<'a> VecType<'a, i64> for i64 {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [i64] { vec.cast_ref_i64() }
}

impl<'a> VecType<'a, &'a str> for &'a str {
    fn cast<'b>(vec: &'b TypedVec<'a>) -> &'b [&'a str] { vec.cast_ref_str() }
}


pub trait IntVecType<'a, T: 'a>: VecType<'a, T> + Into<i64> + Copy {}

impl<'a, T: 'a> IntVecType<'a, T> for T where T: VecType<'a, T> + Into<i64> + Copy {}

pub trait ConstType<T> {
    fn cast(vec: &TypedVec) -> T;
}

impl ConstType<i64> for i64 {
    fn cast(vec: &TypedVec) -> i64 { vec.cast_int_const() }
}

impl ConstType<String> for String {
    fn cast(vec: &TypedVec) -> String { vec.cast_str_const() }
}


pub trait IntoUsize {
    fn to_usize(&self) -> usize;
}

impl IntoUsize for u8 {
    fn to_usize(&self) -> usize { *self as usize }
}

impl IntoUsize for u16 {
    fn to_usize(&self) -> usize { *self as usize }
}

impl IntoUsize for u32 {
    fn to_usize(&self) -> usize { *self as usize }
}
