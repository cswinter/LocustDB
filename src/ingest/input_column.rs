pub enum InputColumn {
    Int(Vec<i64>),
    Float(Vec<f64>),
    NullableFloat(u64, Vec<(u64, f64)>),
    NullableInt(u64, Vec<(u64, i64)>),
    Str(Vec<String>),
    Null(usize),
}
