use serde::{Deserialize, Serialize};

use crate::xor_float;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mixed {
    Int(i64),
    Float(f64),
    Str(String),
    Null,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Column {
    Float(Vec<f64>),
    Int(Vec<i64>),
    String(Vec<String>),
    Mixed(Vec<Mixed>),
    Null(usize),

    Xor(Vec<u8>),
}


impl Column {
    pub fn compress(xs: Vec<f64>, mantissa: Option<u32>) -> Column {
        let xor_compressed = xor_float::double::encode(&xs, 100, mantissa);
        // Require at least 1.5x compression to use xor
        if xor_compressed.len() * 6 > xor_compressed.len() {
            Column::Xor(Vec::from(xor_compressed))
        } else {
            Column::Float(xs)
        }
    }

    pub fn decompress(self) -> Column {
        match self {
            Column::Xor(xs) => Column::Float(xor_float::double::decode(&xs).unwrap()),
            _ => self,
        }
    }
}
