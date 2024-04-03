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
            Column::Xor(xor_compressed)
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

    pub fn size_bytes(&self) -> usize {
        let heapsize = match self {
            Column::Float(xs) => xs.len() * std::mem::size_of::<f64>(),
            Column::Int(xs) => xs.len() * std::mem::size_of::<i64>(),
            Column::String(xs) => xs.iter().map(|s| s.len()).sum(),
            Column::Mixed(xs) => xs
                .iter()
                .map(|m| match m {
                    Mixed::Str(s) => s.len() + std::mem::size_of::<Mixed>(),
                    _ => std::mem::size_of::<Mixed>(),
                })
                .sum(),
            Column::Null(_) => 0,
            Column::Xor(xs) => xs.len(),
        };
        heapsize + std::mem::size_of::<Column>()
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Float(xs) => xs.len(),
            Column::Int(xs) => xs.len(),
            Column::String(xs) => xs.len(),
            Column::Mixed(xs) => xs.len(),
            Column::Null(n) => *n,
            Column::Xor(_) => panic!("len() not implemented for xor compressed columns"),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
