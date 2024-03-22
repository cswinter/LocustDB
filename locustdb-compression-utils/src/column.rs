use serde::{Deserialize, Serialize};

use crate::xor_float;


#[derive(Deserialize, Serialize)]
pub enum Column {
    Float(Vec<f64>),
    Xor(Vec<u8>),
}


impl Column {
    pub fn compress(xs: Vec<f64>) -> Column {
        let xor_compressed = xor_float::double::encode(&xs, 100, None);
        // Require at least 1.5x compression to use xor
        if xor_compressed.len() * 6 > xor_compressed.len() {
            Column::Xor(Vec::from(xor_compressed))
        } else {
            Column::Float(xs)
        }
    }
}
