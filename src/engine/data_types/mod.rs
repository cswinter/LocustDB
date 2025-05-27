mod byte_slices;
mod data;
mod nullable_vec_data;
mod scalar_data;
mod types;
mod val_rows;
mod vec_data;

use ordered_float::OrderedFloat;

pub use self::byte_slices::*;
pub use self::data::*;
pub use self::nullable_vec_data::*;
pub use self::scalar_data::*;
pub use self::types::*;
pub use self::val_rows::*;
pub use self::vec_data::*;

#[allow(non_camel_case_types)]
pub type of64 = OrderedFloat<f64>;

pub fn vec_f64_to_vec_of64(vec: Vec<f64>) -> Vec<of64> {
    unsafe { std::mem::transmute::<Vec<f64>, Vec<of64>>(vec) }
}
