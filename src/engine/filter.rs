use bit_vec::BitVec;
use std::rc::Rc;

#[derive(Clone)]
pub enum Filter {
    None,
    BitVec(Rc<BitVec>),
    Indices(Rc<Vec<usize>>),
}