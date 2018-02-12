use std::marker::PhantomData;
use bit_vec::BitVec;

use mem_store::column::ColIter;
use mem_store::ingest::RawVal;
use value::Val;
use mem_store::column::Column;
use engine::typed_vec::TypedVec;


pub type BoxedOperator<'a, T> = Box<VecOperator<Output=T> + 'a>;

pub trait VecOperator {
    type Output;

    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output);
}

pub struct CollectUntyped<'a> {
    col: &'a Column,
}

impl<'a> CollectUntyped<'a> {
    pub fn new(col: &'a Column) -> CollectUntyped<'a> {
        CollectUntyped {
            col: col,
        }
    }
}

impl<'a> VecOperator for CollectUntyped<'a> {
    type Output = Vec<Val<'a>>;

    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        self.col.dump_untyped(count as usize, 0, operand);
    }
}


pub struct CollectStr<'a> {
    col: &'a Column,
}

impl<'a> CollectStr<'a> {
    pub fn new(col: &'a Column) -> CollectStr<'a> {
        CollectStr {
            col: col,
        }
    }
}

impl<'a> VecOperator for CollectStr<'a> {
    type Output = Vec<&'a str>;

    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        self.col.collect_str(count as usize, 0, filter, operand);
    }
}

pub struct CollectInt<'a> {
    col: &'a Column,
}

impl<'a> CollectInt<'a> {
    pub fn new(col: &'a Column) -> CollectInt<'a> {
        CollectInt {
            col: col,
        }
    }
}

impl<'a> VecOperator for CollectInt<'a> {
    type Output = Vec<i64>;

    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        self.col.collect_int(count as usize, 0, filter, operand);
    }
}


pub struct TagStr<'a> {
    operator: Box<VecOperator<Output=Vec<&'a str>> + 'a>,
}

impl<'a> TagStr<'a> {
    pub fn new(operator: Box<VecOperator<Output=Vec<&'a str>> + 'a>) -> TagStr<'a> {
        TagStr {
            operator: operator
        }
    }
}

impl<'a> VecOperator for TagStr<'a> {
    type Output = TypedVec<'a>;


    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        let mut buffer = Vec::with_capacity(count as usize);
        self.operator.execute(count, filter, &mut buffer);
        *operand = TypedVec::String(buffer);
    }
}


pub struct TagInt<'a> {
    operator: Box<VecOperator<Output=Vec<i64>> + 'a>,
}


impl<'a> TagInt<'a> {
    pub fn new(operator: Box<VecOperator<Output=Vec<i64>> + 'a>) -> TagInt<'a> {
        TagInt {
            operator: operator
        }
    }
}

impl<'a> VecOperator for TagInt<'a> {
    type Output = TypedVec<'a>;


    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        let mut buffer = Vec::with_capacity(count as usize);
        self.operator.execute(count, filter, &mut buffer);
        *operand = TypedVec::Integer(buffer);
    }
}


pub struct TagBool<'a> {
    operator: BoxedOperator<'a, BitVec>,
}

impl<'a> TagBool<'a> {
    pub fn new(operator: BoxedOperator<BitVec>) -> TagBool {
        TagBool {
            operator: operator
        }
    }
}

impl<'a> VecOperator for TagBool<'a> {
    type Output = TypedVec<'a>;


    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        let mut buffer = BitVec::from_elem(count as usize, false);
        self.operator.execute(count, filter, &mut buffer);
        *operand = TypedVec::Boolean(buffer);
    }
}

pub struct TagConst<'a> {
    constant: RawVal,
    phantom: PhantomData<&'a ()>,
}

impl<'a> TagConst<'a> {
    pub fn new(constant: RawVal) -> TagConst<'static> {
        TagConst {
            constant: constant,
            phantom: PhantomData,
        }
    }
}

impl<'a> VecOperator for TagConst<'a> {
    type Output = TypedVec<'a>;


    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        *operand = TypedVec::Constant(self.constant.clone());
    }
}

pub struct LessThanVi64S<'a> {
    lhs: BoxedOperator<'a, Vec<i64>>,
    rhs: i64,
}

impl<'a> LessThanVi64S<'a> {
    pub fn new(lhs: BoxedOperator<Vec<i64>>, rhs: i64) -> LessThanVi64S {
        LessThanVi64S {
            lhs: lhs,
            rhs: rhs,
        }
    }
}

impl<'a> VecOperator for LessThanVi64S<'a> {
    type Output = BitVec;

    fn execute(&mut self, count: i32, filter: &Option<BitVec>, operand: &mut Self::Output) {
        let mut buffer = Vec::with_capacity(count as usize);
        self.lhs.execute(count, filter, &mut buffer);
        let i = self.rhs;
        for j in 0..(count as usize) {
            operand.set(j, buffer[j] < i);
        }
    }
}
