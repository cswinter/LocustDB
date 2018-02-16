use bit_vec::BitVec;
use std::rc::Rc;
use mem_store::ingest::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};
use engine::typed_vec::TypedVec;
use query_engine::QueryStats;


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

pub trait VecOperator<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a>;
}


pub struct Decode<'a> { col: &'a ColumnData }

impl<'a> Decode<'a> {
    pub fn new(col: &'a ColumnData) -> Decode { Decode { col: col } }
}

impl<'a> VecOperator<'a> for Decode<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let result = self.col.collect_decoded();
        stats.record(&"decode");
        stats.ops += result.len();
        result
    }
}

pub struct FilterDecode<'a> {
    col: &'a ColumnData,
    filter: Rc<BitVec>,
}

impl<'a> FilterDecode<'a> {
    pub fn new(col: &'a ColumnData, filter: Rc<BitVec>) -> FilterDecode<'a> {
        FilterDecode {
            col: col,
            filter: filter,
        }
    }
}

impl<'a> VecOperator<'a> for FilterDecode<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let result = self.col.filter_decode(self.filter.as_ref());
        stats.record(&"filter_decode");
        stats.ops += self.filter.len();
        result
    }
}


pub struct GetEncoded<'a> { col: &'a ColumnCodec }

impl<'a> GetEncoded<'a> {
    pub fn new(col: &'a ColumnCodec) -> GetEncoded { GetEncoded { col: col } }
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let result = self.col.get_encoded();
        stats.record(&"get_encoded");
        result
    }
}


pub struct FilterEncoded<'a> {
    col: &'a ColumnCodec,
    filter: Rc<BitVec>,
}

impl<'a> FilterEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, filter: Rc<BitVec>) -> FilterEncoded<'a> {
        FilterEncoded {
            col: col,
            filter: filter,
        }
    }
}

impl<'a> VecOperator<'a> for FilterEncoded<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let result = self.col.filter_encoded(self.filter.as_ref());
        stats.record(&"filter_encoded");
        stats.ops += self.filter.len();
        result
    }
}


pub struct Constant { val: RawVal }

impl Constant {
    pub fn new(val: RawVal) -> Constant {
        Constant { val: val }
    }
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'static> {
        stats.start();
        let result = TypedVec::Constant(self.val.clone());
        stats.record(&"constant");
        result
    }
}


pub struct LessThanVSi64<'a> {
    lhs: BoxedOperator<'a>,
    rhs: i64,
}

impl<'a> LessThanVSi64<'a> {
    pub fn new(lhs: BoxedOperator, rhs: i64) -> LessThanVSi64 {
        LessThanVSi64 {
            lhs: lhs,
            rhs: rhs,
        }
    }
}

impl<'a> VecOperator<'a> for LessThanVSi64<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        let lhs = self.lhs.execute(stats);
        stats.start();
        let data = lhs.cast_i64();
        let mut result = BitVec::with_capacity(lhs.len());
        let i = self.rhs;
        for l in data {
            result.push(*l < i);
        }
        stats.record(&"less_than_vsi_64");
        stats.ops += data.len();
        TypedVec::Boolean(result)
    }
}


pub struct LessThanVSu8<'a> {
    lhs: BoxedOperator<'a>,
    rhs: u8,
}

impl<'a> LessThanVSu8<'a> {
    pub fn new(lhs: BoxedOperator, rhs: u8) -> LessThanVSu8 {
        LessThanVSu8 {
            lhs: lhs,
            rhs: rhs,
        }
    }
}

impl<'a> VecOperator<'a> for LessThanVSu8<'a> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        let lhs = self.lhs.execute(stats);
        stats.start();
        let data = lhs.cast_ref_u8();
        let mut result = BitVec::with_capacity(data.len());
        let i = self.rhs;
        for l in data {
            result.push(*l < i);
        }
        stats.record(&"less_than_vs_u8");
        stats.ops += data.len();
        TypedVec::Boolean(result)
    }
}
