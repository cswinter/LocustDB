use fnv::FnvHashMap;

use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::Val;

#[derive(Debug)]
pub struct HashMapGroupingValRows<'a> {
    input: BufferRef<ValRows<'a>>,
    unique_out: BufferRef<ValRows<'a>>,
    grouping_key_out: BufferRef<u32>,
    cardinality_out: BufferRef<Scalar<i64>>,
    columns: usize,
}

impl<'a> HashMapGroupingValRows<'a> {
    pub fn boxed(input: BufferRef<ValRows<'a>>,
                 unique_out: BufferRef<ValRows<'a>>,
                 grouping_key_out: BufferRef<u32>,
                 cardinality_out: BufferRef<Scalar<i64>>,
                 columns: usize) -> BoxedOperator<'a> {
        Box::new(HashMapGroupingValRows {
            input,
            unique_out,
            grouping_key_out,
            cardinality_out,
            columns,
        })
    }
}

impl<'a> VecOperator<'a> for HashMapGroupingValRows<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        // TODO(#100): Fnv is suboptimal for larger inputs (http://cglab.ca/~abeinges/blah/hash-rs/). use xx hash?
        let count = {
            let raw_grouping_key = scratchpad.get_mut_val_rows(self.input);
            let mut map: FnvHashMap<&[Val<'a>], u32> = FnvHashMap::default();
            let mut grouping = scratchpad.get_mut(self.grouping_key_out);
            let mut unique = scratchpad.get_mut_val_rows(self.unique_out);
            if stream { grouping.clear() }
            for row in raw_grouping_key.data.chunks(raw_grouping_key.row_len) {
                grouping.push(*map.entry(row).or_insert_with(|| {
                    for slice in row {
                        unique.data.push(*slice);
                    }
                    unique.len() as u32 - 1
                }));
            }
            RawVal::Int(unique.len() as i64)
        };
        scratchpad.set_any(self.cardinality_out.any(), Data::constant(count));
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_any(self.unique_out.any(), Box::new(ValRows::new(self.columns)));
        scratchpad.set(self.grouping_key_out, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unique_out.any(), self.grouping_key_out.any(), self.cardinality_out.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, output: usize) -> bool { output != self.unique_out.i }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("hashmap_grouping_val_rows({})", self.input)
    }
}

