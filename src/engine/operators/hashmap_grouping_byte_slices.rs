use fnv::FnvHashMap;

use engine::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct HashMapGroupingByteSlices {
    input: BufferRef<Any>,
    unique_out: BufferRef<Any>,
    grouping_key_out: BufferRef<u32>,
    cardinality_out: BufferRef<Scalar<i64>>,
    columns: usize,
}

impl<'a> HashMapGroupingByteSlices {
    pub fn boxed(input: BufferRef<Any>,
                 unique_out: BufferRef<Any>,
                 grouping_key_out: BufferRef<u32>,
                 cardinality_out: BufferRef<Scalar<i64>>,
                 columns: usize) -> BoxedOperator<'a> {
        Box::new(HashMapGroupingByteSlices {
            input,
            unique_out,
            grouping_key_out,
            cardinality_out,
            columns,
        })
    }
}

impl<'a> VecOperator<'a> for HashMapGroupingByteSlices {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): Fnv is suboptimal for larger inputs (http://cglab.ca/~abeinges/blah/hash-rs/). use xx hash?
        let count = {
            let raw_grouping_key_any = scratchpad.get_any(self.input);
            let raw_grouping_key = raw_grouping_key_any.cast_ref_byte_slices();
            let mut map: FnvHashMap<&[&'a [u8]], u32> = FnvHashMap::default();
            let mut grouping = scratchpad.get_mut(self.grouping_key_out);
            let mut unique_any = scratchpad.get_any_mut(self.unique_out);
            let mut unique = unique_any.cast_ref_mut_byte_slices();
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
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): Estimate capacities for unique + map?
        scratchpad.set_any(self.unique_out, Box::new(ByteSlices::new(self.columns)));
        scratchpad.set(self.grouping_key_out, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unique_out, self.grouping_key_out.any(), self.cardinality_out.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, output: usize) -> bool { output != self.unique_out.i }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("hashmap_grouping({})", self.input)
    }
}

