use engine::typed_vec::TypedVec;
use std::fmt::Debug;
use std::cmp::{max, min};
use engine::aggregator::Aggregator;
use engine::types::*;


pub struct BatchResult<'a> {
    pub group_by: Option<TypedVec<'a>>,
    pub sort_by: Option<usize>,
    pub select: Vec<TypedVec<'a>>,
    pub aggregators: Vec<Aggregator>,
    pub level: u32,
    pub batch_count: usize,
}

impl<'a> BatchResult<'a> {
    pub fn len(&self) -> usize {
        match self.group_by {
            Some(ref g) => g.len(),
            None => self.select[0].len(),
        }
    }
}

#[derive(Debug)]
pub enum MergeOp {
    TakeLeft,
    TakeRight,
    MergeRight,
}

pub fn combine<'a>(batch1: BatchResult<'a>, batch2: BatchResult<'a>) -> BatchResult<'a> {
    match (batch1.group_by, batch2.group_by) {
        (Some(g1), Some(g2)) => {
            // TODO(clemens): other types, val coercion
            let (merged_grouping, ops) = match (g1.get_type(), g2.get_type()) {
                (EncodingType::Str, EncodingType::Str) =>
                    merge_deduplicate(g1.cast_ref_str(), g2.cast_ref_str()),
                (EncodingType::I64, EncodingType::I64) =>
                    merge_deduplicate(g1.cast_ref_i64(), g2.cast_ref_i64()),
                (t1, t2) => unimplemented!("{:?}, {:?}", t1, t2),
            };

            let mut aggregates = Vec::with_capacity(batch1.aggregators.len());
            for (i, aggregator) in batch1.aggregators.iter().enumerate() {
                let merged = merge_aggregate(
                    batch1.select[i].cast_ref_i64(),
                    batch2.select[i].cast_ref_i64(),
                    &ops, *aggregator);
                aggregates.push(merged);
            }

            BatchResult {
                group_by: Some(merged_grouping),
                sort_by: None,
                select: aggregates,
                aggregators: batch1.aggregators,
                level: batch1.level + 1,
                batch_count: batch1.batch_count + batch2.batch_count,
            }
        }
        (None, None) => {
            match batch1.sort_by {
                Some(index) => {
                    let (merged_sort_col, ops) = {
                        let s1 = &batch1.select[index];
                        let s2 = &batch2.select[index];
                        match (s1.get_type(), s2.get_type()) {
                            (EncodingType::Str, EncodingType::Str) =>
                                merge_sort(s1.cast_ref_str(), s2.cast_ref_str()),
                            (EncodingType::I64, EncodingType::I64) =>
                                merge_sort(s1.cast_ref_i64(), s2.cast_ref_i64()),
                            (t1, t2) => unimplemented!("{:?}, {:?}", t1, t2),
                        }
                    };

                    let mut result = Vec::with_capacity(batch1.select.len());
                    for (i, (col1, col2)) in batch1.select.into_iter().zip(batch2.select).enumerate() {
                        if i == index {
                            result.push(TypedVec::Empty);
                        } else {
                            let merged = match (col1.get_type(), col2.get_type()) {
                                (EncodingType::Str, EncodingType::Str) =>
                                    merge(col1.cast_ref_str(), col2.cast_ref_str(), &ops),
                                (EncodingType::I64, EncodingType::I64) =>
                                    merge(col1.cast_ref_i64(), col2.cast_ref_i64(), &ops),
                                (t1, t2) => unimplemented!("{:?}, {:?}", t1, t2),
                            };
                            result.push(merged);
                        }
                    }
                    result[index] = merged_sort_col;

                    BatchResult {
                        group_by: None,
                        sort_by: Some(index),
                        select: result,
                        aggregators: Vec::new(),
                        level: batch1.level + 1,
                        batch_count: batch1.batch_count + batch2.batch_count,
                    }
                }
                None => {
                    let mut result = Vec::with_capacity(batch1.select.len());
                    // TODO(clemens): handle sort case
                    for (col1, col2) in batch1.select.into_iter().zip(batch2.select) {
                        result.push(col1.extend(col2))
                    }
                    BatchResult {
                        group_by: None,
                        sort_by: None,
                        select: result,
                        aggregators: Vec::new(),
                        level: batch1.level + 1,
                        batch_count: batch1.batch_count + batch2.batch_count,
                    }
                }
            }
        }
        _ => panic!("Trying to merge incompatible batch results"),
    }
}

fn merge_deduplicate<'a, T: PartialOrd + Copy + Debug + 'a>(left: &[T], right: &[T]) -> (TypedVec<'a>, Vec<MergeOp>)
    where Vec<T>: Into<TypedVec<'a>> {
    // TODO(clemens): figure out maths for precise estimate + variance derived from how much grouping reduced cardinality
    let output_len_estimate = max(left.len(), right.len()) + min(left.len(), right.len()) / 2;
    let mut result = Vec::with_capacity(output_len_estimate);
    let mut ops = Vec::<MergeOp>::with_capacity(output_len_estimate);

    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        if result.last() == Some(&right[j]) {
            ops.push(MergeOp::MergeRight);
            j += 1;
        } else if left[i] <= right[j] {
            result.push(left[i]);
            ops.push(MergeOp::TakeLeft);
            i += 1;
        } else {
            result.push(right[j]);
            ops.push(MergeOp::TakeRight);
            j += 1;
        }
    }

    for x in left[i..].iter() {
        result.push(*x);
        ops.push(MergeOp::TakeLeft);
    }
    if j < right.len() && result.last() == Some(&right[j]) {
        ops.push(MergeOp::MergeRight);
        j += 1;
    }
    for x in right[j..].iter() {
        result.push(*x);
        ops.push(MergeOp::TakeRight);
    }

    (result.into(), ops)
}

// TODO(clemens): implement descending ordering
fn merge_sort<'a, T: PartialOrd + Copy + Debug + 'a>(left: &[T], right: &[T]) -> (TypedVec<'a>, Vec<bool>)
    where Vec<T>: Into<TypedVec<'a>> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let mut ops = Vec::<bool>::with_capacity(left.len() + right.len());

    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        if left[i] <= right[j] {
            result.push(left[i]);
            ops.push(true);
            i += 1;
        } else {
            result.push(right[j]);
            ops.push(false);
            j += 1;
        }
    }

    for x in left[i..].iter() {
        result.push(*x);
        ops.push(true);
    }
    for x in right[j..].iter() {
        result.push(*x);
        ops.push(false);
    }

    (result.into(), ops)
}

fn merge_aggregate(left: &[i64], right: &[i64], ops: &Vec<MergeOp>, aggregator: Aggregator) -> TypedVec<'static> {
    let mut result = Vec::with_capacity(ops.len());
    let mut i = 0;
    let mut j = 0;
    for op in ops {
        match *op {
            MergeOp::TakeLeft => {
                result.push(left[i]);
                i += 1;
            }
            MergeOp::TakeRight => {
                result.push(right[j]);
                j += 1;
            }
            MergeOp::MergeRight => {
                // TODO(clemens): make inlining of aggregator operation possible
                let last = result.len() - 1;
                result[last] = aggregator.combine_i64(result[last], right[j]);
                j += 1;
            }
        }
    }
    result.into()
}

fn merge<'a, T: PartialOrd + Copy + Debug + 'a>(left: &[T], right: &[T], ops: &Vec<bool>) -> TypedVec<'a>
    where Vec<T>: Into<TypedVec<'a>> {
    let mut result = Vec::with_capacity(ops.len());
    let mut i = 0;
    let mut j = 0;
    for take_left in ops {
        if *take_left {
            result.push(left[i]);
            i += 1;
        } else {
            result.push(right[j]);
            j += 1;
        }
    }
    result.into()
}
