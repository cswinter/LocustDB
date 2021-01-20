use chrono::{Datelike, NaiveDateTime};
use locustdb_derive::ASTBuilder;
use regex;
use regex::Regex;

use crate::QueryError;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::*;
use crate::mem_store::column::DataSource;
use crate::mem_store::value::Val;
use std::collections::HashMap;
use std::i64;
use std::result::Result;
use std::sync::Arc;
use crate::syntax::expression::*;

#[derive(Debug, Clone, ASTBuilder)]
pub enum QueryPlan {
    /// Retrieves the buffer for the specified section of the column with name `name`.
    ColumnSection {
        name: String,
        section: usize,
        #[nohash]
        range: Option<(i64, i64)>,
        #[output(t = "base=provided")]
        column_section: TypedBufferRef,
    },
    Connect {
        input: TypedBufferRef,
        output: TypedBufferRef,
    },
    /// Combines a vector with a null map.
    AssembleNullable {
        data: TypedBufferRef,
        present: BufferRef<u8>,
        #[output(t = "base=data;null=_always")]
        nullable: TypedBufferRef,
    },
    /// Combines a vector with the null map of another vector.
    PropagateNullability {
        nullable: TypedBufferRef,
        data: TypedBufferRef,
        #[output(t = "base=data;null=_always")]
        nullable_data: TypedBufferRef,
    },
    /// Combines the null maps of two vectors, setting the result to null if any if the inputs were null.
    CombineNullMaps {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        present: BufferRef<u8>,
    },
    GetNullMap {
        nullable: TypedBufferRef,
        #[output]
        present: BufferRef<u8>,
    },
    /// Combines a vector with a null map where none of the elements are null.
    MakeNullable {
        data: TypedBufferRef,
        #[internal]
        present: BufferRef<u8>,
        #[output(t = "base=data;null=_always")]
        nullable: TypedBufferRef,
    },
    /// Converts NullableI64 or NullableStr into a representation where nulls are encoded as part
    /// of the data (i64 with i64::MIN representing null for NullableI64, and Option<&str> for NullableStr).
    FuseNulls {
        nullable: TypedBufferRef,
        #[output(t = "base=nullable;null=_fused")]
        fused: TypedBufferRef,
    },
    /// Converts Nullable integer types into a representation where nulls are encoded as value `null`.
    FuseIntNulls {
        offset: i64,
        nullable: TypedBufferRef,
        #[output(t = "base=nullable;null=_never")]
        fused: TypedBufferRef,
    },
    /// Inverse of `FuseNulls`.
    UnfuseNulls {
        fused: TypedBufferRef,
        #[internal(t = "base=fused;null=_never")]
        data: TypedBufferRef,
        #[internal]
        present: BufferRef<u8>,
        #[output(t = "base=fused;null=_always")]
        unfused: TypedBufferRef,
    },
    /// Inverse of `FuseNulls`.
    UnfuseIntNulls {
        offset: i64,
        fused: TypedBufferRef,
        #[internal(t = "base=fused;null=_never")]
        data: TypedBufferRef,
        #[internal]
        present: BufferRef<u8>,
        #[output(t = "base=fused;null=_always")]
        unfused: TypedBufferRef,
    },
    IsNull {
        plan: BufferRef<Nullable<Any>>,
        #[output]
        is_null: BufferRef<u8>,
    },
    IsNotNull {
        plan: BufferRef<Nullable<Any>>,
        #[output]
        is_not_null: BufferRef<u8>,
    },
    /// Resolves dictionary indices to their original string value.
    DictLookup {
        indices: TypedBufferRef,
        offset_len: BufferRef<u64>,
        backing_store: BufferRef<u8>,
        #[output(t = "base=str;null=indices")]
        decoded: TypedBufferRef,
    },
    /// Determines what dictionary index a string constant corresponds to.
    InverseDictLookup {
        offset_len: BufferRef<u64>,
        backing_store: BufferRef<u8>,
        constant: BufferRef<Scalar<&'static str>>,
        #[output]
        decoded: BufferRef<Scalar<i64>>,
    },
    /// Casts `input` to the specified type.
    Cast {
        input: TypedBufferRef,
        #[output(t = "base=provided;null=input")]
        casted: TypedBufferRef,
    },
    /// LZ4 decodes `bytes` into `decoded_len` elements of type `t`.
    LZ4Decode {
        bytes: BufferRef<u8>,
        decoded_len: usize,
        #[output(t = "base=provided")]
        decoded: TypedBufferRef,
    },
    /// Decodes a byte array of tightly packed strings.
    UnpackStrings {
        bytes: BufferRef<u8>,
        #[output]
        unpacked_strings: BufferRef<&'static str>,
    },
    /// Decodes a byte array of tightly packed, hex encoded string.
    UnhexpackStrings {
        bytes: BufferRef<u8>,
        uppercase: bool,
        total_bytes: usize,
        #[internal]
        string_store: BufferRef<u8>,
        #[output]
        unpacked_strings: BufferRef<&'static str>,
    },
    /// Decodes delta encoded integers.
    DeltaDecode {
        plan: TypedBufferRef,
        #[output]
        delta_decoded: BufferRef<i64>,
    },
    HashMapGrouping {
        raw_grouping_key: TypedBufferRef,
        max_cardinality: usize,
        #[output(t = "base=raw_grouping_key")]
        unique: TypedBufferRef,
        #[output]
        grouping_key: BufferRef<u32>,
        #[output]
        cardinality: BufferRef<Scalar<i64>>,
    },
    HashMapGroupingValRows {
        raw_grouping_key: BufferRef<ValRows<'static>>,
        columns: usize,
        max_cardinality: usize,
        #[output]
        unique: BufferRef<ValRows<'static>>,
        #[output]
        grouping_key: BufferRef<u32>,
        #[output]
        cardinality: BufferRef<Scalar<i64>>,
    },
    /// Creates a byte vector of size `max_index` and sets all entries
    /// corresponding to `indices` to 1.
    Exists {
        indices: TypedBufferRef,
        max_index: BufferRef<Scalar<i64>>,
        #[output]
        exists: BufferRef<u8>,
    },
    /// Deletes all zero entries from `plan`.
    NonzeroCompact {
        plan: TypedBufferRef,
        #[output(t = "base=plan")]
        compacted: TypedBufferRef,
    },
    /// Determines the indices of all entries in `plan` that are non-zero.
    NonzeroIndices {
        plan: TypedBufferRef,
        #[output(t = "base=provided")]
        nonzero_indices: TypedBufferRef,
    },
    /// Deletes all entries in `plan` for which the corresponding entry in `select` is 0.
    Compact {
        plan: TypedBufferRef,
        select: TypedBufferRef,
        #[output(t = "base=plan")]
        compacted: TypedBufferRef,
    },
    /// Sums `lhs` and `rhs << shift`.
    BitPack {
        lhs: BufferRef<i64>,
        rhs: BufferRef<i64>,
        shift: i64,
        #[output]
        bit_packed: BufferRef<i64>,
    },
    /// Retrieves the integer bit packed into `plan` at `shift..shift+width`.
    BitUnpack {
        plan: BufferRef<i64>,
        shift: u8,
        width: u8,
        #[output]
        unpacked: BufferRef<i64>,
    },
    SlicePack {
        plan: TypedBufferRef,
        stride: usize,
        offset: usize,
        #[output(shared_byte_slices)]
        packed: BufferRef<Any>,
    },
    SliceUnpack {
        plan: BufferRef<Any>,
        stride: usize,
        offset: usize,
        #[output(t = "base=provided")]
        unpacked: TypedBufferRef,
    },
    ValRowsPack {
        plan: BufferRef<Val<'static>>,
        stride: usize,
        offset: usize,
        #[output(shared_val_rows)]
        packed: BufferRef<ValRows<'static>>,
    },
    ValRowsUnpack {
        plan: BufferRef<ValRows<'static>>,
        stride: usize,
        offset: usize,
        #[output]
        unpacked: BufferRef<Val<'static>>,
    },
    Aggregate {
        plan: TypedBufferRef,
        grouping_key: TypedBufferRef,
        max_index: BufferRef<Scalar<i64>>,
        aggregator: Aggregator,
        #[output(t = "base=provided")]
        aggregate: TypedBufferRef,
    },
    CheckedAggregate {
        plan: TypedBufferRef,
        grouping_key: TypedBufferRef,
        max_index: BufferRef<Scalar<i64>>,
        aggregator: Aggregator,
        #[output(t = "base=provided")]
        aggregate: TypedBufferRef,
    },
    LessThan {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        less_than: TypedBufferRef,
    },
    LessThanEquals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        less_than_equals: TypedBufferRef,
    },
    Equals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        equals: TypedBufferRef,
    },
    NotEquals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        not_equals: TypedBufferRef,
    },
    Add {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        sum: TypedBufferRef,
    },
    CheckedAdd {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        sum: TypedBufferRef,
    },
    NullableCheckedAdd {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        present: BufferRef<u8>,
        #[output]
        sum: BufferRef<Nullable<i64>>,
    },
    Subtract {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        difference: TypedBufferRef,
    },
    CheckedSubtract {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        difference: TypedBufferRef,
    },
    NullableCheckedSubtract {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        present: BufferRef<u8>,
        #[output]
        difference: BufferRef<Nullable<i64>>,
    },
    Multiply {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        product: TypedBufferRef,
    },
    CheckedMultiply {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        product: TypedBufferRef,
    },
    NullableCheckedMultiply {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        present: BufferRef<u8>,
        #[output]
        product: BufferRef<Nullable<i64>>,
    },
    Divide {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        division: TypedBufferRef,
    },
    CheckedDivide {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        division: TypedBufferRef,
    },
    NullableCheckedDivide {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        present: BufferRef<u8>,
        #[output]
        division: BufferRef<Nullable<i64>>,
    },
    Modulo {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        modulo: TypedBufferRef,
    },
    CheckedModulo {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=i64;null=lhs,rhs")]
        modulo: TypedBufferRef,
    },
    NullableCheckedModulo {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        present: BufferRef<u8>,
        #[output]
        modulo: BufferRef<Nullable<i64>>,
    },
    And {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        and: TypedBufferRef,
    },
    Or {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=u8;null=lhs,rhs")]
        or: TypedBufferRef,
    },
    Not {
        input: BufferRef<u8>,
        #[output]
        not: BufferRef<u8>,
    },
    ToYear {
        timestamp: TypedBufferRef,
        #[output(t = "base=i64;null=timestamp")]
        year: TypedBufferRef,
    },
    Regex {
        plan: BufferRef<&'static str>,
        regex: String,
        #[output]
        matches: BufferRef<u8>,
    },
    Length {
        string: BufferRef<&'static str>,
        #[output]
        length: BufferRef<i64>,
    },
    /// Outputs a vector of indices from `0..plan.len()`
    Indices {
        plan: TypedBufferRef,
        #[output]
        indices: BufferRef<usize>,
    },
    /// Outputs a permutation of `indices` under which `ranking` is sorted.
    SortBy {
        ranking: TypedBufferRef,
        indices: BufferRef<usize>,
        desc: bool,
        stable: bool,
        #[output]
        permutation: BufferRef<usize>,
    },
    /// Outputs the `n` largest/smallest elements of `ranking` and their corresponding indices.
    TopN {
        ranking: TypedBufferRef,
        n: usize,
        desc: bool,
        #[internal(t = "base=ranking")]
        tmp_keys: TypedBufferRef,
        #[output]
        top_n: BufferRef<usize>,
    },
    /// Outputs all elements in `plan` where the index corresponds to an entry in `indices`.
    Select {
        plan: TypedBufferRef,
        indices: BufferRef<usize>,
        #[output(t = "base=plan")]
        selection: TypedBufferRef,
    },
    /// Outputs all elements in `plan` for which the corresponding entry in `select` is nonzero.
    Filter {
        plan: TypedBufferRef,
        select: BufferRef<u8>,
        #[output(t = "base=plan")]
        filtered: TypedBufferRef,
    },
    /// Outputs all elements in `plan` for which the corresponding entry in `select` is nonzero and not null.
    NullableFilter {
        plan: TypedBufferRef,
        select: BufferRef<Nullable<u8>>,
        #[output(t = "base=plan")]
        filtered: TypedBufferRef,
    },
    ConstantVec {
        index: usize,
        #[output(t = "base=provided")]
        constant_vec: TypedBufferRef,
    },
    NullVec {
        len: usize,
        #[output(t = "base=provided")]
        nulls: TypedBufferRef,
    },
    ScalarI64 {
        value: i64,
        hide_value: bool,
        #[output]
        scalar_i64: BufferRef<Scalar<i64>>,
    },
    ScalarStr {
        value: String,
        #[internal]
        pinned_string: BufferRef<Scalar<String>>,
        #[output]
        scalar_str: BufferRef<Scalar<&'static str>>,
    },
    /// Outputs a vector with all values equal to `value`.
    ConstantExpand {
        value: i64,
        len: usize,
        #[output(t = "base=provided")]
        expanded: TypedBufferRef,
    },
    /// Merges `lhs` and `rhs` and outputs a merge plan .
    Merge {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        limit: usize,
        desc: bool,
        #[output]
        merge_ops: BufferRef<u8>,
        #[output(t = "base=lhs;null=lhs,rhs")]
        merged: TypedBufferRef,
    },
    /// Merges `lhs` and `lhs` while respecting the `partitioning` and output a merge plan.
    MergePartitioned {
        partitioning: BufferRef<Premerge>,
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        limit: usize,
        desc: bool,
        #[output]
        take_left: BufferRef<u8>,
        #[output(t = "base=lhs")]
        merged: TypedBufferRef,
    },
    /// Merges `lhs` and `lhs` dropping duplicates, and outputs a merge plan.
    MergeDeduplicate {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        merge_ops: BufferRef<MergeOp>,
        #[output(t = "base=lhs")]
        merged: TypedBufferRef,
    },
    /// Merges `lhs` and `lhs` dropping duplicates while respecting the `partitioning`,
    /// and output a merge plan.
    MergeDeduplicatePartitioned {
        partitioning: BufferRef<Premerge>,
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        merge_ops: BufferRef<MergeOp>,
        #[output(t = "base=lhs")]
        merged: TypedBufferRef,
    },
    /// Identifies runs of identical elements when merging `lhs` and `rhs`.
    /// `lhs` and `rhs` are assumed to be sorted.
    Partition {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        limit: usize,
        desc: bool,
        #[output]
        partitioning: BufferRef<Premerge>,
    },
    /// Identifies runs of identical elements when merging `lhs` and `rhs` within an existing partitioning.
    /// `lhs` and `rhs` are assumed to be sorted.
    Subpartition {
        partitioning: BufferRef<Premerge>,
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        desc: bool,
        #[output]
        subpartitioning: BufferRef<Premerge>,
    },
    /// Merges `lhs` and `rhs` such that the resulting vector contains an element from lhs at position `i` iff `take_left[i] == 1`.
    MergeKeep {
        take_left: BufferRef<u8>,
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=lhs;null=lhs,rhs")]
        merged: TypedBufferRef,
    },
    /// Merges `lhs` and `lhs` according to `merge_ops`, dropping duplicates.
    MergeDrop {
        merge_ops: BufferRef<MergeOp>,
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output(t = "base=lhs")]
        merged: TypedBufferRef,
    },
    /// Merges `lhs` and `lhs` according to `merge_ops`, combining duplicates.
    MergeAggregate {
        merge_ops: BufferRef<MergeOp>,
        lhs: BufferRef<i64>,
        rhs: BufferRef<i64>,
        aggregator: Aggregator,
        #[output]
        merged: BufferRef<i64>,
    },
}

// TODO: return struct
#[allow(clippy::type_complexity)]
pub fn prepare_hashmap_grouping(raw_grouping_key: TypedBufferRef,
                                columns: usize,
                                max_cardinality: usize,
                                planner: &mut QueryPlanner)
                                -> Result<(Option<TypedBufferRef>,
                                           TypedBufferRef,
                                           bool,
                                           BufferRef<Scalar<i64>>), QueryError> {
    let (unique_out, grouping_key_out, cardinality_out) =
        if raw_grouping_key.tag == EncodingType::ValRows {
            let (u, g, c) = planner.hash_map_grouping_val_rows(raw_grouping_key.val_rows()?, columns, max_cardinality);
            (u.into(), g, c)
        } else {
            planner.hash_map_grouping(raw_grouping_key, max_cardinality)
        };
    Ok((Some(unique_out),
        grouping_key_out.into(),
        false,
        cardinality_out))
}

pub fn prepare_aggregation(mut plan: TypedBufferRef,
                           plan_type: Type,
                           grouping_key: TypedBufferRef,
                           max_index: BufferRef<Scalar<i64>>,
                           aggregator: Aggregator,
                           planner: &mut QueryPlanner)
                           -> Result<(TypedBufferRef, Type), QueryError> {
    Ok(match aggregator {
        Aggregator::Count => {
            let plan = if plan.tag == EncodingType::ScalarI64 { grouping_key } else { plan };
            (planner.aggregate(plan, grouping_key, max_index, Aggregator::Count, EncodingType::U32),
             Type::encoded(Codec::integer_cast(EncodingType::U32)))
        }
        Aggregator::Sum => {
            if !plan_type.is_summation_preserving() {
                plan = plan_type.codec.unwrap().decode(plan, planner);
            }
            // PERF: determine dense groupings
            (planner.checked_aggregate(plan, grouping_key, max_index, Aggregator::Sum, EncodingType::I64),
             Type::unencoded(BasicType::Integer))
        }
        Aggregator::Max | Aggregator::Min => {
            // PERF: don't always have to decode before taking max/min, and after is more efficient (e.g. dict encoded strings)
            plan = plan_type.codec.unwrap().decode(plan, planner);
            (planner.aggregate(plan, grouping_key, max_index, aggregator, EncodingType::I64),
             Type::unencoded(BasicType::Integer))
        }
    })
}

pub fn order_preserving((plan, t): (TypedBufferRef, Type),
                        planner: &mut QueryPlanner) -> (TypedBufferRef, Type) {
    if t.is_order_preserving() {
        (plan, t)
    } else {
        let new_type = t.decoded();
        (t.codec.unwrap().decode(plan, planner), new_type)
    }
}

type Factory = Box<dyn Fn(&mut QueryPlanner, TypedBufferRef, TypedBufferRef) -> TypedBufferRef + Sync>;

struct Function2 {
    pub factory: Factory,
    pub type_rhs: BasicType,
    pub type_lhs: BasicType,
    pub type_out: Type,
    pub encoding_invariance: bool,
}

impl Function2 {
    pub fn integer_op(factory: Factory) -> Function2 {
        Function2 {
            factory,
            type_lhs: BasicType::Integer,
            type_rhs: BasicType::Integer,
            type_out: Type::unencoded(BasicType::Integer).mutable(),
            encoding_invariance: false,
        }
    }

    pub fn comparison_op(factory: Factory,
                         t: BasicType) -> Function2 {
        Function2 {
            factory,
            type_lhs: t,
            type_rhs: t,
            type_out: Type::unencoded(BasicType::Boolean).mutable(),
            encoding_invariance: true,
        }
    }
}

lazy_static! {
    static ref FUNCTION2_REGISTRY: HashMap<Func2Type, Vec<Function2>> = function2_registry();
}

fn function2_registry() -> HashMap<Func2Type, Vec<Function2>> {
    vec![
        (Func2Type::Add,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs| qp.checked_add(lhs, rhs)))]),
        (Func2Type::Subtract,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs| qp.checked_subtract(lhs, rhs)))]),
        (Func2Type::Multiply,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs| qp.checked_multiply(lhs, rhs)))]),
        (Func2Type::Divide,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs| qp.checked_divide(lhs, rhs)))]),
        (Func2Type::Modulo,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs| qp.checked_modulo(lhs, rhs)))]),
        (Func2Type::LT,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than(lhs, rhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than(lhs, rhs)),
                                       BasicType::String)]),
        (Func2Type::LTE,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than_equals(lhs, rhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than_equals(lhs, rhs)),
                                       BasicType::String)]),
        (Func2Type::GT,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than(rhs, lhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than(rhs, lhs)),
                                       BasicType::String)]),
        (Func2Type::GTE,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than_equals(rhs, lhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.less_than_equals(rhs, lhs)),
                                       BasicType::String)]),
        (Func2Type::Equals,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.equals(lhs, rhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.equals(lhs, rhs)),
                                       BasicType::String)]),
        (Func2Type::NotEquals,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.not_equals(lhs, rhs)),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs| qp.not_equals(lhs, rhs)),
                                       BasicType::String)]),
    ].into_iter().collect()
}

impl QueryPlan {
    #[allow(clippy::trivial_regex)]
    pub fn compile_expr(
        expr: &Expr,
        filter: Filter,
        columns: &HashMap<String, Arc<dyn DataSource>>,
        column_len: usize,
        planner: &mut QueryPlanner) -> Result<(TypedBufferRef, Type), QueryError> {
        use self::Expr::*;
        use self::Func2Type::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let mut plan = planner.column_section(name, 0, c.range(), c.encoding_type());
                    let mut t = c.full_type();
                    if !c.codec().is_elementwise_decodable() {
                        let (codec, fixed_width) = c.codec().ensure_fixed_width(plan, planner);
                        t = Type::encoded(codec);
                        plan = fixed_width;
                    }
                    plan = match filter {
                        Filter::U8(filter) => planner.filter(plan, filter),
                        Filter::NullableU8(filter) => planner.nullable_filter(plan, filter),
                        Filter::Indices(indices) => planner.select(plan, indices),
                        Filter::None => plan,
                    };
                    (plan, t)
                }
                None => (planner.null_vec(column_len, EncodingType::Null), Type::new(BasicType::Null, None)),
            }
            Func2(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, column_len, planner)?;
                let (plan_rhs, type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, column_len, planner)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} OR {}, expected bool OR bool")
                }
                (planner.or(plan_lhs, plan_rhs), Type::bit_vec())
            }
            Func2(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, column_len, planner)?;
                let (plan_rhs, type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, column_len, planner)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (planner.and(plan_lhs, plan_rhs), Type::bit_vec())
            }
            Func2(Like, ref expr, ref pattern) => {
                match pattern {
                    box Const(RawVal::Str(pattern)) => {
                        let mut pattern = pattern.to_string();
                        pattern = regex::escape(&pattern);
                        pattern = Regex::new(r"([^\\])_").unwrap().replace_all(&pattern, "$1.").to_string();
                        pattern = Regex::new(r"\\_").unwrap().replace_all(&pattern, "_").to_string();
                        while pattern.contains("%%%%") {
                            pattern = pattern.replace("%%%%", "%%");
                        }
                        pattern = pattern.replace("%%%", "(%.*)|(.*%)");
                        pattern = Regex::new(r"([^%])%([^%])").unwrap().replace_all(&pattern, "$1.*$2").to_string();
                        pattern = Regex::new(r"^%([^%])").unwrap().replace_all(&pattern, ".*$1").to_string();
                        pattern = Regex::new(r"([^%])%$").unwrap().replace_all(&pattern, "$1.*").to_string();
                        pattern = Regex::new(r"%%").unwrap().replace_all(&pattern, "%").to_string();
                        pattern = format!("^{}$", pattern);
                        let (mut plan, t) = QueryPlan::compile_expr(expr, filter, columns, column_len, planner)?;
                        if t.decoded != BasicType::String {
                            bail!(QueryError::TypeError,
                                  "Expected expression of type `String` as first argument to LIKE. Actual: {:?}", t)
                        }
                        if let Some(codec) = t.codec {
                            plan = codec.decode(plan, planner);
                        }
                        let type_out = Type::unencoded(BasicType::Boolean).mutable();
                        (planner.regex(plan.str()?, &pattern).into(), type_out)
                    }
                    _ => bail!(QueryError::TypeError,
                               "Expected string constant as second argument to `LIKE`, actual: {:?}", pattern),
                }
            }
            Func2(NotLike, ref expr, ref pattern) => {
                QueryPlan::compile_expr(
                    &Expr::Func1(
                        Func1Type::Not,
                        Box::new(Expr::Func2(Func2Type::Like, expr.clone(), pattern.clone()))
                    ), filter, columns, column_len, planner)?
            }
            Func2(RegexMatch, ref expr, ref regex) => {
                match regex {
                    box Const(RawVal::Str(regex)) => {
                        Regex::new(&regex).map_err(|e| QueryError::TypeError(
                            format!("`{}` is not a valid regex: {}", regex, e)))?;
                        let (mut plan, t) = QueryPlan::compile_expr(expr, filter, columns, column_len, planner)?;
                        if t.decoded != BasicType::String {
                            bail!(QueryError::TypeError, "Expected expression of type `String` as first argument to regex. Actual: {:?}", t)
                        }
                        if let Some(codec) = t.codec {
                            plan = codec.decode(plan, planner);
                        }
                        let type_out = Type::unencoded(BasicType::Boolean).mutable();
                        (planner.regex(plan.str()?, regex).into(), type_out)
                    }
                    _ => bail!(QueryError::TypeError, "Expected string constant as second argument to `regex`, actual: {:?}", regex),
                }
            }
            Func2(function, ref lhs, ref rhs) => {
                let (mut plan_lhs, type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, column_len, planner)?;
                let (mut plan_rhs, type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, column_len, planner)?;

                let declarations = match FUNCTION2_REGISTRY.get(&function) {
                    Some(patterns) => patterns,
                    None => bail!(QueryError::NotImplemented, "function {:?}", function),
                };
                let declaration = match declarations.iter().find(
                    |p| p.type_lhs == type_lhs.decoded.non_nullable() &&
                        p.type_rhs == type_rhs.decoded.non_nullable()) {
                    Some(declaration) => declaration,
                    None => bail!(
                        QueryError::TypeError,
                        "Function {:?} is not implemented for types {:?}, {:?}",
                        function, type_lhs, type_rhs),
                };

                if declaration.encoding_invariance && type_lhs.is_scalar && type_rhs.is_encoded() {
                    plan_lhs = if type_rhs.decoded == BasicType::Integer {
                        if let QueryPlan::ScalarI64 { value, .. } = *planner.resolve(&plan_lhs) {
                            planner.scalar_i64(type_rhs.codec.unwrap().encode_int(value), true).into()
                        } else {
                            panic!("whoops");
                        }
                    } else if type_rhs.decoded == BasicType::String {
                        type_rhs.codec.clone().unwrap().encode_str(plan_lhs.scalar_str()?, planner).into()
                    } else {
                        panic!("whoops");
                    };
                } else if declaration.encoding_invariance && type_rhs.is_scalar && type_lhs.is_encoded() {
                    plan_rhs = if type_lhs.decoded == BasicType::Integer {
                        if let QueryPlan::ScalarI64 { value, .. } = *planner.resolve(&plan_rhs) {
                            planner.scalar_i64(type_lhs.codec.unwrap().encode_int(value), true).into()
                        } else {
                            panic!("whoops");
                        }
                    } else if type_lhs.decoded == BasicType::String {
                        type_lhs.codec.clone().unwrap().encode_str(plan_rhs.scalar_str()?, planner).into()
                    } else {
                        panic!("whoops");
                    };
                } else {
                    if let Some(codec) = type_lhs.codec {
                        plan_lhs = codec.decode(plan_lhs, planner);
                    }
                    if let Some(codec) = type_rhs.codec {
                        plan_rhs = codec.decode(plan_rhs, planner);
                    }
                }

                let plan = (declaration.factory)(planner, plan_lhs, plan_rhs);
                (plan, declaration.type_out.clone())
            }
            Func1(Func1Type::Negate, box Const(RawVal::Int(i))) => {
                QueryPlan::compile_expr(&Const(RawVal::Int(-i)), filter, columns, column_len, planner)?
            }
            Func1(ftype, ref inner) => {
                let (plan, t) = QueryPlan::compile_expr(inner, filter, columns, column_len, planner)?;
                let plan = match ftype {
                    Func1Type::ToYear => {
                        let decoded = match t.codec.clone() {
                            Some(codec) => codec.decode(plan, planner),
                            None => plan,
                        };
                        if t.decoded != BasicType::Integer {
                            bail!(QueryError::TypeError, "Found to_year({:?}), expected to_year(integer)", &t)
                        }
                        planner.to_year(decoded)
                    }
                    Func1Type::Length => {
                        let decoded = match t.codec.clone() {
                            Some(codec) => codec.decode(plan, planner),
                            None => plan,
                        };
                        if t.decoded != BasicType::String {
                            bail!(QueryError::TypeError, "Found length({:?}), expected length(string)", &t)
                        }
                        planner.length(decoded.str()?).into()
                    }
                    Func1Type::Not => {
                        let decoded = match t.codec.clone() {
                            Some(codec) => codec.decode(plan, planner),
                            None => plan,
                        };
                        if t.decoded != BasicType::Boolean {
                            bail!(QueryError::TypeError, "Found NOT({:?}), expected NOT(boolean)", &t)
                        }
                        planner.not(decoded.u8()?).into()
                    }
                    Func1Type::IsNull => if plan.is_nullable() {
                        planner.is_null(plan.nullable_any()?).into()
                    } else {
                        planner.constant_expand((false as u8) as i64, column_len, EncodingType::U8)
                    }
                    Func1Type::IsNotNull => if plan.is_nullable() {
                        planner.is_not_null(plan.nullable_any()?).into()
                    } else {
                        planner.constant_expand((true as u8) as i64, column_len, EncodingType::U8)
                    }
                    Func1Type::Negate => {
                        bail!(QueryError::TypeError, "Unary minus not implemented for arbitrary expressions.")
                    }
                };
                (plan, t.decoded())
            }
            Const(RawVal::Int(i)) => (planner.scalar_i64(i, false).into(), Type::scalar(BasicType::Integer)),
            Const(RawVal::Str(ref s)) => (planner.scalar_str(s).into(), Type::scalar(BasicType::String)),
            ref x => bail!(QueryError::NotImplemented, "{:?}.compile_vec()", x),
        })
    }
}

fn encoding_range(plan: &TypedBufferRef, qp: &QueryPlanner) -> Option<(i64, i64)> {
    // This would benefit from more principled approach - it currently doesn't work for all partially decodings
    // Example: [LZ4, Add, Delta] will have as bottom decoding range the range after indices, max_index Delta, but without the Add :/
    // This works in this case because we always have to decode the Delta, but is hard to reason about and has caused bugs
    use self::QueryPlan::*;
    match *qp.resolve(plan) {
        ColumnSection { range, .. } => range,
        ToYear { timestamp, .. } => encoding_range(&timestamp, qp).map(|(min, max)|
            (i64::from(NaiveDateTime::from_timestamp(min, 0).year()),
             i64::from(NaiveDateTime::from_timestamp(max, 0).year()))
        ),
        Filter { ref plan, .. } => encoding_range(plan, qp),
        Divide { ref lhs, ref rhs, .. } => if let ScalarI64 { value: c, .. } = qp.resolve(rhs) {
            encoding_range(lhs, qp).map(|(min, max)|
                if *c > 0 { (min / *c, max / *c) } else { (max / *c, min / *c) })
        } else {
            None
        },
        CheckedDivide { ref lhs, ref rhs, .. } => if let ScalarI64 { value: c, .. } = qp.resolve(rhs) {
            encoding_range(lhs, qp).map(|(min, max)|
                if *c > 0 { (min / *c, max / *c) } else { (max / *c, min / *c) })
        } else {
            None
        },
        Add { ref lhs, ref rhs, .. } => if let ScalarI64 { value: c, .. } = qp.resolve(rhs) {
            encoding_range(lhs, qp).map(|(min, max)| (min + *c, max + *c))
        } else {
            None
        },
        Cast { ref input, .. } => encoding_range(input, qp),
        LZ4Decode { bytes, .. } => encoding_range(&bytes.into(), qp),
        DeltaDecode { ref plan, .. } => encoding_range(plan, qp),
        AssembleNullable { ref data, .. } => encoding_range(data, qp),
        UnpackStrings { .. } | UnhexpackStrings { .. } | Length { .. } => None,
        ref plan => {
            error!("encoding_range not implement for {:?}", plan);
            None
        }
    }
}

// TODO: return struct
#[allow(clippy::type_complexity)]
pub fn compile_grouping_key(
    exprs: &Vec<Expr>,
    filter: Filter,
    columns: &HashMap<String, Arc<dyn DataSource>>,
    partition_len: usize,
    planner: &mut QueryPlanner)
    -> Result<((TypedBufferRef, bool), i64, Vec<(TypedBufferRef, Type)>, TypedBufferRef), QueryError> {
    if exprs.is_empty() {
        let mut plan = planner.constant_expand(0, partition_len, EncodingType::U8);
        plan = match filter {
            Filter::U8(filter) => planner.filter(plan, filter),
            Filter::NullableU8(filter) => planner.nullable_filter(plan, filter),
            Filter::Indices(indices) => planner.select(plan, indices),
            Filter::None => plan,
        };
        Ok((
            (plan, true),
            1,
            vec![],
            planner.buffer_provider.named_buffer("empty_group_by", EncodingType::Null)
        ))
    } else if exprs.len() == 1 {
        QueryPlan::compile_expr(&exprs[0], filter, columns, partition_len, planner)
            .map(|(mut gk_plan, gk_type)| {
                let original_plan = gk_plan;
                let encoding_range = encoding_range(&gk_plan, planner);
                debug!("Encoding range of {:?} for {:?}", &encoding_range, &gk_plan);
                let (max_cardinality, offset) = match encoding_range {
                    Some((min, max)) => if min <= 0 && gk_plan.is_nullable() {
                        (max - min + 1, Some(-min + 1))
                    } else if gk_plan.is_nullable() {
                        (max, Some(0))
                    } else if min < 0 {
                        (max - min, Some(-min))
                    } else {
                        (max, None)
                    }
                    None => (1 << 62, None),
                };

                if gk_plan.is_nullable() {
                    gk_plan = match offset {
                        Some(offset) => planner.fuse_int_nulls(offset, gk_plan),
                        None => planner.fuse_nulls(gk_plan),
                    }
                } else if let Some(offset) = offset {
                    let offset = planner.scalar_i64(offset, true);
                    gk_plan = planner.add(gk_plan, offset.into());
                }

                let encoded_group_by_placeholder = planner.buffer_provider.named_buffer(
                    "encoded_group_by_placeholder", gk_plan.tag);
                let mut decoded_group_by = encoded_group_by_placeholder;
                if original_plan.is_nullable() {
                    decoded_group_by = match offset {
                        Some(offset) => planner.unfuse_int_nulls(offset, decoded_group_by),
                        None => planner.unfuse_nulls(decoded_group_by),
                    }
                } else if let Some(offset) = offset {
                    let offset = planner.scalar_i64(-offset, true);
                    let sum = planner.add(decoded_group_by, offset.into());
                    decoded_group_by = planner.cast(sum, gk_type.encoding_type());
                }
                if let Some(codec) = gk_type.codec.clone() {
                    decoded_group_by = codec.decode(decoded_group_by, planner)
                }

                ((gk_plan, gk_type.is_order_preserving()),
                 max_cardinality,
                 vec![(decoded_group_by, gk_type.decoded())],
                 encoded_group_by_placeholder)
            })
    } else if let Some(result) = try_bitpacking(exprs, filter, columns, partition_len, planner)? {
        Ok(result)
    } else {
        info!("Failed to bitpack grouping key");
        let mut pack = Vec::new();
        let mut decode_plans = Vec::new();
        let encoded_group_by_placeholder = planner.buffer_provider.named_buffer(
            "encoded_group_by_placeholder", EncodingType::ValRows);
        let mut order_preserving = true;
        for (i, expr) in exprs.iter().enumerate() {
            let (query_plan, plan_type) = QueryPlan::compile_expr(expr, filter, columns, partition_len, planner)?;
            order_preserving = order_preserving && plan_type.is_order_preserving();
            let vals = planner.cast(query_plan, EncodingType::Val).val()?;
            pack.push(planner.val_rows_pack(vals, exprs.len(), i));

            let vals = planner.val_rows_unpack(
                encoded_group_by_placeholder.val_rows()?,
                exprs.len(),
                i).into();
            let mut decode_plan = planner.cast(vals, query_plan.tag);
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan, planner);
            }
            decode_plans.push(
                (decode_plan,
                 plan_type.decoded()));
        }
        Ok(((pack[0].clone().into(), order_preserving),
            i64::MAX,
            decode_plans,
            encoded_group_by_placeholder))
        // PERF: evaluate performance of ValRows vs ByteSlices grouping
        /*} else {
            info!("Failed to bitpack grouping key");
            let mut pack = Vec::new();
            let mut decode_plans = Vec::new();
            let encoded_group_by_placeholder =
                planner.buffer_provider.named_buffer("encoded_group_by_placeholder", EncodingType::ByteSlices(exprs.len()));
            for (i, expr) in exprs.iter().enumerate() {
                let (query_plan, plan_type) = QueryPlan::compile_expr(expr, filter, columns, partition_len, planner)?;
                pack.push(planner.slice_pack(query_plan, exprs.len(), i));

                // Before reactivating: negative integers can throw off sort order - need to move into positive range
                let mut decode_plan = planner.slice_unpack(
                    encoded_group_by_placeholder.any(),
                    exprs.len(),
                    i,
                    plan_type.encoding_type());
                if let Some(codec) = plan_type.codec.clone() {
                    decode_plan = codec.decode(decode_plan, planner);
                }
                decode_plans.push(
                    (decode_plan,
                     plan_type.decoded()));
            }
            Ok(((pack[0].into(), true),
                i64::MAX,
                decode_plans,
                encoded_group_by_placeholder))*/
    }
}

// TODO: return struct
#[allow(clippy::type_complexity)]
fn try_bitpacking(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<dyn DataSource>>,
    partition_len: usize,
    planner: &mut QueryPlanner)
    -> Result<Option<((TypedBufferRef, bool), i64, Vec<(TypedBufferRef, Type)>, TypedBufferRef)>, QueryError> {
    planner.checkpoint();
    // PERF: use u64 as grouping key type
    let mut total_width = 0;
    let mut largest_key = 0;
    let mut plan: Option<BufferRef<i64>> = None;
    let mut decode_plans = Vec::with_capacity(exprs.len());
    let mut order_preserving = true;
    let encoded_group_by_placeholder = planner.buffer_provider.buffer_i64("encoded_group_by_placeholder");
    for expr in exprs.iter().rev() {
        let (query_plan, plan_type) = QueryPlan::compile_expr(expr, filter, columns, partition_len, planner)?;
        let encoding_range = encoding_range(&query_plan, planner);
        debug!("Encoding range of {:?} for {:?}", &encoding_range, &query_plan);
        if let Some((min, max)) = encoding_range {
            fn bits(max: i64) -> i64 {
                ((max + 1) as f64).log2().ceil() as i64
            }
            let max = if query_plan.is_nullable() && min <= 0 { max + 1 } else { max };

            // PERF: more intelligent criterion. threshold should probably be a function of total width.
            let subtract_offset = bits(max) - bits(max - min) > 1 || min < 0 || query_plan.is_nullable();
            let adjusted_max = if query_plan.is_nullable() { max - min + 1 } else if subtract_offset { max - min } else { max };
            order_preserving = order_preserving && plan_type.is_order_preserving();
            let adjusted_query_plan = if query_plan.is_nullable() {
                let fused = planner.fuse_int_nulls(-min + 1, query_plan);
                if fused.tag != EncodingType::I64 {
                    planner.cast(fused, EncodingType::I64).i64()?
                } else {
                    fused.i64()?
                }
            } else if subtract_offset {
                let offset = planner.scalar_i64(-min, true);
                planner.add(query_plan, offset.into()).i64()?
            } else {
                planner.cast(query_plan, EncodingType::I64).i64()?
            };

            if total_width == 0 {
                plan = Some(adjusted_query_plan);
            } else if adjusted_max > 0 || query_plan.is_nullable() {
                plan = plan.map(|plan| planner.bit_pack(plan, adjusted_query_plan, total_width));
            }

            let mut decode_plan = planner.bit_unpack(
                encoded_group_by_placeholder,
                total_width as u8,
                bits(adjusted_max) as u8).into();
            if query_plan.is_nullable() {
                decode_plan = planner.unfuse_int_nulls(-min + 1, decode_plan);
            } else if subtract_offset {
                let offset = planner.scalar_i64(min, true);
                decode_plan = planner.add(decode_plan, offset.into());
            }
            decode_plan = planner.cast(decode_plan, plan_type.encoding_type());
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan, planner);
            }
            decode_plans.push((decode_plan, plan_type.decoded()));

            largest_key += adjusted_max << total_width;
            total_width += bits(adjusted_max);
        } else {
            planner.reset();
            return Ok(None);
        }
    }

    Ok(
        if total_width <= 63 {
            plan.map(|plan| {
                decode_plans.reverse();
                ((plan.into(), order_preserving), largest_key, decode_plans, encoded_group_by_placeholder.into())
            })
        } else {
            planner.reset();
            None
        }
    )
}

pub(super) fn prepare<'a>(plan: QueryPlan, constant_vecs: &mut Vec<BoxedData<'a>>, result: &mut QueryExecutor<'a>) -> Result<TypedBufferRef, QueryError> {
    trace!("{:?}", &plan);
    let operation: Box<dyn VecOperator> = match plan {
        QueryPlan::Select { plan, indices, selection } => VecOperator::select(plan, indices, selection)?,
        QueryPlan::ColumnSection { name, section, column_section, .. } => VecOperator::read_column_data(name, section, column_section.any()),
        QueryPlan::AssembleNullable { data, present, nullable } => VecOperator::nullable(data, present, nullable)?,
        QueryPlan::MakeNullable { data, present, nullable } => VecOperator::make_nullable(data, present, nullable)?,
        QueryPlan::PropagateNullability { nullable, data, nullable_data } => VecOperator::propagate_nullability(nullable.nullable_any()?, data, nullable_data)?,
        QueryPlan::CombineNullMaps { lhs, rhs, present } => VecOperator::combine_null_maps(lhs, rhs, present)?,
        QueryPlan::GetNullMap { nullable, present } => VecOperator::get_null_map(nullable.nullable_any()?, present),
        QueryPlan::FuseNulls { nullable, fused } => VecOperator::fuse_nulls(nullable, fused)?,
        QueryPlan::FuseIntNulls { offset, nullable, fused } => VecOperator::fuse_int_nulls(offset, nullable, fused)?,
        QueryPlan::UnfuseNulls { fused, data, present, unfused } => VecOperator::unfuse_nulls(fused, data, present, unfused)?,
        QueryPlan::UnfuseIntNulls { offset, fused, data, present, unfused } => VecOperator::unfuse_int_nulls(offset, fused, data, present, unfused)?,
        QueryPlan::Filter { plan, select, filtered } => VecOperator::filter(plan, select, filtered)?,
        QueryPlan::NullableFilter { plan, select, filtered } => VecOperator::nullable_filter(plan, select, filtered)?,
        QueryPlan::IsNull { plan, is_null } => VecOperator::is_null(plan, is_null),
        QueryPlan::IsNotNull { plan, is_not_null } => VecOperator::is_not_null(plan, is_not_null),
        QueryPlan::ScalarI64 { value, hide_value, scalar_i64 } => VecOperator::scalar_i64(value, hide_value, scalar_i64),
        QueryPlan::ScalarStr { value, pinned_string, scalar_str } => VecOperator::scalar_str(value, pinned_string, scalar_str),
        QueryPlan::NullVec { len, nulls } => VecOperator::null_vec(len, nulls.any()),
        QueryPlan::ConstantExpand { value, len, expanded } => VecOperator::constant_expand(value, len, expanded)?,
        QueryPlan::DictLookup { indices, offset_len, backing_store, decoded } => VecOperator::dict_lookup(indices, offset_len, backing_store, decoded.str()?)?,
        QueryPlan::InverseDictLookup { offset_len, backing_store, constant, decoded } => VecOperator::inverse_dict_lookup(offset_len, backing_store, constant, decoded),
        QueryPlan::Cast { input, casted } => VecOperator::type_conversion(input, casted)?,
        QueryPlan::DeltaDecode { plan, delta_decoded } => VecOperator::delta_decode(plan, delta_decoded)?,
        QueryPlan::LZ4Decode { bytes, decoded_len, decoded } => VecOperator::lz4_decode(bytes, decoded_len, decoded)?,
        QueryPlan::UnpackStrings { bytes, unpacked_strings } => VecOperator::unpack_strings(bytes, unpacked_strings),
        QueryPlan::UnhexpackStrings { bytes, uppercase, total_bytes, string_store, unpacked_strings } => VecOperator::unhexpack_strings(bytes, uppercase, total_bytes, string_store, unpacked_strings),
        QueryPlan::HashMapGrouping { raw_grouping_key, max_cardinality, unique, grouping_key, cardinality } => VecOperator::hash_map_grouping(raw_grouping_key, max_cardinality, unique, grouping_key, cardinality)?,
        QueryPlan::HashMapGroupingValRows { raw_grouping_key, max_cardinality, columns, unique, grouping_key, cardinality } => VecOperator::hash_map_grouping_val_rows(raw_grouping_key, columns, max_cardinality, unique, grouping_key, cardinality)?,
        QueryPlan::Aggregate { plan, grouping_key, max_index, aggregator, aggregate } => VecOperator::aggregate(plan, grouping_key, max_index, aggregator, aggregate)?,
        QueryPlan::CheckedAggregate { plan, grouping_key, max_index, aggregator, aggregate } => VecOperator::checked_aggregate(plan, grouping_key, max_index, aggregator, aggregate)?,
        QueryPlan::Exists { indices, max_index, exists } => VecOperator::exists(indices, max_index, exists)?,
        QueryPlan::Compact { plan, select, compacted } => VecOperator::compact(plan, select, compacted)?,
        QueryPlan::NonzeroIndices { plan, nonzero_indices } => VecOperator::nonzero_indices(plan, nonzero_indices)?,
        QueryPlan::NonzeroCompact { plan, compacted } => VecOperator::nonzero_compact(plan, compacted)?,
        QueryPlan::BitPack { lhs, rhs, shift, bit_packed } => VecOperator::bit_shift_left_add(lhs, rhs, bit_packed, shift),
        QueryPlan::BitUnpack { plan, shift, width, unpacked } => VecOperator::bit_unpack(plan, shift, width, unpacked),
        QueryPlan::SlicePack { plan, stride, offset, packed } => VecOperator::slice_pack(plan, stride, offset, packed)?,
        QueryPlan::SliceUnpack { plan, stride, offset, unpacked } => VecOperator::slice_unpack(plan, stride, offset, unpacked)?,
        QueryPlan::ValRowsPack { plan, stride, offset, packed } => VecOperator::val_rows_pack(plan, stride, offset, packed),
        QueryPlan::ValRowsUnpack { plan, stride, offset, unpacked } => VecOperator::val_rows_unpack(plan, stride, offset, unpacked),
        QueryPlan::LessThan { lhs, rhs, less_than } => VecOperator::less_than(lhs, rhs, less_than.u8()?)?,
        QueryPlan::LessThanEquals { lhs, rhs, less_than_equals } => VecOperator::less_than_equals(lhs, rhs, less_than_equals.u8()?)?,
        QueryPlan::Equals { lhs, rhs, equals } => VecOperator::equals(lhs, rhs, equals.u8()?)?,
        QueryPlan::NotEquals { lhs, rhs, not_equals } => VecOperator::not_equals(lhs, rhs, not_equals.u8()?)?,
        QueryPlan::Add { lhs, rhs, sum } => VecOperator::addition(lhs, rhs, sum.i64()?)?,
        QueryPlan::CheckedAdd { lhs, rhs, sum } => VecOperator::checked_addition(lhs, rhs, sum.i64()?)?,
        QueryPlan::NullableCheckedAdd { lhs, rhs, present, sum } => VecOperator::nullable_checked_addition(lhs, rhs, present, sum)?,
        QueryPlan::Subtract { lhs, rhs, difference } => VecOperator::subtraction(lhs, rhs, difference.i64()?)?,
        QueryPlan::CheckedSubtract { lhs, rhs, difference } => VecOperator::checked_subtraction(lhs, rhs, difference.i64()?)?,
        QueryPlan::NullableCheckedSubtract { lhs, rhs, present, difference } => VecOperator::nullable_checked_subtraction(lhs, rhs, present, difference)?,
        QueryPlan::Multiply { lhs, rhs, product } => VecOperator::multiplication(lhs, rhs, product.i64()?)?,
        QueryPlan::CheckedMultiply { lhs, rhs, product } => VecOperator::checked_multiplication(lhs, rhs, product.i64()?)?,
        QueryPlan::NullableCheckedMultiply { lhs, rhs, present, product } => VecOperator::nullable_checked_multiplication(lhs, rhs, present, product)?,
        QueryPlan::Divide { lhs, rhs, division } => VecOperator::division(lhs, rhs, division.i64()?)?,
        QueryPlan::CheckedDivide { lhs, rhs, division } => VecOperator::checked_division(lhs, rhs, division.i64()?)?,
        QueryPlan::NullableCheckedDivide { lhs, rhs, present, division } => VecOperator::nullable_checked_division(lhs, rhs, present, division)?,
        QueryPlan::Modulo { lhs, rhs, modulo } => VecOperator::modulo(lhs, rhs, modulo.i64()?)?,
        QueryPlan::CheckedModulo { lhs, rhs, modulo } => VecOperator::checked_modulo(lhs, rhs, modulo.i64()?)?,
        QueryPlan::NullableCheckedModulo { lhs, rhs, present, modulo } => VecOperator::nullable_checked_modulo(lhs, rhs, present, modulo)?,
        QueryPlan::Or { lhs, rhs, or } => VecOperator::or(lhs.u8()?, rhs.u8()?, or.u8()?),
        QueryPlan::And { lhs, rhs, and } => VecOperator::and(lhs.u8()?, rhs.u8()?, and.u8()?),
        QueryPlan::Not { input, not } => VecOperator::not(input, not),
        QueryPlan::ToYear { timestamp, year } => VecOperator::to_year(timestamp.i64()?, year.i64()?),
        QueryPlan::Regex { plan, regex, matches } => VecOperator::regex(plan, &regex, matches),
        QueryPlan::Length { string, length } => VecOperator::length(string, length),
        QueryPlan::Indices { plan, indices } => VecOperator::indices(plan, indices),
        QueryPlan::SortBy { ranking, indices, desc, stable, permutation } => VecOperator::sort_by(ranking, indices, desc, stable, permutation)?,
        QueryPlan::TopN { ranking, n, desc, tmp_keys, top_n } => VecOperator::top_n(ranking, tmp_keys, n, desc, top_n)?,
        QueryPlan::Connect { input, output } => VecOperator::identity(input, output),
        QueryPlan::Merge { lhs, rhs, limit, desc, merge_ops, merged } => VecOperator::merge(lhs, rhs, limit, desc, merge_ops, merged)?,
        QueryPlan::MergePartitioned { partitioning, lhs, rhs, limit, desc, take_left, merged } => VecOperator::merge_partitioned(partitioning, lhs, rhs, limit, desc, take_left, merged)?,
        QueryPlan::MergeDeduplicate { lhs, rhs, merge_ops, merged } => VecOperator::merge_deduplicate(lhs, rhs, merge_ops, merged)?,
        QueryPlan::MergeDeduplicatePartitioned { partitioning, lhs, rhs, merge_ops, merged } => VecOperator::merge_deduplicate_partitioned(partitioning, lhs, rhs, merge_ops, merged)?,
        QueryPlan::Partition { lhs, rhs, limit, desc, partitioning } => VecOperator::partition(lhs, rhs, limit, desc, partitioning)?,
        QueryPlan::Subpartition { partitioning, lhs, rhs, desc, subpartitioning } => VecOperator::subpartition(partitioning, lhs, rhs, desc, subpartitioning)?,
        QueryPlan::MergeDrop { merge_ops, lhs, rhs, merged } => VecOperator::merge_drop(merge_ops, lhs, rhs, merged)?,
        QueryPlan::MergeKeep { take_left, lhs, rhs, merged } => VecOperator::merge_keep(take_left, lhs, rhs, merged)?,
        QueryPlan::MergeAggregate { merge_ops, lhs, rhs, aggregator, merged } => VecOperator::merge_aggregate(merge_ops, lhs, rhs, aggregator, merged),
        QueryPlan::ConstantVec { index, constant_vec } => VecOperator::constant_vec(std::mem::replace(&mut constant_vecs[index], Data::empty(1)), constant_vec.any()),
    };
    result.push(operation);
    Ok(result.last_buffer())
}
