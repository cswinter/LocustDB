use std::collections::HashMap;
use std::i64;
use std::result::Result;
use std::sync::Arc;
use std::ops::{Add, BitAnd, BitOr, Div};

use chrono::{Datelike, NaiveDateTime};
use crypto::digest::Digest;
use crypto::md5::Md5;
use itertools::Itertools;

use ::QueryError;
use engine::*;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column::DataSource;
use syntax::expression::*;
use self::syntax::*;
use locustdb_derive::EnumSyntax;


type TypedPlan = (QueryPlan, Type);

#[derive(Debug, Clone, EnumSyntax)]
pub enum QueryPlan {
    /// Retrieves the buffer for the specified section of the column with name `name`.
    ColumnSection {
        name: String,
        section: usize,
        range: Option<(i64, i64)>,
        t: EncodingType,
    },

    /// Retrieves a specific buffer.
    ReadBuffer { buffer: TypedBufferRef },

    /// Resolves dictionary indices to their original string value.
    DictLookup {
        indices: Box<QueryPlan>,
        offset_len: Box<QueryPlan>,
        backing_store: Box<QueryPlan>,
    },

    /// Determines what dictionary index a string constant corresponds to.
    InverseDictLookup {
        offset_len: Box<QueryPlan>,
        backing_store: Box<QueryPlan>,
        constant: Box<QueryPlan>,
    },

    /// Casts `input` to the specified type.
    Cast {
        input: Box<QueryPlan>,
        input_type: EncodingType,
        target_type: EncodingType,
    },

    /// LZ4 decodes `bytes` into `decoded_len` elements of type `t`.
    LZ4Decode {
        bytes: Box<QueryPlan>,
        decoded_len: usize,
        t: EncodingType,
    },

    /// Decodes a byte array of tightly packed strings.
    UnpackStrings { bytes: Box<QueryPlan> },

    /// Decodes a byte array of tightly packed, hex encoded string.
    UnhexpackStrings {
        bytes: Box<QueryPlan>,
        uppercase: bool,
        total_bytes: usize,
    },

    /// Decodes delta encoded integers.
    DeltaDecode { plan: Box<QueryPlan> },

    /// Creates a byte vector of size `max_index` and sets all entries
    /// corresponding to `indices` to 1.
    Exists { indices: Box<QueryPlan>, max_index: Box<QueryPlan> },

    /// Deletes all zero entries from `plan`.
    NonzeroCompact { plan: Box<QueryPlan> },

    /// Determines the indices of all entries in `plan` that are non-zero.
    NonzeroIndices {
        plan: Box<QueryPlan>,
        indices_type: EncodingType,
    },

    /// Deletes all entries in `plan` for which the corresponding entry in `select` is 0.
    Compact {
        plan: Box<QueryPlan>,
        select: Box<QueryPlan>,
    },

    /// Encodes `constant` according to `codec`.
    EncodeIntConstant {
        constant: Box<QueryPlan>,
        codec: Codec,
    },

    /// Sums `lhs` and `rhs << shift`.
    BitPack {
        lhs: Box<QueryPlan>,
        rhs: Box<QueryPlan>,
        shift: i64,
    },

    /// Retrieves the integer bit packed into `plan` at `shift..shift+width`.
    BitUnpack { plan: Box<QueryPlan>, shift: u8, width: u8 },

    SlicePack { plan: Box<QueryPlan>, stride: usize, offset: usize },

    SliceUnpack {
        plan: Box<QueryPlan>,
        t: EncodingType,
        stride: usize,
        offset: usize,
    },

    LessThanVS { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    EqualsVS { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    NotEqualsVS { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    DivideVS { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    AddVS { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    And { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    Or { lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
    ToYear { timestamp: Box<QueryPlan> },

    /// Outputs a vector of indices from `0..plan.len()`
    Indices { plan: Box<QueryPlan> },

    /// Outputs a permutation of `indices` under which `ranking` is sorted.
    SortBy {
        ranking: Box<QueryPlan>,
        indices: Box<QueryPlan>,
        desc: bool,
        stable: bool,
    },

    /// Outputs the `n` largest/smallest elements of `ranking` and their corresponding indices.
    TopN { ranking: Box<QueryPlan>, n: usize, desc: bool },

    /// Outputs all elements in `plan` where the index corresponds to an entry in `indices`.
    Select { plan: Box<QueryPlan>, indices: Box<QueryPlan> },

    /// Outputs all elements in `plan` for which the corresponding entry in `select` is nonzero.
    Filter { plan: Box<QueryPlan>, select: Box<QueryPlan> },

    EncodedGroupByPlaceholder,

    Convergence(Vec<Box<QueryPlan>>),

    Constant { value: RawVal, hide_value: bool },
}

impl QueryPlan {
    fn is_constant(&self) -> bool {
        if let QueryPlan::Constant { .. } = self { true } else { false }
    }
}

pub fn prepare(plan: QueryPlan, result: &mut QueryExecutor) -> Result<TypedBufferRef, QueryError> {
    _prepare(plan, false, result)
}

pub fn prepare_no_alias(plan: QueryPlan, result: &mut QueryExecutor) -> Result<TypedBufferRef, QueryError> {
    _prepare(plan, true, result)
}

fn _prepare(plan: QueryPlan, no_alias: bool, result: &mut QueryExecutor) -> Result<TypedBufferRef, QueryError> {
    trace!("{:?}", &plan);
    let (plan, signature) = if no_alias || plan.is_constant() {
        (plan, [0; 16])
    } else {
        // TODO(clemens): O(n^2) :(   use visitor pattern?
        let (box plan, signature) = replace_common_subexpression(plan, result);
        (plan, signature)
    };
    trace!("{:?} {}", &plan, to_hex_string(&signature));
    let operation: Box<VecOperator> = match plan {
        QueryPlan::Select { plan, indices } => {
            let input = prepare(*plan, result)?;
            let t = input.tag.clone();
            VecOperator::select(input,
                                prepare(*indices, result)?.usize()?,
                                result.named_buffer("selection", t))?
        }
        QueryPlan::ColumnSection { name, section, t, .. } =>
            VecOperator::read_column_data(name, section, result.named_buffer("column", t).any()),
        QueryPlan::Filter { plan, select } => {
            let input = prepare(*plan, result)?;
            let t = input.tag.clone();
            VecOperator::filter(input,
                                prepare(*select, result)?.u8()?,
                                result.named_buffer("filtered", t))?
        }
        QueryPlan::Constant { ref value, hide_value } =>
            VecOperator::constant(value.clone(), hide_value, result.buffer_raw_val("constant")),
        QueryPlan::DictLookup { indices, offset_len, backing_store } =>
            VecOperator::dict_lookup(
                prepare(*indices, result)?,
                prepare(*offset_len, result)?.u64()?,
                prepare(*backing_store, result)?.u8()?,
                result.buffer_str("decoded"))?,
        QueryPlan::InverseDictLookup { offset_len, backing_store, constant } =>
            VecOperator::inverse_dict_lookup(
                prepare(*offset_len, result)?.u64()?,
                prepare(*backing_store, result)?.u8()?,
                prepare(*constant, result)?.string()?,
                result.buffer_raw_val("encoded")),
        QueryPlan::Cast { input, input_type: _, target_type } =>
            VecOperator::type_conversion(
                prepare(*input, result)?,
                result.named_buffer("casted", target_type))?,
        QueryPlan::DeltaDecode { plan } =>
            VecOperator::delta_decode(
                prepare(*plan, result)?,
                result.buffer_i64("decoded"))?,
        QueryPlan::LZ4Decode { bytes, decoded_len, t } =>
            VecOperator::lz4_decode(
                prepare(*bytes, result)?.u8()?,
                result.named_buffer("decoded", t),
                decoded_len)?,
        QueryPlan::UnpackStrings { bytes } =>
            VecOperator::unpack_strings(
                prepare(*bytes, result)?.u8()?,
                result.buffer_str("unpacked")),
        QueryPlan::UnhexpackStrings { bytes, uppercase, total_bytes } => {
            let stringstore = result.buffer_u8("stringstore");
            VecOperator::unhexpack_strings(
                prepare(*bytes, result)?.u8()?,
                result.buffer_str("unpacked"),
                stringstore, uppercase, total_bytes)
        }
        QueryPlan::Exists { indices, max_index } =>
            VecOperator::exists(
                prepare(*indices, result)?,
                result.buffer_u8("exists"),
                prepare(*max_index, result)?.i64()?)?,
        QueryPlan::Compact { plan, select } => {
            let inplace = prepare(*plan, result)?;
            let op = VecOperator::compact(inplace, prepare(*select, result)?)?;
            result.push(op);
            return Ok(inplace);
        }
        QueryPlan::NonzeroIndices { plan, indices_type } =>
            VecOperator::nonzero_indices(
                prepare(*plan, result)?,
                result.named_buffer("nonzero_indices", indices_type))?,
        QueryPlan::NonzeroCompact { plan } => {
            let inplace = prepare(*plan, result)?;
            result.push(VecOperator::nonzero_compact(inplace)?);
            return Ok(inplace);
        }
        QueryPlan::EncodeIntConstant { constant, codec } =>
            VecOperator::encode_int_const(
                prepare(*constant, result)?.const_i64(),
                result.buffer_i64("encoded"),
                codec),
        QueryPlan::BitPack { lhs, rhs, shift } =>
            VecOperator::bit_shift_left_add(
                prepare(*lhs, result)?.i64()?,
                prepare(*rhs, result)?.i64()?,
                result.buffer_i64("bitpacked"),
                shift),
        QueryPlan::BitUnpack { plan, shift, width } =>
            VecOperator::bit_unpack(
                prepare(*plan, result)?.i64()?,
                result.buffer_i64("unpacked"),
                shift,
                width),
        QueryPlan::SlicePack { plan, stride, offset } =>
            VecOperator::slice_pack(
                prepare(*plan, result)?,
                result.shared_buffer("slicepack", EncodingType::ByteSlices(stride)).any(),
                stride,
                offset)?,
        QueryPlan::SliceUnpack { plan, t, stride, offset } =>
            VecOperator::slice_unpack(
                prepare(*plan, result)?.any(),
                result.named_buffer("unpacked", t),
                stride,
                offset)?,
        QueryPlan::Convergence(plans) => {
            let mut first = None;
            for p in plans {
                let x = prepare(*p, result)?;
                if first.is_none() {
                    first = Some(x);
                }
            }
            return Ok(first.unwrap());
        }

        QueryPlan::LessThanVS { lhs, rhs } =>
            VecOperator::less_than_vs(
                prepare(*lhs, result)?,
                prepare(*rhs, result)?.const_i64(),
                result.buffer_u8("less_than"))?,
        QueryPlan::EqualsVS { lhs, rhs } =>
            VecOperator::equals_vs(
                prepare(*lhs, result)?,
                prepare(*rhs, result)?,
                result.buffer_u8("equals"))?,
        QueryPlan::NotEqualsVS { lhs, rhs } =>
            VecOperator::not_equals_vs(
                prepare(*lhs, result)?,
                prepare(*rhs, result)?,
                result.buffer_u8("equals"))?,
        QueryPlan::DivideVS { lhs, rhs } =>
            VecOperator::divide_vs(
                prepare(*lhs, result)?.i64()?,
                prepare(*rhs, result)?.const_i64(),
                result.buffer_i64("division")),
        QueryPlan::AddVS { lhs, rhs } =>
            VecOperator::addition_vs(
                prepare(*lhs, result)?,
                prepare(*rhs, result)?.const_i64(),
                result.buffer_i64("addition"))?,
        QueryPlan::Or { lhs, rhs } => {
            let inplace = prepare(*lhs, result)?;
            let op = VecOperator::or(inplace.u8()?, prepare(*rhs, result)?.u8()?);
            result.push(op);
            return Ok(inplace);
        }
        QueryPlan::And { lhs, rhs } => {
            let inplace = prepare(*lhs, result)?;
            let op = VecOperator::and(inplace.u8()?, prepare(*rhs, result)?.u8()?);
            result.push(op);
            return Ok(inplace);
        }
        QueryPlan::ToYear { timestamp } =>
            VecOperator::to_year(prepare(*timestamp, result)?.i64()?, result.buffer_i64("year")),
        QueryPlan::EncodedGroupByPlaceholder => return Ok(result.encoded_group_by().unwrap()),
        QueryPlan::Indices { plan } => VecOperator::indices(
            prepare(*plan, result)?,
            result.buffer_usize("indices")),
        QueryPlan::SortBy { ranking, indices, desc, stable } => VecOperator::sort_by(
            prepare(*ranking, result)?,
            prepare(*indices, result)?.usize()?,
            result.buffer_usize("permutation"),
            desc,
            stable)?,
        QueryPlan::TopN { ranking, n, desc } => {
            let plan = prepare(*ranking, result)?;
            VecOperator::top_n(
                plan,
                result.named_buffer("tmp_keys", plan.tag),
                result.buffer_usize("top_n"),
                n, desc)?
        }
        QueryPlan::ReadBuffer { buffer } => return Ok(buffer),
    };
    result.push(operation);
    if signature != [0; 16] {
        result.cache_last(signature);
    }
    Ok(result.last_buffer())
}

pub fn prepare_hashmap_grouping(raw_grouping_key: TypedBufferRef,
                                max_cardinality: usize,
                                result: &mut QueryExecutor)
                                -> Result<(Option<TypedBufferRef>, TypedBufferRef, Type, BufferRef<i64>), QueryError> {
    let unique_out = result.named_buffer("unique", raw_grouping_key.tag.clone());
    let grouping_key_out = result.buffer_u32("grouping_key");
    let cardinality_out = result.buffer_i64("cardinality");
    result.push(
        VecOperator::hash_map_grouping(raw_grouping_key,
                                       unique_out,
                                       grouping_key_out,
                                       cardinality_out,
                                       max_cardinality)?);
    Ok((Some(unique_out),
        grouping_key_out.tagged(),
        Type::encoded(Codec::opaque(EncodingType::U32, BasicType::Integer, false, false, true, true)),
        cardinality_out))
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a>(plan: QueryPlan,
                               plan_type: Type,
                               grouping_key: TypedBufferRef,
                               max_index: BufferRef<i64>,
                               aggregator: Aggregator,
                               result: &mut QueryExecutor<'a>)
                               -> Result<(TypedBufferRef, Type), QueryError> {
    let output_location;
    let (operation, t): (BoxedOperator<'a>, _) = match (aggregator, plan) {
        (Aggregator::Count, _) => {
            output_location = result.named_buffer("count", EncodingType::U32);
            (VecOperator::count(grouping_key,
                                output_location.u32()?,
                                max_index)?,
             Type::encoded(Codec::integer_cast(EncodingType::U32)))
        }
        (Aggregator::Sum, mut plan) => {
            output_location = result.named_buffer("sum", EncodingType::I64);
            if !plan_type.is_summation_preserving() {
                plan = plan_type.codec.clone().unwrap().decode(plan);
            }
            (VecOperator::summation(prepare(plan, result)?,
                                    grouping_key,
                                    output_location.i64()?,
                                    max_index)?, // TODO(clemens): determine dense groupings
             Type::unencoded(BasicType::Integer))
        }
    };
    result.push(operation);
    Ok((output_location, t))
}

pub fn order_preserving((plan, t): (QueryPlan, Type)) -> (QueryPlan, Type) {
    if t.is_order_preserving() {
        (plan, t)
    } else {
        let new_type = t.decoded();
        (t.codec.unwrap().decode(plan), new_type)
    }
}

impl QueryPlan {
    pub fn create_query_plan(
        expr: &Expr,
        filter: Filter,
        columns: &HashMap<String, Arc<DataSource>>) -> Result<(QueryPlan, Type), QueryError> {
        use self::Expr::*;
        use self::Func2Type::*;
        use self::Func1Type::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let mut plan = column_section(name, 0, c.range(), c.encoding_type());
                    let mut t = c.full_type();
                    if !c.codec().is_elementwise_decodable() {
                        let (codec, fixed_width) = c.codec().ensure_fixed_width(plan);
                        t = Type::encoded(codec);
                        plan = fixed_width;
                    }
                    plan = match filter {
                        Filter::BitVec(filter) => query_syntax::filter(plan, filter.tagged()),
                        Filter::Indices(indices) => select(plan, indices.tagged()),
                        Filter::None => plan,
                    };
                    (plan, t)
                }
                None => bail!(QueryError::NotImplemented, "Referencing missing column {}", name)
            }
            Func2(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = encode_int_constant(plan_rhs, type_lhs.codec.clone().unwrap());
                                less_than_vs(plan_lhs, encoded)
                            } else {
                                less_than_vs(plan_lhs, plan_rhs)
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "< operator only implemented for column < constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} < {:?}", type_lhs, type_rhs)
                }
            }
            Func2(Equals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = type_lhs.codec.clone().unwrap().encode_str(plan_rhs);
                                equals_vs(plan_lhs, encoded)
                            } else {
                                equals_vs(plan_lhs, plan_rhs)
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "= operator only implemented for column = constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = encode_int_constant(plan_rhs, type_lhs.codec.clone().unwrap());
                                equals_vs(plan_lhs, encoded)
                            } else {
                                equals_vs(plan_lhs, plan_rhs)
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "= operator only implemented for column = constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} = {:?}", type_lhs, type_rhs)
                }
            }
            Func2(NotEquals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = type_lhs.codec.clone().unwrap().encode_str(plan_rhs);
                                not_equals_vs(plan_lhs, encoded)
                            } else {
                                not_equals_vs(plan_lhs, plan_rhs)
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "<> operator only implemented for column <> constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = encode_int_constant(plan_rhs, type_lhs.codec.clone().unwrap());
                                not_equals_vs(plan_lhs, encoded)
                            } else {
                                not_equals_vs(plan_lhs, plan_rhs)
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "<> operator only implemented for column <> constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} <> {:?}", type_lhs, type_rhs)
                }
            }
            Func2(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} OR {}, expected bool OR bool")
                }
                (plan_lhs | plan_rhs, Type::bit_vec())
            }
            Func2(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (plan_lhs & plan_rhs, Type::bit_vec())
            }
            Func2(Divide, ref lhs, ref rhs) => {
                let (mut plan_lhs, mut type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if let Some(codec) = type_lhs.codec {
                                plan_lhs = codec.decode(plan_lhs);
                            }
                            plan_lhs / plan_rhs
                        } else {
                            bail!(QueryError::NotImplemented, "/ operator only implemented for column / constant")
                        };
                        (plan, Type::unencoded(BasicType::Integer).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} / {:?}", type_lhs, type_rhs)
                }
            }
            Func1(ToYear, ref inner) => {
                let (plan, t) = QueryPlan::create_query_plan(inner, filter, columns)?;
                if t.decoded != BasicType::Integer {
                    bail!(QueryError::TypeError, "Found to_year({:?}), expected to_year(integer)", &t)
                }
                let decoded = match t.codec.clone() {
                    Some(codec) => codec.decode(plan),
                    None => plan,
                };
                (to_year(decoded), t.decoded())
            }
            Const(ref v) => (constant(v.clone(), false), Type::scalar(v.get_type())),
            ref x => bail!(QueryError::NotImplemented, "{:?}.compile_vec()", x),
        })
    }

    fn encoding_range(&self) -> Option<(i64, i64)> {
        // TODO(clemens): need more principled approach - this currently doesn't work for all partially decodings
        // Example: [LZ4, Add, Delta] will have as bottom decoding range the range after Delta, but without the Add :/
        // This works in this case because we always have to decode the Delta, but is hard to reason about and has caused bugs
        use self::QueryPlan::*;
        match *self {
            ColumnSection { range, .. } => range,
            ToYear { ref timestamp } => timestamp.encoding_range().map(|(min, max)|
                (i64::from(NaiveDateTime::from_timestamp(min, 0).year()),
                 i64::from(NaiveDateTime::from_timestamp(max, 0).year()))
            ),
            Filter { ref plan, .. } => plan.encoding_range(),
            // TODO(clemens): this is just wrong
            DivideVS { ref lhs, rhs: box Constant { value: RawVal::Int(c), .. } } =>
                lhs.encoding_range().map(|(min, max)|
                    if c > 0 { (min / c, max / c) } else { (max / c, min / c) }),
            AddVS { ref lhs, rhs: box Constant { value: RawVal::Int(c), .. }, .. } =>
                lhs.encoding_range().map(|(min, max)| (min + c, max + c)),
            Cast { ref input, .. } => input.encoding_range(),
            LZ4Decode { ref bytes, .. } => bytes.encoding_range(),
            DeltaDecode { ref plan, .. } => plan.encoding_range(),
            _ => None, // TODO(clemens): many more cases where we can determine range
        }
    }
}

fn replace_common_subexpression(plan: QueryPlan, executor: &mut QueryExecutor) -> (Box<QueryPlan>, [u8; 16]) {
    use std::intrinsics::discriminant_value;
    use self::QueryPlan::*;

    unsafe /* dicriminant_value */ {
        let mut signature = [0u8; 16];
        let mut hasher = Md5::new();
        hasher.input(&discriminant_value(&plan).to_ne_bytes());
        let plan = match plan {
            ColumnSection { name, section, range, t } => {
                hasher.input_str(&name);
                hasher.input(&section.to_ne_bytes());
                ColumnSection { name, section, range, t }
            }
            ReadBuffer { buffer } => {
                hasher.input(&buffer.buffer.i.to_ne_bytes());
                ReadBuffer { buffer }
            }
            DictLookup { indices, offset_len, backing_store } => {
                let (indices, s1) = replace_common_subexpression(*indices, executor);
                let (offset_len, s2) = replace_common_subexpression(*offset_len, executor);
                let (backing_store, s3) = replace_common_subexpression(*backing_store, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&s3);
                DictLookup { indices, offset_len, backing_store }
            }
            InverseDictLookup { offset_len, backing_store, constant } => {
                let (offset_len, s1) = replace_common_subexpression(*offset_len, executor);
                let (backing_store, s2) = replace_common_subexpression(*backing_store, executor);
                let (constant, s3) = replace_common_subexpression(*constant, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&s3);
                InverseDictLookup { offset_len, backing_store, constant }
            }
            Cast { input, input_type, target_type } => {
                let (input, s1) = replace_common_subexpression(*input, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&input_type).to_ne_bytes());
                hasher.input(&discriminant_value(&target_type).to_ne_bytes());
                Cast { input, input_type, target_type }
            }
            LZ4Decode { bytes, decoded_len, t } => {
                let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                LZ4Decode { bytes, decoded_len, t }
            }
            UnpackStrings { bytes } => {
                let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                hasher.input(&s1);
                UnpackStrings { bytes }
            }
            UnhexpackStrings { bytes, uppercase, total_bytes } => {
                let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                hasher.input(&s1);
                hasher.input(&total_bytes.to_ne_bytes());
                hasher.input(&[uppercase as u8]);
                UnhexpackStrings { bytes, uppercase, total_bytes }
            }
            DeltaDecode { plan } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                DeltaDecode { plan }
            }
            Exists { indices, max_index } => {
                let (indices, s1) = replace_common_subexpression(*indices, executor);
                let (max_index, s2) = replace_common_subexpression(*max_index, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Exists { indices, max_index }
            }
            NonzeroCompact { plan } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                NonzeroCompact { plan }
            }
            NonzeroIndices { plan, indices_type } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&indices_type).to_ne_bytes());
                NonzeroIndices { plan, indices_type }
            }
            Compact { plan, select } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                let (select, s2) = replace_common_subexpression(*select, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Compact { plan, select }
            }
            EncodeIntConstant { constant, codec } => {
                // TODO(clemens): codec needs to be part of signature (easy once we encode using actual query plan rather than codec)
                let (constant, s1) = replace_common_subexpression(*constant, executor);
                hasher.input(&s1);
                EncodeIntConstant { constant, codec }
            }
            BitPack { lhs, rhs, shift } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&(shift as u64).to_ne_bytes());
                BitPack { lhs, rhs, shift }
            }
            BitUnpack { plan, shift, width } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&shift.to_ne_bytes());
                hasher.input(&width.to_ne_bytes());
                BitUnpack { plan, shift, width }
            }
            SlicePack { plan, stride, offset } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&stride.to_ne_bytes());
                hasher.input(&offset.to_ne_bytes());
                SlicePack { plan, stride, offset }
            }
            SliceUnpack { plan, t, stride, offset } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&stride.to_ne_bytes());
                hasher.input(&offset.to_ne_bytes());
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                SliceUnpack { plan, t, stride, offset }
            }
            Convergence(plans) => {
                let mut new_plans = Vec::new();
                for p in plans {
                    let (p, s) = replace_common_subexpression(*p, executor);
                    new_plans.push(p);
                    hasher.input(&s);
                }
                Convergence(new_plans)
            }
            LessThanVS { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                LessThanVS { lhs, rhs }
            }
            EqualsVS { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                EqualsVS { lhs, rhs }
            }
            NotEqualsVS { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                NotEqualsVS { lhs, rhs }
            }
            DivideVS { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                DivideVS { lhs, rhs }
            }
            AddVS { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                AddVS { lhs, rhs }
            }
            And { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                And { lhs, rhs }
            }
            Or { lhs, rhs } => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Or { lhs, rhs }
            }
            ToYear { timestamp } => {
                let (timestamp, s1) = replace_common_subexpression(*timestamp, executor);
                hasher.input(&s1);
                ToYear { timestamp }
            }
            Indices { plan } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                Indices { plan }
            }
            SortBy { ranking, indices, desc, stable } => {
                let (ranking, s1) = replace_common_subexpression(*ranking, executor);
                let (indices, s2) = replace_common_subexpression(*indices, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&[desc as u8]);
                SortBy { ranking, indices, desc, stable }
            }
            TopN { ranking, n, desc } => {
                let (ranking, s1) = replace_common_subexpression(*ranking, executor);
                hasher.input(&s1);
                hasher.input(&n.to_ne_bytes());
                hasher.input(&[desc as u8]);
                TopN { ranking, n, desc }
            }
            Select { plan, indices } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                let (indices, s2) = replace_common_subexpression(*indices, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Select { plan, indices }
            }
            Filter { plan, select } => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                let (select, s2) = replace_common_subexpression(*select, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Filter { plan, select }
            }
            EncodedGroupByPlaceholder => EncodedGroupByPlaceholder,
            Constant { value, hide_value } => {
                match value {
                    RawVal::Int(i) => hasher.input(&(i as u64).to_ne_bytes()),
                    RawVal::Str(ref s) => hasher.input_str(s),
                    RawVal::Null => {}
                }
                Constant { value, hide_value }
            }
        };

        hasher.result(&mut signature);
        match executor.get(&signature) {
            Some(plan) => (Box::new(plan), signature),
            None => (Box::new(plan), signature),
        }
    }
}

pub fn compile_grouping_key(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<DataSource>>)
    -> Result<(TypedPlan, i64, Vec<TypedPlan>), QueryError> {
    if exprs.len() == 1 {
        QueryPlan::create_query_plan(&exprs[0], filter, columns)
            .map(|(gk_plan, gk_type)| {
                let encoding_range = QueryPlan::encoding_range(&gk_plan);
                debug!("Encoding range of {:?} for {:?}", &encoding_range, &gk_plan);
                let (max_cardinality, offset) = match encoding_range {
                    Some((min, max)) => if min < 0 { (max - min, -min) } else { (max, 0) }
                    None => (1 << 62, 0),
                };
                let gk_plan = if offset != 0 {
                    gk_plan + constant(RawVal::Int(offset), true)
                } else { gk_plan };

                let decoded_group_by = gk_type.codec.clone().map_or(
                    QueryPlan::EncodedGroupByPlaceholder,
                    |codec| codec.decode(QueryPlan::EncodedGroupByPlaceholder));
                let decoded_group_by = if offset == 0 { decoded_group_by } else {
                    syntax::cast(
                        decoded_group_by + constant(RawVal::Int(-offset), true),
                        EncodingType::I64,
                        gk_type.encoding_type())
                };

                ((gk_plan.clone(), gk_type.clone()),
                 max_cardinality,
                 vec![(decoded_group_by, gk_type.decoded())])
            })
    } else if let Some(result) = try_bitpacking(exprs, filter, columns)? {
        Ok(result)
    } else {
        info!("Failed to bitpack grouping key");
        let mut pack = Vec::new();
        let mut decode_plans = Vec::new();
        for (i, expr) in exprs.iter().enumerate() {
            let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            pack.push(Box::new(slice_pack(query_plan, exprs.len(), i)));

            // TODO(clemens): negative integers can throw off sort oder - need to move into positive range
            let mut decode_plan = slice_unpack(
                QueryPlan::EncodedGroupByPlaceholder,
                plan_type.encoding_type(),
                exprs.len(),
                i);
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan);
            }
            decode_plans.push(
                (decode_plan,
                 plan_type.decoded()));
        }
        let t = Type::encoded(Codec::opaque(
            EncodingType::ByteSlices(exprs.len()),
            BasicType::Val,
            false /* is_summation_preserving */,
            true  /* is_order_preserving */,
            false /* is_positive_integer */,
            false /* is_fixed_width */,
        ));
        Ok(((QueryPlan::Convergence(pack), t),
            i64::MAX,
            decode_plans))
    }
}

fn try_bitpacking(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<DataSource>>)
    -> Result<Option<(TypedPlan, i64, Vec<TypedPlan>)>, QueryError> {
    // TODO(clemens): use u64 as grouping key type
    let mut total_width = 0;
    let mut largest_key = 0;
    let mut plan = None;
    let mut decode_plans = Vec::with_capacity(exprs.len());
    let mut order_preserving = true;
    for expr in exprs.iter().rev() {
        let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
        let encoding_range = QueryPlan::encoding_range(&query_plan);
        debug!("Encoding range of {:?} for {:?}", &encoding_range, &query_plan);
        if let Some((min, max)) = encoding_range {
            fn bits(max: i64) -> i64 {
                ((max + 1) as f64).log2().ceil() as i64
            }

            // TODO(clemens): more intelligent criterion. threshold should probably be a function of total width.
            let subtract_offset = bits(max) - bits(max - min) > 1 || min < 0;
            let adjusted_max = if subtract_offset { max - min } else { max };
            order_preserving = order_preserving && plan_type.is_order_preserving();
            let query_plan = if subtract_offset {
                query_plan + constant(RawVal::Int(-min), true)
            } else {
                syntax::cast(query_plan, plan_type.encoding_type(), EncodingType::I64)
            };

            if total_width == 0 {
                plan = Some(query_plan);
            } else if adjusted_max > 0 {
                plan = plan.map(|plan| bit_pack(plan, query_plan, total_width));
            }

            let mut decode_plan = bit_unpack(
                QueryPlan::EncodedGroupByPlaceholder,
                total_width as u8,
                bits(adjusted_max) as u8);
            if subtract_offset {
                decode_plan = decode_plan + constant(RawVal::Int(min), true);
            }
            decode_plan = syntax::cast(decode_plan, EncodingType::I64, plan_type.encoding_type());
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan);
            }
            decode_plans.push((decode_plan, plan_type.decoded()));

            largest_key += adjusted_max << total_width;
            total_width += bits(adjusted_max);
        } else {
            return Ok(None);
        }
    }

    Ok(
        if total_width <= 64 {
            plan.map(|plan| {
                decode_plans.reverse();
                let t = Type::encoded(Codec::opaque(
                    EncodingType::I64, BasicType::Integer, false, order_preserving, true, true));
                ((plan, t), largest_key, decode_plans)
            })
        } else {
            None
        }
    )
}

fn to_hex_string(bytes: &[u8]) -> String {
    bytes.iter()
        .map(|b| format!("{:02X}", b))
        .join("")
}

impl From<TypedBufferRef> for QueryPlan {
    fn from(buffer: TypedBufferRef) -> QueryPlan { read_buffer(buffer) }
}

impl Add for QueryPlan {
    type Output = QueryPlan;

    fn add(self, other: QueryPlan) -> QueryPlan {
        add_vs(self, other)
    }
}

impl BitOr for QueryPlan {
    type Output = QueryPlan;

    fn bitor(self, other: QueryPlan) -> QueryPlan {
        or(self, other)
    }
}

impl BitAnd for QueryPlan {
    type Output = QueryPlan;

    fn bitand(self, other: QueryPlan) -> QueryPlan {
        and(self, other)
    }
}

impl Div for QueryPlan {
    type Output = QueryPlan;

    fn div(self, other: QueryPlan) -> QueryPlan {
        divide_vs(self, other)
    }
}
