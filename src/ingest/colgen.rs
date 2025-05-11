use std::collections::HashMap;

use crate::ingest::alias_method_fork::*;
use crate::ingest::raw_val::RawVal;
use locustdb_serialization::event_buffer::{ColumnBuffer, ColumnData, EventBuffer, TableBuffer};
use rand::distr::{Alphanumeric, StandardUniform};
use rand::Rng;
use rand::SeedableRng;

use crate::scheduler::inner_locustdb::InnerLocustDB;

pub trait ColumnGenerator: Sync + Send {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal>;
}

pub fn int_markov_chain(
    elements: Vec<i64>,
    transition_probabilities: Vec<Vec<f64>>,
) -> Box<dyn ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
    })
}

pub fn int_uniform(low: i64, high: i64) -> Box<dyn ColumnGenerator> {
    Box::new(UniformInteger { low, high })
}

pub fn splayed(offset: i64, coefficient: i64) -> Box<dyn ColumnGenerator> {
    Box::new(Splayed {
        offset,
        coefficient,
    })
}

pub fn int_weighted(values: Vec<i64>, weights: Vec<f64>) -> Box<dyn ColumnGenerator> {
    Box::new(Weighted {
        elem: values,
        weights,
    })
}

pub fn nullable_ints(values: Vec<Option<i64>>, weights: Vec<f64>) -> Box<dyn ColumnGenerator> {
    Box::new(Weighted {
        elem: values,
        weights,
    })
}

pub fn incrementing_int() -> Box<dyn ColumnGenerator> {
    Box::new(IncrementingInteger)
}

pub fn string_markov_chain(
    elements: Vec<String>,
    transition_probabilities: Vec<Vec<f64>>,
) -> Box<dyn ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
    })
}

pub fn string_weighted(values: Vec<String>, weights: Vec<f64>) -> Box<dyn ColumnGenerator> {
    Box::new(Weighted {
        elem: values,
        weights,
    })
}

pub fn random_hex_string(length: usize) -> Box<dyn ColumnGenerator> {
    Box::new(HexString { length })
}

pub fn random_string(min_length: usize, max_length: usize) -> Box<dyn ColumnGenerator> {
    Box::new(RandomString {
        min_length,
        max_length,
    })
}

pub fn partition_sparse(
    null_probability: f64,
    generator: Box<dyn ColumnGenerator>,
) -> Box<dyn ColumnGenerator> {
    Box::new(PartitionSparse {
        null_probability,
        generator,
    })
}

#[derive(Clone)]
struct MarkovChain<T> {
    elem: Vec<T>,
    p_transition: Vec<Vec<f64>>,
}

impl<T: Sync + Send> ColumnGenerator for MarkovChain<T>
where
    T: Clone + Into<RawVal>,
{
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut rng = seeded_rng(seed);
        let mut builder = Vec::new();
        let mut state = rng.random_range(0..self.elem.len());
        let p = self
            .p_transition
            .iter()
            .map(|p| new_alias_table(p).unwrap())
            .collect::<Vec<_>>();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            state = alias_method.random(&p[state]);
            builder.push(self.elem[state].clone().into());
        }
        builder
    }
}

#[derive(Clone)]
struct Weighted<T> {
    elem: Vec<T>,
    weights: Vec<f64>,
}

impl<T: Sync + Send> ColumnGenerator for Weighted<T>
where
    T: Clone + Into<RawVal>,
{
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let rng = seeded_rng(seed);
        let mut builder = Vec::new();
        let p = new_alias_table(&self.weights).unwrap();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            let i = alias_method.random(&p);
            builder.push(self.elem[i].clone().into());
        }
        builder
    }
}

struct UniformInteger {
    low: i64,
    high: i64,
}

impl ColumnGenerator for UniformInteger {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut rng = seeded_rng(seed);
        let mut builder = Vec::new();
        for _ in 0..length {
            builder.push(rng.random_range(self.low..self.high).into());
        }
        builder
    }
}

struct Splayed {
    offset: i64,
    coefficient: i64,
}

impl ColumnGenerator for Splayed {
    fn generate(&self, length: usize, partition: u64) -> Vec<RawVal> {
        let mut rng = seeded_rng(partition);
        let mut builder = Vec::new();
        for _ in 0..length {
            let from = self.offset + self.coefficient * length as i64 * partition as i64;
            let to = from + self.coefficient * length as i64;
            builder.push(rng.random_range(from..to).into());
        }
        builder
    }
}

struct PartitionSparse {
    null_probability: f64,
    generator: Box<dyn ColumnGenerator>,
}

impl ColumnGenerator for PartitionSparse {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut rng = seeded_rng(seed);
        if rng.random::<f64>() < self.null_probability {
            vec![RawVal::Null; length]
        } else {
            self.generator.generate(length, seed)
        }
    }
}

struct HexString {
    length: usize,
}

impl ColumnGenerator for HexString {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut builder = Vec::new();
        for i in 0..length {
            let rng = seeded_rng(seed + i as u64);
            let bytes: Vec<u8> = rng
                .sample_iter(&StandardUniform)
                .take(self.length)
                .collect();
            builder.push(hex::encode(&bytes).into());
        }
        builder
    }
}

struct RandomString {
    min_length: usize,
    max_length: usize,
}

impl ColumnGenerator for RandomString {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut builder = Vec::new();
        for i in 0..length {
            let mut rng = seeded_rng(seed + i as u64);
            let len = rng.random_range(self.min_length..self.max_length + 1);
            let string: String = rng
                .sample_iter(&Alphanumeric)
                .map(|c| c as char)
                .take(len)
                .collect();
            builder.push(string.into());
        }
        builder
    }
}

struct IncrementingInteger;

impl ColumnGenerator for IncrementingInteger {
    fn generate(&self, length: usize, seed: u64) -> Vec<RawVal> {
        let mut builder = Vec::new();
        for i in seed as i64 * length as i64..length as i64 * (seed as i64 + 1) {
            builder.push(i.into());
        }
        builder
    }
}

pub struct GenTable {
    pub name: String,
    pub partitions: usize,
    pub partition_size: usize,
    pub columns: Vec<(String, Box<dyn ColumnGenerator>)>,
}

impl GenTable {
    pub fn gen(&self, db: &InnerLocustDB, partition_number: u64) {
        let cols = self
            .columns
            .iter()
            .map(|(name, c)| {
                (
                    name.to_string(),
                    c.generate(self.partition_size, partition_number),
                )
            })
            .collect();
        let event_buffer = event_buffer_from_raw_vals(&self.name, cols);
        db.ingest_efficient(event_buffer);
    }
}

fn seeded_rng(seed: u64) -> rand_xorshift::XorShiftRng {
    rand_xorshift::XorShiftRng::seed_from_u64(seed)
}

pub fn event_buffer_from_raw_vals(
    table: &str,
    columns: HashMap<String, Vec<RawVal>>,
) -> EventBuffer {
    let mut event_buffer = EventBuffer::default();
    let mut table_buffer = TableBuffer::default();
    for (colname, values) in columns {
        table_buffer.columns.insert(
            colname,
            ColumnBuffer {
                data: ColumnData::Mixed(values.into_iter().map(RawVal::into).collect()),
            },
        );
    }
    event_buffer.tables.insert(table.to_string(), table_buffer);
    event_buffer
}
