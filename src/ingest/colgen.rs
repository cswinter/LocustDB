use std::marker::PhantomData;
use std::sync::Arc;

use crypto::digest::Digest;
use crypto::md5::Md5;
use ingest::alias_method_fork::*;
use rand::Rng;
use rand::SeedableRng;
use rand;

use mem_store::column::*;
use mem_store::column_builder::{ColumnBuilder, IntColBuilder, StringColBuilder};
use mem_store::lru::LRU;
use mem_store::partition::Partition;
use mem_store::table::Table;

pub trait ColumnGenerator: Sync + Send {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column>;
}

pub fn int_markov_chain(
    elements: Vec<i64>,
    transition_probabilities: Vec<Vec<f64>>) -> Box<ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
        s: PhantomData::<IntColBuilder>,
    })
}

pub fn string_markov_chain(
    elements: Vec<String>,
    transition_probabilities: Vec<Vec<f64>>) -> Box<ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
        s: PhantomData::<StringColBuilder>,
    })
}

#[derive(Clone)]
struct MarkovChain<T, S> {
    elem: Vec<T>,
    p_transition: Vec<Vec<f64>>,
    s: PhantomData<S>,
}

unsafe impl<T: Sync + Send, S: ColumnBuilder<T>> Send for MarkovChain<T, S> {}

unsafe impl<T: Sync + Send, S: ColumnBuilder<T>> Sync for MarkovChain<T, S> {}

impl<T: Sync + Send, S: ColumnBuilder<T>> ColumnGenerator for MarkovChain<T, S> {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut seed_bytes = [0u8; 16];
        let mut hasher = Md5::new();
        hasher.input(&seed.to_ne_bytes());
        hasher.result(&mut seed_bytes);
        let mut rng = rand::XorShiftRng::from_seed(seed_bytes);
        let mut builder = S::default();
        let mut state = rng.gen_range(0, self.elem.len());
        let p = self.p_transition.iter()
            .map(|p| new_alias_table(p).unwrap())
            .collect::<Vec<_>>();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            state = alias_method.random(&p[state]);
            builder.push(&self.elem[state]);
        }
        builder.finalize(name)
    }
}

pub struct GenTable {
    pub name: String,
    pub partitions: usize,
    pub partition_size: usize,
    pub columns: Vec<(String, Box<ColumnGenerator>)>,
}

impl GenTable {
    pub fn gen(&self, lru: &LRU, seed: u64) -> Table {
        let table = Table::new(self.partition_size, &self.name, lru.clone());
        for p in 0..self.partitions {
            let mut partition = self.columns
                .iter()
                .map(|(name, c)| c.generate(self.partition_size, &name, seed + p as u64))
                .collect();
            table.load_partition(
                Partition::new(
                    p as u64,
                    partition,
                    lru.clone(),
                ).0);
        }
        table
    }
}
