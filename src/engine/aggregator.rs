#[derive(Debug, Clone, Copy)]
pub enum Aggregator {
    Sum,
    Count,
}

impl Aggregator {
    pub fn combine_i64(&self, accumulator: i64, elem: i64) -> i64 {
        accumulator + elem
    }
}
