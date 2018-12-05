#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregator {
    Sum = 0,
    Count = 1,
}

impl Aggregator {
    pub fn combine_i64(self, accumulator: i64, elem: i64) -> i64 {
        accumulator + elem
    }
}
