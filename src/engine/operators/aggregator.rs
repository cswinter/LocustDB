#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregator {
    Sum = 0,
    Count = 1,
    Max = 2,
    Min = 3,
}

impl Aggregator {
    pub fn combine_i64(self, accumulator: i64, elem: i64) -> i64 {
        match self {
            Aggregator::Sum | Aggregator::Count => accumulator + elem,
            Aggregator::Max => std::cmp::max(accumulator, elem),
            Aggregator::Min => std::cmp::min(accumulator, elem),
        }
    }
}
