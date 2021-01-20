use crate::engine::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregator {
    Sum = 0,
    Count = 1,
    Max = 2,
    Min = 3,
}

impl Aggregator {
    pub fn combine_i64(self, accumulator: i64, elem: i64) -> Result<i64, QueryError> {
        match self {
            Aggregator::Sum => accumulator.checked_add(elem).ok_or(QueryError::Overflow),
            Aggregator::Count => Ok(accumulator + elem),
            Aggregator::Max => Ok(std::cmp::max(accumulator, elem)),
            Aggregator::Min => Ok(std::cmp::min(accumulator, elem)),
        }
    }

    pub fn get_string(self, expr: String) -> String {
        match self {
            Aggregator::Sum => format!("Sum({})", expr),
            Aggregator::Count => format!("Count({})", expr),
            Aggregator::Max => format!("Max({})", expr),
            Aggregator::Min => format!("Min({})", expr),
        }
    }
}
