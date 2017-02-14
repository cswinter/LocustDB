use value::ValueType;


#[derive(Debug, Clone, Copy)]
pub enum Aggregator {
    Sum,
    Count,
}

impl Aggregator {
    pub fn zero(self) -> ValueType {
        match self {
            Aggregator::Sum | Aggregator::Count => ValueType::Integer(0),
        }
    }

    pub fn reduce(self, accumulator: &ValueType, elem: &ValueType) -> ValueType {
        match (self, accumulator, elem) {
            (Aggregator::Sum, &ValueType::Integer(i1), &ValueType::Integer(i2)) => ValueType::Integer(i1 + i2),
            (Aggregator::Count, accumulator, &ValueType::Null) => accumulator.clone(),
            (Aggregator::Count, &ValueType::Integer(i1), _) => ValueType::Integer(i1 + 1),
            (aggregator, accumulator, elem) => panic!("Type error: aggregator {:?} not defined for values {:?} and {:?}", aggregator, *accumulator, *elem),
        }
    }
}