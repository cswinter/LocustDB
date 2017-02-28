use value::ValueType;


#[derive(Debug, Clone, Copy)]
pub enum Aggregator {
    Sum,
    Count,
}

impl Aggregator {
    pub fn zero<'a>(self) -> ValueType<'a> {
        match self {
            Aggregator::Sum | Aggregator::Count => ValueType::Integer(0),
        }
    }

    pub fn reduce<'a>(&self, accumulator: &ValueType, elem: &ValueType) -> ValueType<'a> {
        match (self, accumulator, elem) {
            (&Aggregator::Sum, &ValueType::Integer(i1), &ValueType::Integer(i2)) => {
                ValueType::Integer(i1 + i2)
            }
            (&Aggregator::Sum, &ValueType::Integer(i1), &ValueType::Null) => ValueType::Integer(i1),
            (&Aggregator::Count, &ValueType::Integer(i), &ValueType::Null) => ValueType::Integer(i),
            (&Aggregator::Count, &ValueType::Integer(i1), _) => ValueType::Integer(i1 + 1),
            (&aggregator, accumulator, elem) => {
                panic!("Type error: aggregator {:?} not defined for values {:?} and {:?}",
                       aggregator,
                       *accumulator,
                       *elem)
            }
        }
    }

    pub fn combine<'a>(&self, accumulator: &ValueType, elem: &ValueType) -> ValueType<'a> {
        match (self, accumulator, elem) {
            (_, &ValueType::Integer(i1), &ValueType::Integer(i2)) => ValueType::Integer(i1 + i2),
            (&aggregator, accumulator, elem) => {
                panic!("Type error: aggregator.combine {:?} not defined for values {:?} and {:?}",
                       aggregator,
                       *accumulator,
                       *elem)
            }
        }
    }
}
