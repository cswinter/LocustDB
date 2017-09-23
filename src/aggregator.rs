use value::Val;


#[derive(Debug, Clone, Copy)]
pub enum Aggregator {
    Sum,
    Count,
}

impl Aggregator {
    pub fn zero<'a>(self) -> Val<'a> {
        match self {
            Aggregator::Sum | Aggregator::Count => Val::Integer(0),
        }
    }

    pub fn reduce<'a>(&self, accumulator: &Val, elem: &Val) -> Val<'a> {
        match (self, accumulator, elem) {
            (&Aggregator::Sum, &Val::Integer(i1), &Val::Integer(i2)) => {
                Val::Integer(i1 + i2)
            }
            (&Aggregator::Sum, &Val::Integer(i1), &Val::Null) => Val::Integer(i1),
            (&Aggregator::Count, &Val::Integer(i), &Val::Null) => Val::Integer(i),
            (&Aggregator::Count, &Val::Integer(i1), _) => Val::Integer(i1 + 1),
            (&aggregator, accumulator, elem) => {
                panic!("Type error: aggregator {:?} not defined for values {:?} and {:?}",
                       aggregator,
                       *accumulator,
                       *elem)
            }
        }
    }

    pub fn combine<'a>(&self, accumulator: &Val, elem: &Val) -> Val<'a> {
        match (self, accumulator, elem) {
            (_, &Val::Integer(i1), &Val::Integer(i2)) => Val::Integer(i1 + i2),
            (&aggregator, accumulator, elem) => {
                panic!("Type error: aggregator.combine {:?} not defined for values {:?} and {:?}",
                       aggregator,
                       *accumulator,
                       *elem)
            }
        }
    }
}
