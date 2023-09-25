// TODO: would probably be better to have two types here, an UntypedAggregator emitted by parser which is then converted into the right TypedAggregator by query planner
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregator {
    SumI64 = 0,
    SumF64 = 1,
    Count = 2,
    MaxI64 = 3,
    MaxF64 = 4,
    MinI64 = 5,
    MinF64 = 6,
}