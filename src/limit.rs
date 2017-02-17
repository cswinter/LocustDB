#[derive(Clone, Debug, Hash, PartialEq)]
pub struct LimitClause {
    pub limit: u64,
    pub offset: u64,
}

