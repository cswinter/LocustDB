extern crate sqlparser;

use sqlparser::sqlparser::*;
use sqlparser::sqlast::*;
use engine::*;
use syntax::expression::*;
use ingest::raw_val::RawVal;
use syntax::limit::*;
use sqlparser::dialect::GenericSqlDialect;
use QueryError;

// Convert sqlparser-rs `ASTNode` to LocustDB's `Query`
pub fn parse_query(query: &str) -> Result<Query, QueryError> {
    let dialect = GenericSqlDialect {};
    let ast = Parser::parse_sql(&dialect, query.to_string())
        .map_err(|e| match e {
            ParserError::ParserError(e_str) => QueryError::ParseError(e_str),
            _ => fatal!("{:?}", e),
        })?;

    let (projection, relation, selection, order_by, limit) = get_query_components(ast)?;
    let projection = get_projection(projection)?;
    let table = get_table_name(relation)?;
    let filter = match selection {
        Some(ref s) => *expr(s)?,
        None => Expr::Const(RawVal::Int(1)),
    };
    let order_by = get_order_by(order_by)?;
    let limit_clause = LimitClause { limit: get_limit(limit)?, offset: 0 };

    Ok(Query {
        select: projection,
        table,
        filter,
        order_by,
        limit: limit_clause,
    })
}

fn get_query_components(ast: ASTNode)
                        -> Result<(
                            Vec<ASTNode>,
                            Option<Box<ASTNode>>,
                            Option<Box<ASTNode>>,
                            Option<Vec<SQLOrderByExpr>>,
                            Option<Box<ASTNode>>),
                            QueryError>
{
    match ast {
        ASTNode::SQLSelect { projection, relation, selection, order_by, group_by, having, limit } => {
            if group_by.is_some() {
                Err(QueryError::NotImplemented(format!("Group By")))
            } else if having.is_some() {
                Err(QueryError::NotImplemented(format!("Having")))
            } else {
                Ok((projection, relation, selection, order_by, limit))
            }
        }
        _ => Err(QueryError::NotImplemented(format!("{:?}", ast))),
    }
}

fn get_projection(projection: Vec<ASTNode>) -> Result<Vec<Expr>, QueryError> {
    let mut result = Vec::<Expr>::new();
    for elem in &projection {
        match elem {
            ASTNode::SQLWildcard => result.push(Expr::ColName('*'.to_string())),
            _ => result.push(*expr(elem)?),
        }
    }

    Ok(result)
}

fn get_table_name(relation: Option<Box<ASTNode>>) -> Result<String, QueryError> {
    match relation {
        Some(box ASTNode::SQLIdentifier(table_name)) => Ok(table_name),
        Some(s) => Err(QueryError::ParseError(format!("Invalid expression for table name: {:?}", s))),
        None => Err(QueryError::ParseError("Table name missing.".to_string())),
    }
}

fn get_order_by(order_by: Option<Vec<SQLOrderByExpr>>) -> Result<Vec<(Expr, bool)>, QueryError> {
    let mut order = Vec::new();
    if let Some(sql_order_by_exprs) = order_by {
        for e in sql_order_by_exprs {
            order.push((*(expr(&e.expr))?, !e.asc));
        }
    }
    Ok(order)
}

fn get_limit(limit: Option<Box<ASTNode>>) -> Result<u64, QueryError> {
    match limit {
        Some(box ASTNode::SQLValue(Value::Long(int))) => Ok(int as u64),
        None => Ok(100),
        _ => Err(QueryError::NotImplemented(format!("Invalid expression in limit clause: {:?}", limit))),
    }
}

fn expr(node: &ASTNode) -> Result<Box<Expr>, QueryError> {
    Ok(Box::new(match node {
        ASTNode::SQLBinaryExpr { ref left, ref op, ref right } =>
            Expr::Func2(map_operator(op)?, expr(left)?, expr(right)?),
        ASTNode::SQLValue(ref literal) => Expr::Const(get_raw_val(literal)?),
        ASTNode::SQLIdentifier(ref identifier) => Expr::ColName(identifier.to_string()),
        ASTNode::SQLFunction { id, args } => match id.to_uppercase().as_ref() {
            "TO_YEAR" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in TO_YEAR function".to_string()));
                }
                Expr::Func1(Func1Type::ToYear, expr(&args[0])?)
            }
            "REGEX" => {
                if args.len() != 2 {
                    return Err(QueryError::ParseError(
                        "Expected two arguments in regex function".to_string()));
                }
                Expr::Func2(Func2Type::RegexMatch, expr(&args[0])?, expr(&args[1])?)
            }
            "COUNT" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in COUNT function".to_string()));
                }
                Expr::Aggregate(Aggregator::Count, expr(&args[0])?)
            }
            "SUM" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in SUM function".to_string()));
                }
                Expr::Aggregate(Aggregator::Sum, expr(&args[0])?)
            }
            "AVG" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in AVG function".to_string()));
                }
                Expr::Func2(Func2Type::Divide,
                            Box::new(Expr::Aggregate(Aggregator::Sum, expr(&args[0])?)),
                            Box::new(Expr::Aggregate(Aggregator::Count, expr(&args[0])?)))
            }
            "MAX" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MAX function".to_string()));
                }
                Expr::Aggregate(Aggregator::Max, expr(&args[0])?)
            }
            "MIN" => {
                if args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MIN function".to_string()));
                }
                Expr::Aggregate(Aggregator::Min, expr(&args[0])?)
            }
            _ => return Err(QueryError::NotImplemented(format!("Function {:?}", id))),
        }
        ASTNode::SQLIsNull(ref node) => Expr::Func1(Func1Type::IsNull, expr(node)?),
        ASTNode::SQLIsNotNull(ref node) => Expr::Func1(Func1Type::IsNotNull, expr(node)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", node))),
    }))
}

fn map_operator(o: &SQLOperator) -> Result<Func2Type, QueryError> {
    Ok(match o {
        SQLOperator::And => Func2Type::And,
        SQLOperator::Plus => Func2Type::Add,
        SQLOperator::Minus => Func2Type::Subtract,
        SQLOperator::Multiply => Func2Type::Multiply,
        SQLOperator::Divide => Func2Type::Divide,
        SQLOperator::Modulus => Func2Type::Modulo,
        SQLOperator::Gt => Func2Type::GT,
        SQLOperator::GtEq => Func2Type::GTE,
        SQLOperator::Lt => Func2Type::LT,
        SQLOperator::LtEq => Func2Type::LTE,
        SQLOperator::Eq => Func2Type::Equals,
        SQLOperator::NotEq => Func2Type::NotEquals,
        SQLOperator::Or => Func2Type::Or,
        SQLOperator::Like => Func2Type::Like,
    })
}


// Fn to map sqlparser-rs `Value` to LocustDB's `RawVal`.
fn get_raw_val(constant: &Value) -> Result<RawVal, QueryError> {
    match constant {
        Value::Long(int) => Ok(RawVal::Int(*int)),
        Value::String(string)
        | Value::SingleQuotedString(string)
        | Value::DoubleQuotedString(string) => Ok(RawVal::Str(string.to_string())),
        Value::Null => Ok(RawVal::Null),
        _ => {
            return Err(QueryError::NotImplemented(format!("{:?}", constant)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star() {
        assert_eq!(
            format!("{:?}", parse_query("select * from default")),
            "Ok(Query { select: [ColName(\"*\")], table: \"default\", filter: Const(Int(1)), order_by: [], limit: LimitClause { limit: 100, offset: 0 } })");
    }

    #[test]
    fn test_to_year() {
        assert_eq!(
            format!("{:?}", parse_query("select to_year(ts) from default")),
            "Ok(Query { select: [Func1(ToYear, ColName(\"ts\"))], table: \"default\", filter: Const(Int(1)), order_by: [], limit: LimitClause { limit: 100, offset: 0 } })");
    }
}
