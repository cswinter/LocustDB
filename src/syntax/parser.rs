extern crate sqlparser;

use sqlparser::sqlparser::*;
use sqlparser::sqlast::*;
use engine::query::Query;
use syntax::expression::*;
use engine::aggregator::*;
use ingest::raw_val::RawVal;
use syntax::limit::*;
use sqlparser::dialect::GenericSqlDialect;
use QueryError;

// Convert sqlparser-rs `ASTNode` to LocustDB's `Query`
pub fn parse_query(query: &str) -> Result<Query, QueryError> {
    let dialect = GenericSqlDialect {};
    let ast = Parser::parse_sql(&dialect, query.to_string()).map_err(
        |e| if let ParserError::ParserError(e_str) = e {
                QueryError::ParseError(e_str)
            } else {
                QueryError::FatalError(format!("{:?}", e))
            }
    )?;

    let (projection, relation, selection, order_by, limit) = get_query_components(ast)?;

    let (select, aggregate) = get_select_aggregate(projection)?;

    let table = get_table_name(relation)?;

    let filter = get_filter(selection)?;

    let (order_by_str, order_desc) = get_order_by(order_by)?;

    let limit_clause = LimitClause { limit: get_limit(limit)?, offset: 0 };

    Ok(Query {
        select,
        table,
        filter,
        aggregate,
        order_by: order_by_str,
        order_desc,
        limit: limit_clause,
        order_by_index: None,
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
    if let ASTNode::SQLSelect
        {projection, relation, selection, order_by, group_by, having, limit} = ast
    {
        if group_by.is_some() {
            return Err(QueryError::NotImplemented(format!("Group By")))
        } else if having.is_some() {
            return Err(QueryError::NotImplemented(format!("Having")))
        }

        Ok((projection, relation, selection, order_by, limit))
    } else {
        Err(QueryError::NotImplemented(format!("{:?}", ast)))
    }
}

fn get_select_aggregate(projection: Vec<ASTNode>) -> Result<(Vec<Expr>, Vec<(Aggregator, Expr)>), QueryError> {
    let mut select = Vec::<Expr>::new();
    let mut aggregate = Vec::<(Aggregator, Expr)>::new();
    for elem in projection {
        match elem {
            ASTNode::SQLIdentifier(expr) =>
                select.push(Expr::ColName(expr)),
            ASTNode::SQLValue(ref constant) =>
                select.push(Expr::Const(get_raw_val(constant)?)),
            ASTNode::SQLWildcard =>
                select.push(Expr::ColName('*'.to_string())),
            ASTNode::SQLBinaryExpr{ref left, ref op, ref right} => {
                select.push(generate_expression(left, op, right)?);
            },
            ASTNode::SQLFunction{id, args} => {
                let expr = match args[0] {
                    ASTNode::SQLValue(ref literal) =>
                        Expr::Const(get_raw_val(literal)?),
                    ASTNode::SQLIdentifier(ref identifier) =>
                        Expr::ColName(identifier.to_string()),
                    ASTNode::SQLBinaryExpr{ref left, ref op, ref right} =>
                        generate_expression(left, op, right)?,
                    _ => Expr::Const(RawVal::Int(1))
                };

                match id.to_uppercase().as_ref() {
                    "COUNT" => {
                        aggregate.push((Aggregator::Count, expr));
                    },
                    "SUM" => {
                        aggregate.push((Aggregator::Sum, expr));
                    },
                    "TO_YEAR" => {
                        select.push(Expr::Func1(Func1Type::ToYear, Box::new(expr)));
                    },
                    _ => return Err(QueryError::NotImplemented(format!("{:?}", id))),
                };
            },

            _ => return Err(QueryError::NotImplemented(format!("{:?}", elem))),
        }
    }

    Ok((select, aggregate))
}

fn get_table_name(relation: Option<Box<ASTNode>>) -> Result<String, QueryError> {
    match relation {
        Some(sql_identifier) => if let ASTNode::SQLIdentifier(table_name) = *sql_identifier {
            Ok(table_name)
        } else {
            Err(QueryError::FatalError(format!("{:?}", *sql_identifier)))
        },
        None => Err(QueryError::ParseError(format!("Table name missing {:?}", relation)))
    }
}

fn get_filter(selection: Option<Box<ASTNode>>) -> Result<Expr, QueryError> {
    match selection {
        Some(binary_expr) => if let ASTNode::SQLBinaryExpr{ref left, ref op, ref right} = *binary_expr {
            generate_expression(left, op, right)
        } else {
            Err(QueryError::FatalError(format!("{:?}", *binary_expr)))
        },
        None => Ok(Expr::Const(RawVal::Int(1)))
    }
}

fn get_order_by(order_by: Option<Vec<SQLOrderByExpr>>) -> Result<(Option<String>, bool), QueryError> {
    match order_by {
        Some(sql_order_by_exprs) => {
            // Remove when `QueryTask` supports multiple columns in `order_by`
            if sql_order_by_exprs.len() > 1 {
                return Err(QueryError::NotImplemented(format!("Mutliple columns in order by")));
            }
            if let ASTNode::SQLIdentifier(ref identifier) = *sql_order_by_exprs[0].expr {
                Ok((Some(identifier.to_string()), !sql_order_by_exprs[0].asc))
            } else {
                Err(QueryError::NotImplemented(format!("{:?}", sql_order_by_exprs)))
            }
        },
        None => Ok((None, false)),
    }
}

fn get_limit(limit: Option<Box<ASTNode>>) -> Result<u64, QueryError> {
    match limit {
        Some(sql_limit) => if let ASTNode::SQLValue(literal) = *sql_limit {
            if let Value::Long(int) = literal {
                Ok(int as u64)
            } else {
                Err(QueryError::NotImplemented(format!("{:?}", literal)))
            }
        } else {
            Err(QueryError::NotImplemented(format!("{:?}", sql_limit)))
        },
        None => Ok(100),
    }
}

/// Fn to convert `SQLBinaryExpr` to LocustDB's `Expr`.
fn generate_expression(l: &ASTNode, o: &SQLOperator, r: &ASTNode) -> Result<Expr, QueryError> {
    let operator = match o {
        SQLOperator::And => Func2Type::And,
        SQLOperator::Plus => Func2Type::Add,
        SQLOperator::Minus => Func2Type::Subtract,
        SQLOperator::Multiply => Func2Type::Multiply,
        SQLOperator::Divide => Func2Type::Divide,
        SQLOperator::Gt => Func2Type::GT,
        SQLOperator::Lt => Func2Type::LT,
        SQLOperator::Eq => Func2Type::Equals,
        SQLOperator::NotEq => Func2Type::NotEquals,
        SQLOperator::Or => Func2Type::Or,
        _ => return Err(QueryError::NotImplemented(format!("Operator {:?}", o))),
    };

    let lhs = match l {
        ASTNode::SQLValue(ref literal) => Box::new(Expr::Const(get_raw_val(literal)?)),
        ASTNode::SQLIdentifier(ref identifier) => Box::new(Expr::ColName(identifier.to_string())),
        ASTNode::SQLBinaryExpr{ref left, ref op, ref right} => Box::new(generate_expression(left, op, right)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", *l)))
    };

    let rhs = match r {
        ASTNode::SQLValue(ref literal) => Box::new(Expr::Const(get_raw_val(literal)?)),
        ASTNode::SQLIdentifier(ref identifier) => Box::new(Expr::ColName(identifier.to_string())),   
        ASTNode::SQLBinaryExpr{ref left, ref op, ref right} => Box::new(generate_expression(left, op, right)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", *r)))
    };

    Ok(Expr::Func2(operator, lhs, rhs))
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
            "Ok(Query { select: [ColName(\"*\")], table: \"default\", filter: Const(Int(1)), aggregate: [], order_by: None, order_desc: false, limit: LimitClause { limit: 100, offset: 0 }, order_by_index: None })");
    }

    #[test]
    fn test_to_year() {
        assert_eq!(
            format!("{:?}", parse_query("select to_year(ts) from default")),
            "Ok(Query { select: [Func1(ToYear, ColName(\"ts\"))], table: \"default\", filter: Const(Int(1)), aggregate: [], order_by: None, order_desc: false, limit: LimitClause { limit: 100, offset: 0 }, order_by_index: None })");
    }
}
