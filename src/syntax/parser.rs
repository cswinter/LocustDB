extern crate sqlparser;

use crate::engine::Query;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::syntax::expression::Expr;
use crate::syntax::expression::*;
use crate::syntax::limit::*;
use crate::QueryError;
use sqlparser::ast::{Expr as ASTNode, *};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

// Convert sqlparser-rs `ASTNode` to LocustDB's `Query`
pub fn parse_query(query: &str) -> Result<Query, QueryError> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, query).map_err(|e| match e {
        ParserError::ParserError(e_str) => QueryError::ParseError(e_str),
        _ => fatal!("{:?}", e),
    })?;
    if ast.len() > 1 {
        return Err(QueryError::ParseError(format!(
            "Expected a single query statement, but there are {}",
            ast.len()
        )));
    }

    let query = match ast.pop().unwrap() {
        Statement::Query(query) => query,
        _ => {
            return Err(QueryError::ParseError(
                "Only SELECT queries are supported.".to_string(),
            ))
        }
    };

    let (projection, relation, selection, order_by, limit, offset) = get_query_components(query)?;
    let projection = get_projection(projection)?;
    let table = get_table_name(relation)?;
    let filter = match selection {
        Some(ref s) => *convert_to_expr(s)?,
        None => Expr::Const(RawVal::Int(1)),
    };
    let order_by = get_order_by(order_by)?;
    let limit_clause = LimitClause {
        limit: get_limit(limit)?,
        offset: get_offset(offset)?,
    };

    Ok(Query {
        select: projection,
        table,
        filter,
        order_by,
        limit: limit_clause,
    })
}

// TODO: use struct
#[allow(clippy::type_complexity)]
fn get_query_components(
    query: Box<sqlparser::ast::Query>,
) -> Result<
    (
        Vec<SelectItem>,
        Option<TableFactor>,
        Option<ASTNode>,
        Option<Vec<OrderByExpr>>,
        Option<ASTNode>,
        Option<Offset>,
    ),
    QueryError,
> {
    // TODO: return error if any unsupported query parts are present
    let sqlparser::ast::Query {
        body,
        order_by,
        limit,
        offset,
        ..
    } = *query;
    match body {
        SetExpr::Select(box Select {
            distinct,
            projection,
            mut from,
            selection,
            group_by,
            having,
            // TODO: ensure top is not set
            top: _,
        }) => {
            if !group_by.is_empty() {
                Err(QueryError::NotImplemented("Group By  (Hint: If your SELECT clause contains any aggregation expressions, results will implicitly grouped by all other expresssions.)".to_string()))
            } else if having.is_some() {
                Err(QueryError::NotImplemented("Having".to_string()))
            } else if distinct {
                Err(QueryError::NotImplemented("DISTINCT".to_string()))
            } else if from.len() > 1 {
                Err(QueryError::NotImplemented(
                    "Selecting from multiple tables.".to_string(),
                ))
            } else if !from.is_empty() && !from[0].joins.is_empty() {
                Err(QueryError::NotImplemented("JOIN".to_string()))
            } else {
                Ok((
                    projection,
                    from.pop().map(|t| t.relation),
                    selection,
                    if order_by.is_empty() {
                        None
                    } else {
                        Some(order_by)
                    },
                    limit,
                    offset,
                ))
            }
        }
        // TODO: more specific error messages
        _ => Err(QueryError::NotImplemented(
            "Only SELECT queries are supported.".to_string(),
        )),
    }
}

fn get_projection(projection: Vec<SelectItem>) -> Result<Vec<(Expr, Option<String>)>, QueryError> {
    let mut result = Vec::<(Expr, Option<String>)>::new();
    for elem in &projection {
        match elem {
            SelectItem::UnnamedExpr(e) => result.push( (*convert_to_expr(&e)?, None) ),
            SelectItem::Wildcard => result.push( (Expr::ColName('*'.to_string()), None) ),
            SelectItem::ExprWithAlias { expr, alias } => result.push( (*convert_to_expr(&expr)?, Some(alias.to_string())) ),
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "Unsupported projection in SELECT: {}",
                    elem
                )))
            }
        }
    }

    Ok(result)
}

fn get_table_name(relation: Option<TableFactor>) -> Result<String, QueryError> {
    match relation {
        // TODO: error message if any unused fields are set
        Some(TableFactor::Table { name, .. }) => Ok(format!("{}", name)),
        Some(s) => Err(QueryError::ParseError(format!(
            "Invalid expression for table name: {:?}",
            s
        ))),
        None => Err(QueryError::ParseError("Table name missing.".to_string())),
    }
}

fn get_order_by(order_by: Option<Vec<OrderByExpr>>) -> Result<Vec<(Expr, bool)>, QueryError> {
    let mut order = Vec::new();
    if let Some(sql_order_by_exprs) = order_by {
        for e in sql_order_by_exprs {
            order.push((*(convert_to_expr(&e.expr))?, !e.asc.unwrap_or(true)));
        }
    }
    Ok(order)
}

fn get_limit(limit: Option<ASTNode>) -> Result<u64, QueryError> {
    match limit {
        Some(ASTNode::Value(Value::Number(int))) => Ok(int.parse::<u64>().unwrap()),
        None => Ok(100),
        _ => Err(QueryError::NotImplemented(format!(
            "Invalid expression in limit clause: {:?}",
            limit
        ))),
    }
}

fn get_offset(offset: Option<Offset>) -> Result<u64, QueryError> {
    match offset {
        None => Ok(0),
        Some(offset) => match offset.value {
            ASTNode::Value(Value::Number(rows)) => Ok(rows.parse::<u64>().unwrap()),
            expr => Err(QueryError::ParseError(format!(
                "Invalid expression in offset clause: Expected constant integer, got {:?}",
                expr,
            ))),
        },
    }
}

fn convert_to_expr(node: &ASTNode) -> Result<Box<Expr>, QueryError> {
    Ok(Box::new(match node {
        ASTNode::BinaryOp {
            ref left,
            ref op,
            ref right,
        } => Expr::Func2(map_binary_operator(op)?, convert_to_expr(left)?, convert_to_expr(right)?),
        ASTNode::UnaryOp {
            ref op,
            expr: ref expression,
        } => Expr::Func1(map_unary_operator(op)?, convert_to_expr(expression)?),
        ASTNode::Value(ref literal) => Expr::Const(get_raw_val(literal)?),
        ASTNode::Identifier(ref identifier) => {
            Expr::ColName(strip_quotes(identifier.value.as_ref()))
        }
        ASTNode::Nested(inner) => *convert_to_expr(inner)?,
        ASTNode::Function(f) => match format!("{}", f.name).to_uppercase().as_ref() {
            "TO_YEAR" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in TO_YEAR function".to_string(),
                    ));
                }
                Expr::Func1(Func1Type::ToYear, convert_to_expr(&f.args[0])?)
            }
            "REGEX" => {
                if f.args.len() != 2 {
                    return Err(QueryError::ParseError(
                        "Expected two arguments in regex function".to_string(),
                    ));
                }
                Expr::Func2(Func2Type::RegexMatch, convert_to_expr(&f.args[0])?, convert_to_expr(&f.args[1])?)
            }
            "LENGTH" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one arguments in length function".to_string(),
                    ));
                }
                Expr::Func1(Func1Type::Length, convert_to_expr(&f.args[0])?)
            }
            "COUNT" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in COUNT function".to_string(),
                    ));
                }
                Expr::Aggregate(Aggregator::Count, convert_to_expr(&f.args[0])?)
            }
            "SUM" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in SUM function".to_string(),
                    ));
                }
                Expr::Aggregate(Aggregator::Sum, convert_to_expr(&f.args[0])?)
            }
            "AVG" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in AVG function".to_string(),
                    ));
                }
                Expr::Func2(
                    Func2Type::Divide,
                    Box::new(Expr::Aggregate(Aggregator::Sum, convert_to_expr(&f.args[0])?)),
                    Box::new(Expr::Aggregate(Aggregator::Count, convert_to_expr(&f.args[0])?)),
                )
            }
            "MAX" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MAX function".to_string(),
                    ));
                }
                Expr::Aggregate(Aggregator::Max, convert_to_expr(&f.args[0])?)
            }
            "MIN" => {
                if f.args.len() != 1 {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MIN function".to_string(),
                    ));
                }
                Expr::Aggregate(Aggregator::Min, convert_to_expr(&f.args[0])?)
            }
            _ => return Err(QueryError::NotImplemented(format!("Function {:?}", f.name))),
        },
        ASTNode::IsNull(ref node) => Expr::Func1(Func1Type::IsNull, convert_to_expr(node)?),
        ASTNode::IsNotNull(ref node) => Expr::Func1(Func1Type::IsNotNull, convert_to_expr(node)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", node))),
    }))
}

fn strip_quotes(ident: &str) -> String {
    if ident.starts_with('`') || ident.starts_with('"') {
        ident[1..ident.len() - 1].to_string()
    } else {
        ident.to_string()
    }
}

fn map_unary_operator(op: &UnaryOperator) -> Result<Func1Type, QueryError> {
    Ok(match op {
        UnaryOperator::Not => Func1Type::Not,
        UnaryOperator::Minus => Func1Type::Negate,
        _ => return Err(fatal!("Unexpected unary operator: {}", op)),
    })
}

fn map_binary_operator(o: &BinaryOperator) -> Result<Func2Type, QueryError> {
    Ok(match o {
        BinaryOperator::And => Func2Type::And,
        BinaryOperator::Plus => Func2Type::Add,
        BinaryOperator::Minus => Func2Type::Subtract,
        BinaryOperator::Multiply => Func2Type::Multiply,
        BinaryOperator::Divide => Func2Type::Divide,
        BinaryOperator::Modulus => Func2Type::Modulo,
        BinaryOperator::Gt => Func2Type::GT,
        BinaryOperator::GtEq => Func2Type::GTE,
        BinaryOperator::Lt => Func2Type::LT,
        BinaryOperator::LtEq => Func2Type::LTE,
        BinaryOperator::Eq => Func2Type::Equals,
        BinaryOperator::NotEq => Func2Type::NotEquals,
        BinaryOperator::Or => Func2Type::Or,
        BinaryOperator::Like => Func2Type::Like,
        BinaryOperator::NotLike => Func2Type::NotLike,
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Unsupported operator {:?}",
                o
            )))
        }
    })
}

// Fn to map sqlparser-rs `Value` to LocustDB's `RawVal`.
fn get_raw_val(constant: &Value) -> Result<RawVal, QueryError> {
    match constant {
        Value::Number(int) => Ok(RawVal::Int(int.parse::<i64>().unwrap())),
        Value::SingleQuotedString(string) => Ok(RawVal::Str(string.to_string())),
        Value::Null => Ok(RawVal::Null),
        _ => Err(QueryError::NotImplemented(format!("{:?}", constant))),
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
