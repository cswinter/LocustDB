extern crate sqlparser;

use crate::engine::Query;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::syntax::expression::Expr;
use crate::syntax::expression::*;
use crate::syntax::limit::LimitClause;
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
        Some(ref s) => *convert_to_native_expr(s)?,
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
        limit_clause,
        ..
    } = *query;
    match *body {
        SetExpr::Select(box Select {
            distinct,
            projection,
            mut from,
            selection,
            group_by,
            having,
            // TODO: ensure other items not set
            ..
        }) => {
            if let GroupByExpr::Expressions(exprs, with_mods) = group_by
                && (!exprs.is_empty() || !with_mods.is_empty())
            {
                Err(QueryError::NotImplemented("Group By  (Hint: If your SELECT clause contains any aggregation expressions, results will implicitly grouped by all other expresssions.)".to_string()))
            } else if having.is_some() {
                Err(QueryError::NotImplemented("Having".to_string()))
            } else if distinct.is_some() {
                Err(QueryError::NotImplemented("DISTINCT".to_string()))
            } else if from.len() > 1 {
                Err(QueryError::NotImplemented(
                    "Selecting from multiple tables.".to_string(),
                ))
            } else if !from.is_empty() && !from[0].joins.is_empty() {
                Err(QueryError::NotImplemented("JOIN".to_string()))
            } else {
                let (limit, offset) = match limit_clause {
                    Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                        (limit, offset)
                    }
                    _ => (None, None),
                };
                Ok((
                    projection,
                    from.pop().map(|t| t.relation),
                    selection,
                    order_by.and_then(|o| match o.kind {
                        OrderByKind::Expressions(exprs) => Some(exprs),
                        _ => None,
                    }),
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

fn get_projection(projection: Vec<SelectItem>) -> Result<Vec<ColumnInfo>, QueryError> {
    let mut result = Vec::<ColumnInfo>::new();
    for elem in &projection {
        match elem {
            SelectItem::UnnamedExpr(e) => {
                // sqlparser-rs provides string of the projection as entered by the user.
                // Storing this string in Query.select corresponding to locustdb's Expr.
                // These will later be used as colnames of query results.
                result.push(ColumnInfo {
                    expr: *convert_to_native_expr(e)?,
                    name: strip_quotes(&format!("{}", e)),
                })
            }
            SelectItem::Wildcard(_) => result.push(ColumnInfo {
                expr: Expr::ColName('*'.to_string()),
                name: "*".to_string(),
            }),
            SelectItem::ExprWithAlias { expr, alias } => result.push(ColumnInfo {
                expr: *convert_to_native_expr(expr)?,
                name: strip_quotes(&alias.to_string()),
            }),
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
        Some(TableFactor::Table { name, .. }) => Ok(strip_quotes(&format!("{}", name))),
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
            order.push((
                *(convert_to_native_expr(&e.expr))?,
                !e.options.asc.unwrap_or(true),
            ));
        }
    }
    Ok(order)
}

fn get_limit(limit: Option<ASTNode>) -> Result<u64, QueryError> {
    match limit {
        Some(ASTNode::Value(ValueWithSpan {
            value: Value::Number(int, _),
            ..
        })) => Ok(int.parse::<u64>().unwrap()),
        None => Ok(u64::MAX),
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
            ASTNode::Value(ValueWithSpan {
                value: Value::Number(rows, _),
                ..
            }) => Ok(rows.parse::<u64>().unwrap()),
            expr => Err(QueryError::ParseError(format!(
                "Invalid expression in offset clause: Expected constant integer, got {:?}",
                expr,
            ))),
        },
    }
}

fn function_arg_to_expr(node: &FunctionArg) -> Result<&ASTNode, QueryError> {
    match node {
        FunctionArg::Named { name, .. } => Err(QueryError::NotImplemented(format!(
            "Named function arguments are not supported: {}",
            name
        ))),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(QueryError::NotImplemented(
            "Wildcard function arguments are not supported".to_string(),
        )),
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
            Err(QueryError::NotImplemented(
                "Qualified wildcard function arguments are not supported".to_string(),
            ))
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Function argument {:?}",
            node
        ))),
    }
}

fn convert_to_native_expr(node: &ASTNode) -> Result<Box<Expr>, QueryError> {
    Ok(Box::new(match node {
        ASTNode::BinaryOp {
            ref left,
            ref op,
            ref right,
        } => Expr::Func2(
            map_binary_operator(op)?,
            convert_to_native_expr(left)?,
            convert_to_native_expr(right)?,
        ),
        ASTNode::UnaryOp {
            ref op,
            expr: ref expression,
        } => Expr::Func1(map_unary_operator(op)?, convert_to_native_expr(expression)?),
        ASTNode::Value(ValueWithSpan {
            value: ref literal, ..
        }) => Expr::Const(get_raw_val(literal)?),
        ASTNode::Identifier(ref identifier) => {
            Expr::ColName(strip_quotes(identifier.value.as_ref()))
        }
        ASTNode::Nested(inner) => *convert_to_native_expr(inner)?,
        ASTNode::Function(f) => match format!("{}", f.name).to_uppercase().as_ref() {
            "TO_YEAR" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => Expr::Func1(
                    Func1Type::ToYear,
                    convert_to_native_expr(function_arg_to_expr(&list.args[0])?)?,
                ),
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in TO_YEAR function".to_string(),
                    ));
                }
            },
            "REGEX" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 2 => Expr::Func2(
                    Func2Type::RegexMatch,
                    func_arg_to_native_expr(&list.args[0])?,
                    func_arg_to_native_expr(&list.args[1])?,
                ),
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected two arguments in regex function".to_string(),
                    ));
                }
            },
            "LENGTH" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => {
                    Expr::Func1(Func1Type::Length, func_arg_to_native_expr(&list.args[0])?)
                }
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one arguments in length function".to_string(),
                    ));
                }
            },
            "COUNT" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => {
                    Expr::Aggregate(Aggregator::Count, func_arg_to_native_expr(&list.args[0])?)
                }
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in COUNT function".to_string(),
                    ));
                }
            },
            "SUM" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => {
                    Expr::Aggregate(Aggregator::SumI64, func_arg_to_native_expr(&list.args[0])?)
                }
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in SUM function".to_string(),
                    ));
                }
            },
            "AVG" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => Expr::Func2(
                    Func2Type::Divide,
                    Box::new(Expr::Aggregate(
                        Aggregator::SumI64,
                        func_arg_to_native_expr(&list.args[0])?,
                    )),
                    Box::new(Expr::Aggregate(
                        Aggregator::Count,
                        func_arg_to_native_expr(&list.args[0])?,
                    )),
                ),
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in AVG function".to_string(),
                    ));
                }
            },
            "MAX" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => {
                    Expr::Aggregate(Aggregator::MaxI64, func_arg_to_native_expr(&list.args[0])?)
                }
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MAX function".to_string(),
                    ));
                }
            },
            "MIN" => match &f.args {
                FunctionArguments::List(list) if list.args.len() == 1 => {
                    Expr::Aggregate(Aggregator::MinI64, func_arg_to_native_expr(&list.args[0])?)
                }
                _ => {
                    return Err(QueryError::ParseError(
                        "Expected one argument in MIN function".to_string(),
                    ));
                }
            },
            _ => return Err(QueryError::NotImplemented(format!("Function {:?}", f.name))),
        },
        ASTNode::IsNull(ref node) => Expr::Func1(Func1Type::IsNull, convert_to_native_expr(node)?),
        ASTNode::IsNotNull(ref node) => {
            Expr::Func1(Func1Type::IsNotNull, convert_to_native_expr(node)?)
        }
        ASTNode::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            if escape_char.is_some() {
                return Err(QueryError::NotImplemented(
                    "LIKE with escape character".to_string(),
                ));
            }
            Expr::Func2(
                if *negated {
                    Func2Type::NotLike
                } else {
                    Func2Type::Like
                },
                convert_to_native_expr(expr)?,
                convert_to_native_expr(pattern)?,
            )
        }
        ASTNode::Floor { expr, .. } => Expr::Func1(Func1Type::Floor, convert_to_native_expr(expr)?),
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Parsing for this ASTNode not implemented: {:?}",
                node
            )))
        }
    }))
}

fn func_arg_to_native_expr(node: &FunctionArg) -> Result<Box<Expr>, QueryError> {
    convert_to_native_expr(function_arg_to_expr(node)?)
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
        BinaryOperator::Modulo => Func2Type::Modulo,
        BinaryOperator::Gt => Func2Type::GT,
        BinaryOperator::GtEq => Func2Type::GTE,
        BinaryOperator::Lt => Func2Type::LT,
        BinaryOperator::LtEq => Func2Type::LTE,
        BinaryOperator::Eq => Func2Type::Equals,
        BinaryOperator::NotEq => Func2Type::NotEquals,
        BinaryOperator::Or => Func2Type::Or,
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
        Value::Number(num, _) => {
            if num.parse::<i64>().is_ok() {
                Ok(RawVal::Int(num.parse::<i64>().unwrap()))
            } else {
                Ok(RawVal::Float(ordered_float::OrderedFloat(
                    num.parse::<f64>().unwrap(),
                )))
            }
        }
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
            format!("{:?}", parse_query("select * from default limit 100")),
            "Ok(Query { select: [ColumnInfo { expr: ColName(\"*\"), name: \"*\" }], table: \"default\", filter: Const(Int(1)), order_by: [], limit: LimitClause { limit: 100, offset: 0 } })");
    }

    #[test]
    fn test_alias() {
        assert_eq!(
            format!("{:?}", parse_query("select trip_id as id from default limit 100")),
            "Ok(Query { select: [ColumnInfo { expr: ColName(\"trip_id\"), name: \"id\" }], table: \"default\", filter: Const(Int(1)), order_by: [], limit: LimitClause { limit: 100, offset: 0 } })");
    }

    #[test]
    fn test_to_year() {
        assert_eq!(
            format!("{:?}", parse_query("select to_year(ts) from default limit 100")),
            "Ok(Query { select: [ColumnInfo { expr: Func1(ToYear, ColName(\"ts\")), name: \"to_year(ts)\" }], table: \"default\", filter: Const(Int(1)), order_by: [], limit: LimitClause { limit: 100, offset: 0 } })");
    }
}
