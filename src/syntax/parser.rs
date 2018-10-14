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
    let ast = match Parser::parse_sql(&dialect, query.to_string()) {
        Ok(ast) => ast,
        Err(e) => {
           if let ParserError::ParserError(e_str) = e {
               return Err(QueryError::ParseError(e_str));
           }
           return Err(QueryError::FatalError(format!("{:?}", e)));
        }
    };

    let mut select = Vec::<Expr>::new();
    let mut aggregate = Vec::<(Aggregator, Expr)>::new();
    let mut table = String::new();
    let mut filter = Expr::Const(RawVal::Int(1));
    let mut order_by_str = None;
    let mut order_desc = false;
    let mut limit_clause = LimitClause { limit: 100, offset: 0 };
    let order_by_index = None;

    if let ASTNode::SQLSelect 
        {projection, relation, selection, order_by, limit, .. } = ast.to_owned() 
    {
        for elem in projection {
            match elem.to_owned() {
                ASTNode::SQLIdentifier(expr) => 
                    select.push(Expr::ColName(expr)),
                ASTNode::SQLValue(constant) => 
                    select.push(Expr::Const(get_raw_val(constant))),
                ASTNode::SQLWildcard => 
                    select.push(Expr::ColName('*'.to_string())),
                ASTNode::SQLBinaryExpr{left, op, right} => {
                    select.push(generate_expression(left, op, right)?);
                },
                ASTNode::SQLFunction{id, args} => {
                    let expr = match args[0].to_owned() {
                        ASTNode::SQLValue(literal) => 
                            Expr::Const(get_raw_val(literal)),
                        ASTNode::SQLIdentifier(identifier) => 
                            Expr::ColName(identifier),
                        ASTNode::SQLBinaryExpr{left, op, right} => 
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

        let sql_identifier = relation.ok_or(QueryError::ParseError(format!("Table name missing.")))?;
        if let ASTNode::SQLIdentifier(table_name) = *sql_identifier {
            table = table_name;
        } else {
            return Err(QueryError::FatalError(format!("{:?}", *sql_identifier)));
        }

        filter = match selection {
            Some(binary_expr) => if let ASTNode::SQLBinaryExpr{left, op, right} = *binary_expr.to_owned() {
                generate_expression(left, op, right)?
            } else {
                return Err(QueryError::FatalError(format!("{:?}", *binary_expr)));
            },
            None => Expr::Const(RawVal::Int(1))
        };

        match order_by {
            Some(sql_order_by_exprs) => {
                for sql_order_by_expr in sql_order_by_exprs {
                    if let ASTNode::SQLIdentifier(identifier) = *sql_order_by_expr.expr {
                        order_by_str = Some(identifier);
                    }
                    order_desc = !sql_order_by_expr.asc;
                }
            },
            None => (),
        };

        
        limit_clause = LimitClause {
            limit: 100,
            offset: 0,
        };
        match limit {
            Some(sql_limit) => if let ASTNode::SQLValue(literal) = *sql_limit {
                let lmt = match literal {
                    Value::Long(int) => int,
                    _ => 100
                };
                limit_clause.limit = lmt as u64;
            },
            None => (),
        }
    }

    let result = Query {
        select,
        table,
        filter,
        aggregate,
        order_by: order_by_str,
        order_desc,
        limit: limit_clause,
        order_by_index,
    };

    Ok(result)
}

/// Fn to convert `SQLBinaryExpr` to LocustDB's `Expr`.
fn generate_expression(l: Box<ASTNode>, o: SQLOperator, r: Box<ASTNode>) 
    -> Result<Expr, QueryError> 
{
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

    let lhs = match *l.to_owned() {
        ASTNode::SQLValue(literal) => Box::new(Expr::Const(get_raw_val(literal))),
        ASTNode::SQLIdentifier(identifier) => Box::new(Expr::ColName(identifier)),
        // Recursively call in case of multiple operator expression
        ASTNode::SQLBinaryExpr{left, op, right} => Box::new(generate_expression(left, op, right)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", *l)))
    };

    let rhs = match *r.to_owned() {
        ASTNode::SQLValue(literal) => Box::new(Expr::Const(get_raw_val(literal))),
        ASTNode::SQLIdentifier(identifier) => Box::new(Expr::ColName(identifier)),
        // Recursively call in case of multiple operator expression        
        ASTNode::SQLBinaryExpr{left, op, right} => Box::new(generate_expression(left, op, right)?),
        _ => return Err(QueryError::NotImplemented(format!("{:?}", *r)))
    };

    Ok(Expr::Func2(operator, lhs, rhs))
}

// Fn to map sqlparser-rs `Value` to LocustDB's `RawVal`.
fn get_raw_val(constant: Value) -> RawVal {
    match constant.to_owned() {
        Value::Long(int) => RawVal::Int(int),
        Value::String(string)
        | Value::SingleQuotedString(string) 
        | Value::DoubleQuotedString(string) => RawVal::Str(string),
        Value::Null => RawVal::Null,
        _ => RawVal::Null
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

    // TODO: Add $LAST_HOUR, $LAST_DAY support
    // #[test]
    // fn test_last_hour() {
    //     assert!(
    //         format!("{:?}", parse_query("select * from default where $LAST_HOUR")).starts_with(
    //             "Ok(Query { select: [ColName(\"*\")], table: \"default\", filter: Func2(GT, ColName(\"timestamp\"), Const(Int(")
    //     )
    // }

    #[test]
    fn test_to_year() {
        assert_eq!(
            format!("{:?}", parse_query("select to_year(ts) from default")),
            "Ok(Query { select: [Func1(ToYear, ColName(\"ts\"))], table: \"default\", filter: Const(Int(1)), aggregate: [], order_by: None, order_desc: false, limit: LimitClause { limit: 100, offset: 0 }, order_by_index: None })");
    }
}
