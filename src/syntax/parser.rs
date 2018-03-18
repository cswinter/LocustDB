#![allow(unused_parens)]

use std::str;
use std::str::FromStr;
use nom::{digit, is_alphabetic, is_alphanumeric, multispace};

use syntax::expression::*;
use syntax::limit::LimitClause;
use engine::query::*;
use engine::aggregator::Aggregator;
use ingest::raw_val::RawVal;
use std::boxed::Box;
use time;


named!(pub parse_query<&[u8], Query>, alt_complete!(full_query | simple_query));

named!(full_query<&[u8], Query>,
    do_parse!(
        tag_no_case!("select") >>
        multispace >>
        select: select_clauses >>
        opt!(multispace) >>
        table: from_clause >>
        multispace >>
        tag_no_case!("where") >>
        multispace >>
        filter: expr >>
        opt!(multispace) >>
        order_by: opt!(order_by_clause) >>
        opt!(multispace) >>
        limit: opt!(limit_clause) >>
        opt!(multispace) >>
        char!(';') >>
        (construct_query(select, table, filter, order_by, limit))
    )
);


named!(simple_query<&[u8], Query>,
    do_parse!(
        tag_no_case!("select") >>
        multispace >>
        select: select_clauses >>
        opt!(multispace) >>
        table: from_clause >>
        opt!(multispace) >>
        order_by: opt!(order_by_clause) >>
        opt!(multispace) >>
        limit: opt!(limit_clause) >>
        opt!(multispace) >>
        opt!(char!(';')) >>
        (construct_query(select, table, Expr::Const(RawVal::Int(1)), order_by, limit))
    )
);

fn construct_query(select_clauses: Vec<AggregateOrSelect>,
                       table: &str,
                       filter: Expr,
                       order_by: Option<(String, bool)>,
                       limit: Option<LimitClause>)
                       -> Query {
    let (select, aggregate) = partition(select_clauses);
    let order_desc = order_by.as_ref().map(|x| x.1).unwrap_or(false);
    Query {
        select,
        table: table.to_string(),
        filter,
        aggregate,
        order_by: order_by.map(|x| x.0),
        order_desc,
        limit: limit.unwrap_or(LimitClause { limit: 100, offset: 0 }),
        order_by_index: None,
    }
}

fn partition(select_or_aggregates: Vec<AggregateOrSelect>)
                 -> (Vec<Expr>, Vec<(Aggregator, Expr)>) {
    let (selects, aggregates): (Vec<AggregateOrSelect>, Vec<AggregateOrSelect>) =
        select_or_aggregates.into_iter()
            .partition(|x| match *x {
                AggregateOrSelect::Select(_) => true,
                _ => false,
            });

    (selects.into_iter()
         .filter_map(|x| match x {
             AggregateOrSelect::Select(expr) => Some(expr),
             _ => None,
         })
         .collect(),
     aggregates.into_iter()
         .filter_map(|x| match x {
             AggregateOrSelect::Aggregate(agg) => Some(agg),
             _ => None,
         })
         .collect())
}

named!(from_clause<&[u8], &str>,
    do_parse!(
        tag_no_case!("from") >>
        multispace >>
        from: identifier >>
        (from)
    )
);

named!(select_clauses<&[u8], Vec<AggregateOrSelect>>,
    alt!(
        do_parse!(
            opt!(multispace) >>
            tag!("*") >>
            opt!(multispace) >>
            (vec![AggregateOrSelect::Select(Expr::ColName("*".to_string()))])
        ) |
        separated_list!(
            tag!(","),
            alt_complete!(aggregate_clause | select_clause)
        )
    )
);

named!(aggregate_clause<&[u8], AggregateOrSelect>,
    do_parse!(
        opt!(multispace) >>
        atype: aggregate_func >>
        char!('(') >>
        e: expr >>
        opt!(multispace) >>
        char!(')') >>
        (AggregateOrSelect::Aggregate((atype, e)))
    )
);

named!(select_clause<&[u8], AggregateOrSelect>, map!(expr, AggregateOrSelect::Select));

named!(aggregate_func<&[u8], Aggregator>, alt!(count | sum));

named!(count<&[u8], Aggregator>,
    map!( tag_no_case!("count"), |_| Aggregator::Count )
);

named!(sum<&[u8], Aggregator>,
    map!( tag_no_case!("sum"), |_| Aggregator::Sum )
);

named!(expr<&[u8], Expr>,
    do_parse!(
        opt!(multispace) >>
        result: alt!(infix_expr | expr_no_left_recur) >>
        (result)
    )
);

named!(expr_no_left_recur<&[u8], Expr>,
    do_parse!(
        opt!(multispace) >>
        result: alt!(parentheses | template | function | negation | colname | constant) >>
        (result)
    )
);

named!(parentheses<&[u8], Expr>,
    do_parse!(
        char!('(') >>
        e1: expr >>
        opt!(multispace) >>
        char!(')') >>
        (e1)
    )
);

named!(template<&[u8], Expr>,
    alt!( last_hour | last_day )
);

named!(last_hour<&[u8], Expr>,
    map!(
        tag_no_case!("$LAST_HOUR"),
        |_| Expr::Func(
                FuncType::GT,
                Box::new(Expr::ColName("timestamp".to_string())),
                Box::new(Expr::Const(RawVal::Int(time::now().to_timespec().sec - 3600)))
        )
    )
);

named!(last_day<&[u8], Expr>,
    map!(
        tag_no_case!("$LAST_DAY"),
        |_| Expr::Func(
                FuncType::GT,
                Box::new(Expr::ColName("timestamp".to_string())),
                Box::new(Expr::Const(RawVal::Int(time::now().to_timespec().sec - 86400)))
        )
    )
);

named!(function<&[u8], Expr>,
    do_parse!(
        ft: function_name >>
        char!('(') >>
        e1: expr >>
        opt!(multispace) >>
        char!(',') >>
        e2: expr >>
        opt!(multispace) >>
        char!(')') >>
        (Expr::func(ft, e1, e2))
    )
);

named!(infix_expr<&[u8], Expr>,
    do_parse!(
        e1: expr_no_left_recur >>
        opt!(multispace) >>
        ft: infix_function_name >>
        e2: expr >>
        (Expr::func(ft, e1, e2))
    )
);

named!(negation<&[u8], Expr>,
    do_parse!(
        char!('-') >>
        opt!(multispace) >>
        e: expr >>
        (Expr::func(FuncType::Negate, e, Expr::Const(RawVal::Null)))
    )
);

named!(constant<&[u8], Expr>,
    map!(
        alt!(integer |  string),
        Expr::Const
    )
);


named!(integer<&[u8], RawVal>,
    map!(
        map_res!(
            map_res!(
                digit,
                str::from_utf8
            ),
            FromStr::from_str
        ),
        RawVal::Int
    )
);

named!(number<&[u8], u64>,
    map_res!(
        map_res!(
            digit,
            str::from_utf8
        ),
        FromStr::from_str
    )
);

named!(string<&[u8], RawVal>,
    do_parse!(
        char!('"') >>
        s: is_not!("\"") >>
        char!('"') >>
        (RawVal::Str(str::from_utf8(s).unwrap().to_string()))
    )
);

named!(colname<&[u8], Expr>,
    map!(
        identifier,
        |ident: &str| Expr::ColName(ident.to_string())
    )
);

named!(function_name<&[u8], FuncType>,
    alt!( infix_function_name | regex )
);

named!(infix_function_name<&[u8], FuncType>,
    alt!( equals | and | or | greater | less | add | subtract | divide | multiply )
);

named!(divide<&[u8], FuncType>,
    map!( tag!("/"), |_| FuncType::Divide)
);

named!(add<&[u8], FuncType>,
    map!( tag!("+"), |_| FuncType::Add)
);

named!(multiply<&[u8], FuncType>,
    map!( tag!("*"), |_| FuncType::Multiply)
);

named!(subtract<&[u8], FuncType>,
    map!( tag!("-"), |_| FuncType::Subtract)
);

named!(equals<&[u8], FuncType>,
    map!( tag!("="), |_| FuncType::Equals)
);

named!(greater<&[u8], FuncType>,
    map!( tag!(">"), |_| FuncType::GT)
);

named!(less<&[u8], FuncType>,
    map!( tag!("<"), |_| FuncType::LT)
);

named!(and<&[u8], FuncType>,
    map!( tag_no_case!("and"), |_| FuncType::And)
);

named!(or<&[u8], FuncType>,
    map!( tag_no_case!("or"), |_| FuncType::Or)
);

named!(regex<&[u8], FuncType>,
    map!( tag_no_case!("regex"), |_| FuncType::RegexMatch)
);


named!(identifier<&[u8], &str>,
    map_res!(
        take_while1!(is_ident_char),
        create_sql_identifier
    )
);

fn create_sql_identifier(bytes: &[u8]) -> Result<&str, String> {
    if is_ident_start_char(bytes[0]) {
        str::from_utf8(bytes).map_err(|_| "UTF8Error".to_string())
    } else {
        Err("Identifier must not start with number.".to_string())
    }
}

fn is_ident_start_char(chr: u8) -> bool {
    is_alphabetic(chr) || chr == b'_'
}

fn is_ident_char(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

named!(limit_clause<&[u8], LimitClause>,
    do_parse!(
        tag_no_case!("limit") >>
        multispace >>
        limit_val: number >>
        offset_val: opt!(
            do_parse!(
                multispace >>
                tag_no_case!("offset") >>
                multispace >>
                val: number >>
                (val)
            )) >>
        (LimitClause{limit: limit_val,
                     offset: match offset_val {
                                 Some(inner) => inner,
                                 None => 0,
                             }
                    })
    )
);

named!(order_by_clause<&[u8], (String, bool)>,
    alt!(
        do_parse!(
            tag_no_case!("order by") >>
            multispace >>
            order_by: identifier >>
            multispace >>
            tag_no_case!("desc") >>
            (order_by.to_string(), true)
        ) |
        do_parse!(
            tag_no_case!("order by") >>
            multispace >>
            order_by: identifier >>
            opt!(preceded!(multispace, tag_no_case!("asc"))) >>
            (order_by.to_string(), false)
        )
    )
);

enum AggregateOrSelect {
    Aggregate((Aggregator, Expr)),
    Select(Expr),
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star() {
        assert_eq!(
            format!("{:?}", parse_query("select * from default;".as_bytes())),
            "Done([], Query { select: [ColName(\"*\")], table: \"default\", filter: Const(Int(1)), aggregate: [], order_by: None, order_desc: false, limit: LimitClause { limit: 100, offset: 0 }, order_by_index: None })");
    }

    #[test]
    fn test_last_hour() {
        assert!(
        format!("{:?}", parse_query("select * from default where $LAST_HOUR;".as_bytes())).starts_with(
            "Done([], Query { select: [ColName(\"*\")], table: \"default\", filter: Func(GT, ColName(\"timestamp\"), Const(Int(")
        )
    }
}
