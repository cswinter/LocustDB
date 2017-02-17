use std::str;
use std::str::FromStr;
use std::rc::Rc;
use nom::{digit, alphanumeric, is_alphabetic, multispace};

use value::*;
use expression::*;
use query_engine::*;
use aggregator::Aggregator;
use limit::LimitClause;



pub fn test() {
    let qstring = "select 1 where 2";
    let res = parse_query(qstring.as_bytes());
    println!("{:?}", res);
    println!("{:?}", res.unwrap().1);
}

named!(pub parse_query<&[u8], Query>, alt_complete!(full_query | simple_query));

named!(full_query<&[u8], Query>,
    do_parse!(
        tag_no_case!("select") >>
        multispace >>
        select: select_clauses >>
        multispace >>
        tag_no_case!("where") >>
        multispace >>
        filter: expr >>
        opt!(multispace) >>
        limit: opt!(limit_clause) >>
        opt!(multispace) >>
        char!(';') >>
        (construct_query(select, filter, limit))
    )
);

named!(simple_query<&[u8], Query>,
    do_parse!(
        tag_no_case!("select") >>
        multispace >>
        select: select_clauses >>
        opt!(multispace) >>
        limit: opt!(limit_clause) >>
        opt!(multispace) >>
        opt!(char!(';')) >>
        (construct_query(select, Expr::Const(ValueType::Bool(true)), limit))
    )
);

fn construct_query<'a>(select_clauses: Vec<AggregateOrSelect<'a>>, filter: Expr<'a>, limit: Option<LimitClause>) -> Query<'a> {
    let (select, aggregate) = partition(select_clauses);
    Query { select: select, filter: filter, aggregate: aggregate, limit: limit }
}

fn partition<'a>(select_or_aggregates: Vec<AggregateOrSelect<'a>>) -> (Vec<Expr<'a>>, Vec<(Aggregator, Expr<'a>)>) {
    let (selects, aggregates): (Vec<AggregateOrSelect>, Vec<AggregateOrSelect>) =
        select_or_aggregates.into_iter()
            .partition(|x| match x {
                &AggregateOrSelect::Select(_) => true,
                _ => false,
            });

    (
        selects.into_iter().filter_map(|x| match x { AggregateOrSelect::Select(expr) => Some(expr), _ => None }).collect(),
        aggregates.into_iter().filter_map(|x| match x { AggregateOrSelect::Aggregate(agg) => Some(agg), _ => None }).collect(),
    )
}


named!(select_clauses<&[u8], Vec<AggregateOrSelect>>,
    separated_list!(
        tag!(","),
        alt_complete!(aggregate_clause | select_clause)
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
        result: alt!(function | colname | constant) >>
        (result)
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

named!(constant<&[u8], Expr>,
    map!(
        alt!(integer |  string),
        Expr::Const
    )
);

named!(integer<&[u8], ValueType>,
    map!(
        map_res!(
            map_res!(
                digit,
                str::from_utf8
            ),
            FromStr::from_str
        ),
        |int| ValueType::Integer(int)
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

named!(string<&[u8], ValueType>,
    do_parse!(
        char!('"') >>
        s: is_not!("\"") >>
        char!('"') >>
        (ValueType::Str(str::from_utf8(s).unwrap()))
    )
);

named!(colname<&[u8], Expr>,
    map!(
        identifier,
        |ident: &str| Expr::ColName(Rc::new(ident.to_string()))
    )
);

named!(function_name<&[u8], FuncType>,
    alt!( equals | and | greater | less )
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


named!(identifier<&[u8], &str>,
    map_res!(
        take_while1!(is_sql_identifier),
        str::from_utf8
    )
);

named!(limit_clause<&[u8], LimitClause>,
    do_parse!(
        tag_no_case!("limit") >>
        multispace >>
        limit_val: number >>
        (LimitClause{limit: limit_val, offset:0})
    )
);

fn is_sql_identifier(chr: u8) -> bool {
    is_alphabetic(chr) || chr == '_' as u8
}

enum AggregateOrSelect<'a> {
    Aggregate((Aggregator, Expr<'a>)),
    Select(Expr<'a>),
}
