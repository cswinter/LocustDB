#![feature(proc_macro_non_items, proc_macro_diagnostic)]
extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;

use self::proc_macro::TokenStream;
use syn::parse::{Parse, ParseStream, Result};
use syn::token::{Brace, Match};
use syn::punctuated::Punctuated;
use syn::{Arm, Pat, Block, Stmt, parse_macro_input, parse_quote, Expr, Ident, LitStr, Token, ExprMatch};


struct TypeExpand {
    name: LitStr,
    specs: Vec<Declaration>,
    expr: Expr,
}

#[derive(Clone)]
struct Declaration {
    variables: Vec<Ident>,
    t: Ident,
}

impl Parse for TypeExpand {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: LitStr = input.parse()?;
        input.parse::<Token![;]>()?;

        let specs = Punctuated::<Declaration, Token![,]>::parse_separated_nonempty(input)?;
        input.parse::<Token![;]>()?;

        let expr: Expr = input.parse()?;
        input.parse::<Token![;]>()?;

        Ok(TypeExpand {
            name,
            specs: specs.into_iter().collect(),
            expr,
        })
    }
}

impl Parse for Declaration {
    fn parse(input: ParseStream) -> Result<Self> {
        let variables = Punctuated::<Ident, Token![,]>::parse_separated_nonempty(input)?;
        input.parse::<Token![:]>()?;
        let t: Ident = input.parse()?;

        Ok(Declaration {
            variables: variables.into_iter().collect(),
            t,
        })
    }
}

#[proc_macro]
pub fn reify_types(input: TokenStream) -> TokenStream {
    let TypeExpand {
        name,
        specs,
        expr,
    } = parse_macro_input!(input as TypeExpand);
    // TODO(clemens): add assertion that types within each spec are equal

    let mut type_domains = Vec::with_capacity(specs.len());
    let mut variable_groups = Vec::with_capacity(specs.len());
    for Declaration { variables, t } in specs {
        type_domains.push(match types(&t) {
            Some(ts) => ts,
            None => {
                t.span().unstable().error(format!("{} is not a valid type.", t)).emit();
                return TokenStream::new();
            }
        });
        variable_groups.push(variables);
    }

    let mut cross_product = Vec::new();
    let mut indices = vec![0; type_domains.len()];
    'outer: loop {
        cross_product.push(
            indices
                .iter()
                .enumerate()
                .map(|(t, &i)| type_domains[t][i])
                .collect::<Vec<_>>()
        );

        for i in 0..type_domains.len() {
            indices[i] += 1;
            if indices[i] < type_domains[i].len() {
                break;
            }
            if i == type_domains.len() - 1 {
                break 'outer;
            } else {
                indices[i] = 0;
            }
        }
    }

    let mut match_arms = cross_product.into_iter().map(|types| {
        let mut pattern = types[0].pattern();
        let mut block: Block = parse_quote!({
            #expr
        });
        for (i, t) in types.into_iter().enumerate() {
            for v in variable_groups[i].clone().into_iter() {
                block.stmts.insert(block.stmts.len() - 1, t.reify(v));
            }
            if i != 0 {
                let p2 = t.pattern();
                pattern = parse_quote!((#pattern, #p2));
            }
        }

        parse_quote!(#pattern => #block)
    }).collect::<Vec<Arm>>();

    let variable = variable_groups[0][0].clone();
    let mut match_expr: Expr = parse_quote!(#variable.tag);
    for vg in &variable_groups[1..] {
        let variable = vg[0].clone();
        match_expr = parse_quote!((#match_expr, #variable.tag))
    }

    match_arms.push(parse_quote! {
        t => panic!("{} not supported for type {:?}", #name, t),
    });

    let expanded = ExprMatch {
        attrs: vec![],
        match_token: Match::default(),
        expr: Box::new(match_expr),
        brace_token: Brace::default(),
        arms: match_arms,
    };

    TokenStream::from(quote!(#expanded))
}

fn types(t: &Ident) -> Option<Vec<Type>> {
    match t.to_string().as_ref() {
        "IntegerNoU64" => Some(vec![Type::U8, Type::U16, Type::U32, Type::I64]),
        "Integer" => Some(vec![Type::U8, Type::U16, Type::U32, Type::U64, Type::I64]),
        "Primitive" => Some(vec![Type::U8, Type::U16, Type::U32, Type::U64, Type::I64, Type::Str]),
        _ => None,
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
enum Type {
    U8,
    U16,
    U32,
    U64,
    I64,
    Str,
}

impl Type {
    fn pattern(&self) -> Pat {
        match self {
            Type::U8 => parse_quote!(EncodingType::U8),
            Type::U16 => parse_quote!(EncodingType::U16),
            Type::U32 => parse_quote!(EncodingType::U32),
            Type::U64 => parse_quote!(EncodingType::U64),
            Type::I64 => parse_quote!(EncodingType::I64),
            Type::Str => parse_quote!(EncodingType::Str),
        }
    }

    fn reify(&self, variable: Ident) -> Stmt {
        match self {
            Type::U8 => parse_quote!( let #variable = #variable.buffer.u8(); ),
            Type::U16 => parse_quote!( let #variable = #variable.buffer.u16(); ),
            Type::U32 => parse_quote!( let #variable = #variable.buffer.u32(); ),
            Type::U64 => parse_quote!( let #variable = #variable.buffer.u64(); ),
            Type::I64 => parse_quote!( let #variable = #variable.buffer.i64(); ),
            Type::Str => parse_quote!( let #variable = #variable.buffer.str(); ),
        }
    }
}
