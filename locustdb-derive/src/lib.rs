#![feature(proc_macro_hygiene, proc_macro_diagnostic)]
#![recursion_limit = "128"]
extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;
#[macro_use]
extern crate lazy_static;
extern crate regex;

mod reify_types;
mod enum_syntax;
mod ast_builder;

use self::proc_macro::TokenStream;

#[proc_macro]
pub fn reify_types(input: TokenStream) -> TokenStream {
    reify_types::reify_types(input)
}

#[proc_macro_derive(EnumSyntax)]
pub fn enum_syntax(input: TokenStream) -> TokenStream {
    enum_syntax::enum_syntax(input)
}

#[proc_macro_derive(ASTBuilder, attributes(newstyle, input, internal, output, nohash))]
pub fn ast_builder(input: TokenStream) -> TokenStream { ast_builder::ast_builder(input) }
