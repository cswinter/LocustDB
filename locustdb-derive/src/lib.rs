#![feature(proc_macro_non_items, proc_macro_diagnostic)]
extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;

mod reify_types;
mod enum_syntax;

use self::proc_macro::TokenStream;

#[proc_macro]
pub fn reify_types(input: TokenStream) -> TokenStream {
    reify_types::reify_types(input)
}

#[proc_macro_derive(EnumSyntax)]
pub fn enum_syntax(input: TokenStream) -> TokenStream {
    enum_syntax::enum_syntax(input)
}
