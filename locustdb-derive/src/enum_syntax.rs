use super::proc_macro::TokenStream;
use syn::*;
use proc_macro2::Span;

pub fn enum_syntax(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);


    if let Data::Enum(DataEnum { variants, .. }) = input.data {
        let enum_ident = input.ident;
        let mut constructors = Vec::<Item>::new();
        let boxed: Type = parse_quote!(Box<#enum_ident>);
        let string: Type = parse_quote!(String);
        for variant in variants.into_iter() {
            if let Fields::Named(fields) = variant.fields {
                let variant_ident = variant.ident.clone();
                let ident_snake = studley_to_snake(variant.ident);
                let mut fn_inputs = Vec::<FnArg>::new();
                let mut struct_args = Vec::<FieldValue>::new();
                let mut fn_generics = Vec::<GenericArgument>::new();
                for field in fields.named.into_iter() {
                    let field_ident = field.ident.clone().unwrap();
                    if field.ty == boxed {
                        let type_ident = Ident::new(&format!("_T{}", fn_generics.len()), Span::call_site());
                        fn_generics.push(parse_quote!(#type_ident: Into<#enum_ident>));
                        fn_inputs.push(parse_quote!(#field_ident: #type_ident));
                        struct_args.push(parse_quote!(#field_ident: Box::new(#field_ident.into())));
                    } else if field.ty == string {
                        fn_inputs.push(parse_quote!(#field_ident: &str));
                        struct_args.push(parse_quote!(#field_ident: #field_ident.to_string()));
                    } else {
                        fn_inputs.push(parse_quote!(#field));
                        struct_args.push(parse_quote!(#field_ident));
                    }
                }

                let item = parse_quote! {
                    pub fn #ident_snake <#(#fn_generics),*> ( #(#fn_inputs),* ) -> #enum_ident {
                        #enum_ident::#variant_ident { #(#struct_args),* }
                    }
                };
                constructors.push(item);
            }
        }
        let expanded = quote! {
            // TODO(clemens): inherit visibility modifier from definition
            pub mod syntax {
                use super::*;

                #(#constructors)*
            }
        };

        // Hand the output tokens back to the compiler
        TokenStream::from(expanded)
    } else {
        // TODO(clemens): emit error
        panic!("")
    }
}

fn studley_to_snake(ident: Ident) -> Ident {
    let mut snake_case = String::new();
    let mut previous_lowercase = false;
    for c in format!("{}", ident).chars() {
        if c.is_uppercase() {
            if previous_lowercase {
                snake_case.push('_');
            }
            previous_lowercase = false;
            for l in c.to_lowercase() {
                snake_case.push(l);
            }
        } else {
            previous_lowercase = true;
            snake_case.push(c);
        }
    }
    Ident::new(&snake_case, ident.span())
}