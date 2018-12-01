use super::proc_macro::TokenStream;
use syn::*;
use proc_macro2::Span;

use regex::Regex;


pub fn ast_builder(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);


    if let Data::Enum(DataEnum { variants, .. }) = input.data {
        let enum_ident = input.ident;
        let mut constructors = Vec::<Item>::new();
        let mut productions = Vec::<Item>::new();
        let boxed: Type = parse_quote!(Box<#enum_ident>);
        let string: Type = parse_quote!(String);
        // let attribute: Path = parse_quote!(newstyle);
        for variant in variants.into_iter() {
            if let Fields::Named(fields) = variant.fields {
                let variant_ident = variant.ident.clone();
                let ident_snake = studley_to_snake(variant.ident);
                if variant.attrs.iter().find(|attr| attr.path == parse_quote!(newstyle)).is_some() {
                    let mut fn_inputs = Vec::<FnArg>::new();
                    let mut new_buffers = Vec::<Stmt>::new();
                    let mut struct_args = Vec::<FieldValue>::new();
                    let mut result = Vec::<Ident>::new();
                    let mut result_type = Vec::<Type>::new();
                    let mut output: Expr = parse_quote!(None);
                    for field in fields.named.into_iter() {
                        let field_ident = field.ident.clone().unwrap();
                        let field_type = field.ty;
                        let ident_str = LitStr::new(&format!("{}", field_ident), Span::call_site());
                        if let Some(attr) = field.attrs.iter().find(|attr| attr.path == parse_quote!(internal)) {
                            let new_buffer = if let Some(t) = parse_type(attr.tts.to_string()) {
                                parse_quote!(let #field_ident = result.named_buffer(#ident_str, #t);)
                            } else {
                                create_buffer(&field_ident, &field_type)
                            };
                            new_buffers.push(new_buffer);
                        } else if let Some(attr) = field.attrs.iter().find(|attr| attr.path == parse_quote!(output)) {
                            let new_buffer: Stmt = if attr.tts.to_string().contains("t_provided") {
                                let provided_type_ident = Ident::new(&format!("{}_type", field_ident), Span::call_site());
                                fn_inputs.push(parse_quote!(#provided_type_ident: EncodingType));
                                output = parse_quote!(Some(#field_ident.buffer.i));
                                parse_quote!(let #field_ident = result.named_buffer(#ident_str, #provided_type_ident);)
                            } else if attr.tts.to_string().contains("shared") {
                                output = parse_quote!(Some(#field_ident.i));
                                parse_quote!(let #field_ident = result.shared_buffer(#ident_str, EncodingType::ByteSlices(stride)).any();)
                            } else if let Some(t) = parse_type(attr.tts.to_string()) {
                                output = parse_quote!(Some(#field_ident.buffer.i));
                                parse_quote!(let #field_ident = result.named_buffer(#ident_str, #t);)
                            } else {
                                output = parse_quote!(Some(#field_ident.i));
                                create_buffer(&field_ident, &field_type)
                            };
                            result_type.push(field_type);
                            result.push(field_ident.clone());
                            new_buffers.push(new_buffer);
                        } else {
                            if field_type == string {
                                fn_inputs.push(parse_quote!(#field_ident: &str));
                                new_buffers.push(parse_quote!(let #field_ident = #field_ident.to_string();));
                            } else {
                                fn_inputs.push(parse_quote!(#field_ident: #field_type));
                            }
                        }
                        struct_args.push(parse_quote!(#field_ident));
                    }

                    let item = parse_quote! {
                        pub fn #ident_snake ( &mut self, #(#fn_inputs),* , result: &mut QueryExecutor)
                                                                                 -> (#(#result_type),*) {
                            #(#new_buffers)*

                            if let Some(output) = #output {
                                while self.buffer_to_operation.len() <= output {
                                    self.buffer_to_operation.push(None);
                                }
                                self.buffer_to_operation[output] = Some(self.operations.len());
                            }

                            self.operations.push(#enum_ident::#variant_ident { #(#struct_args),* });

                            (#(#result),*)
                        }
                    };
                    productions.push(item);
                } else {
                    let mut fn_inputs = Vec::<FnArg>::new();
                    let mut struct_args = Vec::<FieldValue>::new();
                    let mut fn_generics = Vec::<GenericArgument>::new();
                    for field in fields.named.into_iter() {
                        let field_ident = field.ident.clone().unwrap();
                        if let Some(attr) = field.attrs.iter().find(|attr| attr.path == parse_quote!(output)) {
                            println!("{}", attr.tts.to_string());
                        }
                        if field.ty == boxed {
                            let type_ident = Ident::new(&format!("_T{}", fn_generics.len()), Span::call_site());
                            fn_generics.push(parse_quote!(#type_ident: Into<#enum_ident>));
                            fn_inputs.push(parse_quote!(#field_ident: #type_ident));
                            struct_args.push(parse_quote!(#field_ident: Box::new(#field_ident.into())));
                        } else if field.ty == string {
                            fn_inputs.push(parse_quote!(#field_ident: &str));
                            struct_args.push(parse_quote!(#field_ident: #field_ident.to_string()));
                        } else {
                            let field_type = field.ty;
                            fn_inputs.push(parse_quote!(#field_ident: #field_type));
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
        }
        let expanded = quote! {
            impl QueryPlanner {
                #(#productions)*
            }

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

fn create_buffer(field_ident: &Ident, field_type: &Type) -> Stmt {
    let field_name = LitStr::new(&format!("{}", field_ident), Span::call_site());
    if *field_type == parse_quote!(BufferRef<u8>) {
        parse_quote!(let #field_ident = result.buffer_u8(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<&'static str>) {
        parse_quote!(let #field_ident = result.buffer_str2(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<usize>) {
        parse_quote!(let #field_ident = result.buffer_usize(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<i64>) {
        parse_quote!(let #field_ident = result.buffer_i64(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<i64>>) {
        parse_quote!(let #field_ident = result.buffer_scalar_i64(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<String>>) {
        parse_quote!(let #field_ident = result.buffer_scalar_string(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<&'static str>>) {
        parse_quote!(let #field_ident = result.buffer_scalar_str(#field_name);)
    } else {
        field_ident.span().unstable().error(format!("{} has unknown buffer type {:?}", field_ident, field_type)).emit();
        return parse_quote!(let #field_ident = #field_ident;);
    }
}


fn parse_type(type_def: String) -> Option<Expr> {
    lazy_static! {
        // E.g. `data` in `( t = "data.nullable" )`
        static ref T_NULLABLE: Regex = Regex::new(r#"t = "(.*)\.nullable""#).unwrap();
        static ref T: Regex = Regex::new(r#"t = "(.*)""#).unwrap();
    }
    if let Some(t) = T_NULLABLE.captures(&type_def) {
        let ident = Ident::new(t.get(1).unwrap().as_str(), Span::call_site());
        Some(parse_quote!(#ident.tag.nullable()))
    } else if let Some(t) = T.captures(&type_def) {
        let ident = Ident::new(t.get(1).unwrap().as_str(), Span::call_site());
        Some(parse_quote!(#ident.tag))
    } else {
        None
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
