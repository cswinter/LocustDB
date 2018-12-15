use super::proc_macro::TokenStream;
use syn::*;
use proc_macro2::Span;

use regex::Regex;


pub fn ast_builder(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);


    if let Data::Enum(DataEnum { variants, .. }) = input.data {
        let enum_ident = input.ident;
        let mut productions = Vec::<Item>::new();
        let string: Type = parse_quote!(String);
        for (index, variant) in variants.into_iter().enumerate() {
            if let Fields::Named(fields) = variant.fields {
                let variant_ident = variant.ident.clone();
                let ident_snake = studley_to_snake(variant.ident);
                let mut fn_inputs = Vec::<FnArg>::new();
                let mut new_buffers = Vec::<Stmt>::new();
                let mut struct_args = Vec::<FieldValue>::new();
                let mut result = Vec::<Ident>::new();
                let mut cache_retrieve = Vec::<Expr>::new();
                let mut result_type = Vec::<Type>::new();
                let mut output: Expr = parse_quote!(None);
                let mut hashes = Vec::<Stmt>::new();
                let mut output_index = 0;
                for field in fields.named.into_iter() {
                    let field_ident = field.ident.clone().unwrap();
                    let field_type = field.ty;
                    let ident_str = LitStr::new(&format!("{}", field_ident), Span::call_site());
                    if let Some(attr) = field.attrs.iter().find(|attr| attr.path == parse_quote!(internal)) {
                        let new_buffer = if let Some((t, fn_arg)) = parse_type(&field_ident, attr.tts.to_string()) {
                            assert!(fn_arg.is_none(), "Can't provide internal type ({}).", field_ident);
                            parse_quote!(let #field_ident = self.buffer_provider.named_buffer(#ident_str, #t);)
                        } else {
                            create_buffer(&field_ident, &field_type)
                        };
                        new_buffers.push(new_buffer);
                    } else if let Some(attr) = field.attrs.iter().find(|attr| attr.path == parse_quote!(output)) {
                        let new_buffer: Stmt = if attr.tts.to_string().contains("shared_byte_slices") {
                            output = parse_quote!(Some(#field_ident.i));
                            parse_quote!(let #field_ident = self.buffer_provider.shared_buffer(#ident_str, EncodingType::ByteSlices(stride)).any();)
                        } else if attr.tts.to_string().contains("shared_val_rows") {
                            output = parse_quote!(Some(#field_ident.i));
                            parse_quote!(let #field_ident = self.buffer_provider.shared_buffer(#ident_str, EncodingType::ValRows).val_rows().unwrap();)
                        } else if let Some((t, fn_input)) = parse_type(&field_ident, attr.tts.to_string()) {
                            if let Some(fn_input) = fn_input {
                                fn_inputs.push(fn_input);
                            }
                            output = parse_quote!(Some(#field_ident.buffer.i));
                            parse_quote!(let #field_ident = self.buffer_provider.named_buffer(#ident_str, #t);)
                        } else {
                            output = parse_quote!(Some(#field_ident.i));
                            create_buffer(&field_ident, &field_type)
                        };
                        let index_lit = LitInt::new(output_index as u64, IntSuffix::Usize, Span::call_site());
                        if attr.tts.to_string().contains("shared_byte_slices") {
                            cache_retrieve.push(parse_quote!(buffer[#index_lit].any()));
                        } else if attr.tts.to_string().contains("shared_val_rows") {
                            cache_retrieve.push(parse_quote!(buffer[#index_lit].val_rows().unwrap()));
                        } else {
                            cache_retrieve.push(convert(parse_quote!(buffer[#index_lit]), &field_type));
                        }
                        result_type.push(field_type);
                        result.push(field_ident.clone());
                        new_buffers.push(new_buffer);
                        output_index += 1;
                    } else {
                        if field_type == string {
                            fn_inputs.push(parse_quote!(#field_ident: &str));
                            new_buffers.push(parse_quote!(let #field_ident = #field_ident.to_string();));
                        } else {
                            fn_inputs.push(parse_quote!(#field_ident: #field_type));
                        }
                        if field.attrs.iter().find(|attr| attr.path == parse_quote!(nohash)).is_none() {
                            hashes.push(hash(&field_ident, &field_type));
                        }
                    }
                    struct_args.push(parse_quote!(#field_ident));
                }

                let index = LitInt::new(index as u64, IntSuffix::U64, Span::call_site());
                let result2 = result.clone();
                let item = parse_quote! {
                        pub fn #ident_snake(&mut self, #(#fn_inputs),*) -> (#(#result_type),*) {

                            use crypto::digest::Digest;
                            use crypto::md5::Md5;
                            let mut signature = [0u8; 16];
                            let mut hasher = Md5::new();
                            if self.enable_common_subexpression_elimination() {
                                hasher.input(&#index.to_ne_bytes());
                                #(#hashes)*
                                hasher.result(&mut signature);
                                if let Some(buffer) = self.cache.get(&signature) {
                                    return (#(#cache_retrieve),*)
                                }
                            }

                            #(#new_buffers)*

                            if let Some(output) = #output {
                                while self.buffer_to_operation.len() <= output {
                                    self.buffer_to_operation.push(None);
                                }
                                self.buffer_to_operation[output] = Some(self.operations.len());
                            }

                            self.operations.push(#enum_ident::#variant_ident { #(#struct_args),* });

                            if self.enable_common_subexpression_elimination() {
                                self.cache.insert(signature, vec![#(#result2.into()),*]);
                            }

                            (#(#result),*)
                        }
                    };
                productions.push(item);
            }
        }
        let expanded = quote! {
            impl QueryPlanner {
                #(#productions)*
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
        parse_quote!(let #field_ident = self.buffer_provider.buffer_u8(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<&'static str>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_str(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Val<'static>>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_val(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<ValRows<'static>>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_val_rows(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<usize>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_usize(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<i64>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_i64(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<u32>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_u32(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<MergeOp>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_merge_op(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Premerge>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_premerge(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<i64>>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_scalar_i64(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<String>>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_scalar_string(#field_name);)
    } else if *field_type == parse_quote!(BufferRef<Scalar<&'static str>>) {
        parse_quote!(let #field_ident = self.buffer_provider.buffer_scalar_str(#field_name);)
    } else {
        field_ident.span().unstable().error(format!("{} has unknown buffer type {:?}", field_ident, field_type)).emit();
        parse_quote!(let #field_ident = #field_ident;)
    }
}

fn convert(expr: Expr, field_type: &Type) -> Expr {
    if *field_type == parse_quote!(BufferRef<u8>) {
        parse_quote!(#expr.u8().unwrap())
    } else if *field_type == parse_quote!(BufferRef<&'static str>) {
        parse_quote!(#expr.str().unwrap())
    } else if *field_type == parse_quote!(BufferRef<Val<'static>>) {
        parse_quote!(#expr.val().unwrap())
    } else if *field_type == parse_quote!(BufferRef<ValRows<'static>>) {
        parse_quote!(#expr.val_rows().unwrap())
    } else if *field_type == parse_quote!(BufferRef<usize>) {
        parse_quote!(#expr.usize().unwrap())
    } else if *field_type == parse_quote!(BufferRef<i64>) {
        parse_quote!(#expr.i64().unwrap())
    } else if *field_type == parse_quote!(BufferRef<u32>) {
        parse_quote!(#expr.u32().unwrap())
    } else if *field_type == parse_quote!(BufferRef<MergeOp>) {
        parse_quote!(#expr.merge_op().unwrap())
    } else if *field_type == parse_quote!(BufferRef<Premerge>) {
        parse_quote!(#expr.premerge().unwrap())
    } else if *field_type == parse_quote!(BufferRef<Scalar<i64>>) {
        parse_quote!(#expr.scalar_i64().unwrap())
    } else if *field_type == parse_quote!(BufferRef<Scalar<String>>) {
        parse_quote!(#expr.scalar_string().unwrap())
    } else if *field_type == parse_quote!(BufferRef<Scalar<&'static str>>) {
        parse_quote!(#expr.scalar_str().unwrap())
    } else {
        expr
    }
}

fn hash(field_ident: &Ident, field_type: &Type) -> Stmt {
    if *field_type == parse_quote!(String) {
        parse_quote!(hasher.input_str(#field_ident);)
    } else if *field_type == parse_quote!(usize) || *field_type == parse_quote!(i64) {
        parse_quote!(hasher.input(&#field_ident.to_ne_bytes());)
    } else if *field_type == parse_quote!(u8) {
        parse_quote!(hasher.input(&[#field_ident]);)
    } else if *field_type == parse_quote!(bool) {
        parse_quote!(hasher.input(&[#field_ident as u8]);)
    } else if *field_type == parse_quote!(Aggregator) {
        parse_quote!(hasher.input(&[#field_ident as u8]);)
    } else if *field_type == parse_quote!(TypedBufferRef) {
        parse_quote!(hasher.input(&#field_ident.buffer.i.to_ne_bytes());)
    } else {
        parse_quote!(hasher.input(&#field_ident.i.to_ne_bytes());)
    }
}

fn parse_type(field_ident: &Ident, type_def: String) -> Option<(Expr, Option<FnArg>)> {
    lazy_static! {
        // E.g. `data` in `( t = "data.nullable" )`
        static ref T: Regex = Regex::new(r#"t = "(.*)""#).unwrap();
        static ref BASE: Regex = Regex::new(r#"base=([^;]*)"#).unwrap();
        static ref NULL: Regex = Regex::new(r#"null=([^;]*)"#).unwrap();

    }

    if let Some(t) = T.captures(&type_def) {
        let t = t.get(1).unwrap().as_str();

        let base = BASE.captures(t)
            .expect(&format!("No `base` specified for {}", field_ident))
            .get(1).unwrap().as_str();
        let mut fn_input = None;
        let base_type: Expr = if base == "provided" {
            let provided_type_ident = Ident::new(&format!("{}_type", field_ident), Span::call_site());
            fn_input = Some(parse_quote!(#provided_type_ident: EncodingType));
            parse_quote!(#provided_type_ident)
        } else if base == "i64" {
            parse_quote!(EncodingType::I64)
        } else if base == "u8" {
            parse_quote!(EncodingType::U8)
        } else if base == "str" {
            parse_quote!(EncodingType::Str)
        } else {
            let ident = Ident::new(base, Span::call_site());
            parse_quote!(#ident.tag)
        };

        let null_adjusted_type = match NULL.captures(t) {
            Some(null) => {
                let null = null.get(1).unwrap().as_str();
                if null == "_always" {
                    parse_quote!(#base_type.nullable())
                } else if null == "_never" {
                    parse_quote!(#base_type.non_nullable())
                } else if null == "_fused" {
                    parse_quote!(#base_type.nullable_fused())
                } else {
                    let parents = null.split(",").map(|ident| Ident::new(ident, Span::call_site())).collect::<Vec<_>>();
                    parse_quote! {
                        if #(#parents.is_nullable())||* { #base_type.nullable() } else { #base_type }
                    }
                }
            }
            None => base_type,
        };

        Some((null_adjusted_type, fn_input))
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
