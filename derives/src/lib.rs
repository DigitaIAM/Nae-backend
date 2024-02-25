use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ImplBytes)]
pub fn impl_bytes(input: TokenStream) -> TokenStream {
  let DeriveInput { ident, .. } = parse_macro_input!(input);
  let output = quote! {
      impl ToBytes for #ident {
          fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
              serde_json::to_string(self)
                  .map(|s| s.as_bytes().to_vec())
                  .map_err(|_| format!("fail to encode #ident {:?}", self).into())
          }
      }

      impl FromBytes<Self> for #ident {
          fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
              serde_json::from_slice(bs)
                  .map_err(|_| "fail to decode #ident".into())
          }
      }
  };
  output.into()
}

#[proc_macro_derive(ImplID)]
pub fn impl_id(input: TokenStream) -> TokenStream {
  let DeriveInput { ident, .. } = parse_macro_input!(input);
  let output = quote! {
      impl From<#ident> for ID {
          fn from(f: #ident) -> Self {
              f.0
          }
      }

      impl From<ID> for #ident {
          fn from(id: ID) -> Self {
              #ident(id)
          }
      }

      impl From<#ident> for Value {
          fn from(f: #ident) -> Self {
              Value::ID(f.0)
          }
      }

  };
  output.into()
}
