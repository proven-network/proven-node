//! Procedural macros for proven-logger test support

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Test attribute that automatically captures logs and displays them on test failure.
///
/// This works like `#[test]` but with automatic log capture:
/// - Logs are captured during test execution
/// - If the test passes, logs are discarded
/// - If the test fails, logs are printed to help debug
///
/// # Example
///
/// ```no_run
/// use proven_logger_macros::logged_test;
///
/// #[logged_test]
/// fn test_something() {
///     proven_logger::info!("This will only show if the test fails");
///     assert_eq!(2 + 2, 4);
/// }
///
/// #[logged_test]
/// async fn test_async() {
///     proven_logger::debug!("Async test with logging");
///     assert!(true);
/// }
/// ```
#[proc_macro_attribute]
pub fn logged_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let test_name = &input.sig.ident;
    let test_name_str = test_name.to_string();
    let is_async = input.sig.asyncness.is_some();
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    // Extract any existing test attributes (like #[tokio::test])
    let (test_attrs, other_attrs): (Vec<_>, Vec<_>) = attrs.iter().partition(|attr| {
        attr.path().is_ident("test")
            || attr
                .path()
                .segments
                .last()
                .map(|s| s.ident == "test")
                .unwrap_or(false)
    });

    // If no test attribute exists, add the appropriate one
    let test_attr = if test_attrs.is_empty() {
        if is_async {
            quote! { #[tokio::test] }
        } else {
            quote! { #[test] }
        }
    } else {
        quote! { #(#test_attrs)* }
    };

    let result = if is_async {
        quote! {
            #test_attr
            #(#other_attrs)*
            #vis #sig {
                use ::proven_logger::test_support::TestLogGuard;
                use ::futures::FutureExt as _;

                let test_name = #test_name_str;
                let mut guard = TestLogGuard::new(test_name);

                // Wrap the async body to handle panics properly
                let result = ::std::panic::AssertUnwindSafe(async move {
                    #body
                })
                .catch_unwind()
                .await;

                match result {
                    Ok(value) => {
                        guard.passed();
                        value
                    }
                    Err(e) => {
                        // Drop guard to print logs before resuming panic
                        drop(guard);
                        ::std::panic::resume_unwind(e);
                    }
                }
            }
        }
    } else {
        quote! {
            #test_attr
            #(#other_attrs)*
            #vis #sig {
                let mut _guard = ::proven_logger::test_support::TestLogGuard::new(#test_name_str);

                let _result = ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| {
                    #body
                }));

                if _result.is_ok() {
                    _guard.passed();
                }

                if let Err(e) = _result {
                    ::std::panic::resume_unwind(e);
                }
            }
        }
    };

    TokenStream::from(result)
}

/// Test attribute for async tests using the tokio runtime.
///
/// This is a convenience wrapper that combines `#[tokio::test]` with `#[logged_test]`.
///
/// # Example
///
/// ```no_run
/// use proven_logger_macros::logged_tokio_test;
///
/// #[logged_tokio_test]
/// async fn test_async_operation() {
///     proven_logger::info!("Testing async operation");
///     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
///     assert!(true);
/// }
/// ```
#[proc_macro_attribute]
pub fn logged_tokio_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let test_name = &input.sig.ident;
    let test_name_str = test_name.to_string();
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let result = quote! {
        #[tokio::test]
        #(#attrs)*
        #vis #sig {
            use ::proven_logger::test_support::TestLogGuard;
            use ::futures::FutureExt as _;

            let test_name = #test_name_str;
            let mut guard = TestLogGuard::new(test_name);

            // Wrap the async body to handle panics properly
            let result = ::std::panic::AssertUnwindSafe(async move {
                #body
            })
            .catch_unwind()
            .await;

            match result {
                Ok(value) => {
                    guard.passed();
                    value
                }
                Err(e) => {
                    // Drop guard to print logs before resuming panic
                    drop(guard);
                    ::std::panic::resume_unwind(e);
                }
            }
        }
    };

    TokenStream::from(result)
}
