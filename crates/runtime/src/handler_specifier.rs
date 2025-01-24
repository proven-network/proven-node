use deno_core::url::{ParseError, Url};
use deno_core::ModuleSpecifier;

/// A specifier for a handler - specifies both entrypoint module and handler name (in fragment).
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct HandlerSpecifier(Url);

impl HandlerSpecifier {
    /// Creates a new `HandlerSpecifier` from the given `Url`.
    #[must_use]
    pub const fn new(url: Url) -> Self {
        Self(url)
    }

    /// Returns the handler specifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Parses a `HandlerSpecifier` from a string.
    ///
    /// # Errors
    ///
    /// This function will return an error if the provided string is not a valid URL.
    pub fn parse(specifier: &str) -> Result<Self, ParseError> {
        Ok(Self(Url::parse(specifier)?))
    }

    /// Returns the handler name as an `Option<String>`.
    ///
    /// If the URL contains a fragment, it will be returned as a `String`.
    /// Otherwise, `None` will be returned.
    ///
    /// The fragment should be empty for default exports.
    pub fn handler_name(&self) -> Option<String> {
        self.0.fragment().map(String::from)
    }

    #[must_use]
    /// Returns the bare module specifier without the handler name info.
    pub fn module_specifier(&self) -> ModuleSpecifier {
        let mut module_specifier = self.0.clone();
        module_specifier.set_fragment(None);

        module_specifier
    }
}
