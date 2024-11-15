use std::collections::HashMap;
use std::sync::LazyLock;

static VENDOR_REPLACEMENTS: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut map = HashMap::with_capacity(1);
    map.insert(
        "@radixdlt/babylon-gateway-api-sdk".to_string(),
        "proven:radixdlt_babylon_gateway_api".to_string(),
    );
    map
});

pub fn replace_vendor_imports(module_source: String) -> String {
    let mut result = module_source.to_string();

    for (vendor, replacement) in VENDOR_REPLACEMENTS.iter() {
        result = result.replace(vendor, replacement);
    }

    result
}
