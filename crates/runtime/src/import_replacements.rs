use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

static IMPORT_REPLACEMENTS: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut map = HashMap::with_capacity(1);

    // First-party packages
    map.insert(
        "@proven-network/crypto".to_string(),
        "proven:crypto".to_string(),
    );

    map.insert(
        "@proven-network/handler".to_string(),
        "proven:handler".to_string(),
    );

    map.insert("@proven-network/kv".to_string(), "proven:kv".to_string());

    map.insert(
        "@proven-network/session".to_string(),
        "proven:session".to_string(),
    );

    map.insert("@proven-network/sql".to_string(), "proven:sql".to_string());

    // Vendor packages

    map.insert(
        "@radixdlt/babylon-gateway-api-sdk".to_string(),
        "proven:radixdlt_babylon_gateway_api".to_string(),
    );

    map.insert(
        "@radixdlt/radix-engine-toolkit".to_string(),
        "proven:radixdlt_radix_engine_toolkit".to_string(),
    );

    map.insert("openai".to_string(), "proven:openai".to_string());

    map.insert("uuid".to_string(), "proven:uuid".to_string());

    map.insert("zod".to_string(), "proven:zod".to_string());

    map
});

pub fn replace_esm_imports(module_source: &str) -> String {
    let import_regex = Regex::new(
        r#"((import|export)(?:[\s\n]+(?:type\s+)?[^"']+from\s+)?[\s\n]*["'])([^"']+)(["'])"#,
    )
    .unwrap();

    import_regex
        .replace_all(module_source, |caps: &regex::Captures| {
            let pre_path = &caps[1];
            let path = &caps[3];
            let post_path = &caps[4];

            let new_path = IMPORT_REPLACEMENTS
                .iter()
                .find(|(vendor, _)| path.starts_with(*vendor))
                .map_or_else(
                    || path.to_string(),
                    |(vendor, replacement)| path.replace(vendor, replacement),
                );

            format!("{pre_path}{new_path}{post_path}")
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_esm_imports() {
        let input = r#"
            import { v4 } from "uuid";
            import { Gateway } from "@radixdlt/babylon-gateway-api-sdk";
            const uuid = "uuid"; // Should not replace this
            export { something } from "uuid/v4";
        "#;

        let output = replace_esm_imports(input);

        assert!(output.contains(r#"from "proven:uuid""#));
        assert!(output.contains(r#"from "proven:radixdlt_babylon_gateway_api""#));
        assert!(output.contains(r#"const uuid = "uuid""#)); // Unchanged
        assert!(output.contains(r#"from "proven:uuid/v4""#));
    }

    #[test]
    fn test_handle_package_name_appearing_in_renames() {
        let input = r#"import { v4 as uuidv4 } from "uuid";"#;

        let output = replace_esm_imports(input);

        assert_eq!(output, r#"import { v4 as uuidv4 } from "proven:uuid";"#);
    }
}
