use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

static VENDOR_REPLACEMENTS: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut map = HashMap::with_capacity(1);

    map.insert(
        "@radixdlt/babylon-gateway-api-sdk".to_string(),
        "proven:radixdlt_babylon_gateway_api".to_string(),
    );

    map.insert("uuid".to_string(), "proven:uuid".to_string());

    map
});

pub fn replace_vendor_imports(module_source: String) -> String {
    let import_regex = Regex::new(
        r#"((import|export)(?:[\s\n]+(?:type\s+)?[^"']+from\s+)?[\s\n]*["'])([^"']+)(["'])"#,
    )
    .unwrap();

    import_regex
        .replace_all(&module_source, |caps: &regex::Captures| {
            let pre_path = &caps[1];
            let path = &caps[3];
            let post_path = &caps[4];

            let new_path = VENDOR_REPLACEMENTS
                .iter()
                .find(|(vendor, _)| path.starts_with(*vendor))
                .map(|(vendor, replacement)| path.replace(vendor, replacement))
                .unwrap_or(path.to_string());

            format!("{}{}{}", pre_path, new_path, post_path)
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_vendor_imports() {
        let input = r#"
            import { v4 } from "uuid";
            import { Gateway } from "@radixdlt/babylon-gateway-api-sdk";
            const uuid = "uuid"; // Should not replace this
            export { something } from "uuid/v4";
        "#;

        let output = replace_vendor_imports(input.to_string());

        assert!(output.contains(r#"from "proven:uuid""#));
        assert!(output.contains(r#"from "proven:radixdlt_babylon_gateway_api""#));
        assert!(output.contains(r#"const uuid = "uuid""#)); // Unchanged
        assert!(output.contains(r#"from "proven:uuid/v4""#));
    }

    #[test]
    fn test_handle_package_name_appearing_in_renames() {
        let input = r#"import { v4 as uuidv4 } from "uuid";"#;

        let output = replace_vendor_imports(input.to_string());

        assert_eq!(output, r#"import { v4 as uuidv4 } from "proven:uuid";"#);
    }
}
