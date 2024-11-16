use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize)]
struct PackageJson {
    module: Option<String>,
}

fn strip_comments(content: &str) -> String {
    // Handle multiline comments first
    let multi_line = Regex::new(r"/\*[\s\S]*?\*/").unwrap();
    let without_multi = multi_line.replace_all(content, "");

    // Process single line comments line by line
    without_multi
        .lines()
        .map(|line| {
            if line.trim().starts_with("//") {
                ""
            } else {
                line
            }
        })
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<&str>>()
        .join("\n")
}

fn copy_and_clean_package(package_name: &str) -> std::io::Result<()> {
    // Read package.json
    let package_json_path = format!("node_modules/{}/package.json", package_name);
    let package_json_content = fs::read_to_string(package_json_path)?;
    let package_json: PackageJson = serde_json::from_str(&package_json_content)?;

    // Get source path from module field, fallback to lib/index.mjs
    let source_path = format!(
        "node_modules/{}/{}",
        package_name,
        package_json
            .module
            .unwrap_or_else(|| "lib/index.mjs".to_string())
    );

    let target_dir = Path::new("vendor").join(package_name);
    let target_path = target_dir.join("index.mjs");

    // Create vendor/<package> directory
    create_dir_all(&target_dir)?;

    // Read, clean and write
    let content = fs::read_to_string(source_path)?;
    let cleaned = strip_comments(&content);
    fs::write(target_path, cleaned)?;

    Ok(())
}

fn clean_vendor_file(package_name: &str) -> std::io::Result<()> {
    let file_path = format!("vendor/{}/index.mjs", package_name);

    // Read the rolled up content
    let content = fs::read_to_string(&file_path)?;

    // Clean comments and remove non-acsii characters
    let cleaned = strip_comments(&content).replace("…", "...");

    // Write back
    fs::write(file_path, cleaned)?;

    Ok(())
}

fn main() {
    // Run npm install
    let npm_status = Command::new("npm")
        .arg("install")
        .status()
        .expect("Failed to run npm install");

    if !npm_status.success() {
        panic!("npm install failed");
    }

    // Rollup uuid
    let uuid_rollup_status = Command::new("npm")
        .arg("run")
        .arg("bundle:uuid")
        .status()
        .expect("Failed to run rollup for uuid");

    if !uuid_rollup_status.success() {
        panic!("rollup bundling of uuid failed");
    }
    clean_vendor_file("uuid").expect("Failed to clean uuid bundle");

    // Rollup openai
    let openai_rollup_status = Command::new("npm")
        .arg("run")
        .arg("bundle:openai")
        .status()
        .expect("Failed to run rollup for openai");

    if !openai_rollup_status.success() {
        panic!("rollup bundling of openai failed");
    }
    clean_vendor_file("openai").expect("Failed to clean openai bundle");

    // Copy and clean other packages
    copy_and_clean_package("zod").expect("Failed to process zod files");
    copy_and_clean_package("@radixdlt/babylon-gateway-api-sdk")
        .expect("Failed to process @radixdlt/babylon-gateway-api-sdk files");
    copy_and_clean_package("@radixdlt/radix-engine-toolkit")
        .expect("Failed to process @radixdlt/radix-engine-toolkit files");

    // Tell Cargo when to rerun
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=rollup.config.js");
}
