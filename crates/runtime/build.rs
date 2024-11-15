use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

use regex::Regex;

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

fn copy_and_clean_zod() -> std::io::Result<()> {
    let source_path = "node_modules/zod/lib/index.mjs";
    let target_dir = Path::new("vendor/zod");
    let target_path = target_dir.join("index.mjs");

    // Create vendor/zod directory
    create_dir_all(target_dir)?;

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

    // Copy and clean Zod
    copy_and_clean_zod().expect("Failed to process Zod files");

    // Tell Cargo when to rerun
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=rollup.config.js");
}
