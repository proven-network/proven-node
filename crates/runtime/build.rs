use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

use regex::Regex;

fn strip_comments(content: &str) -> String {
    let single_line = Regex::new(r"//.*").unwrap();
    let multi_line = Regex::new(r"/\*[\s\S]*?\*/").unwrap();

    let without_single = single_line.replace_all(content, "");
    let without_multi = multi_line.replace_all(&without_single, "");

    // Remove empty lines
    without_multi
        .lines()
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

fn main() {
    // Run npm install
    let npm_status = Command::new("npm")
        .arg("install")
        .status()
        .expect("Failed to run npm install");

    if !npm_status.success() {
        panic!("npm install failed");
    }

    // Run rollup bundle
    let rollup_status = Command::new("npm")
        .arg("run")
        .arg("bundle:uuid")
        .status()
        .expect("Failed to run rollup");

    if !rollup_status.success() {
        panic!("rollup bundling failed");
    }

    // Copy and clean Zod
    copy_and_clean_zod().expect("Failed to process Zod files");

    // Tell Cargo when to rerun
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=rollup.config.js");
}
