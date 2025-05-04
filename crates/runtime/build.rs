use std::env;
use std::fs::{self, create_dir_all};
use std::path::Path;
use std::process::Command;

use serde::Deserialize;

include!("src/import_replacements.rs");

#[derive(Deserialize)]
struct PackageJson {
    main: Option<String>,
    module: Option<String>,
}

fn strip_comments(content: &str) -> String {
    // Process single line comments line by line
    content
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

fn retain_ascii(content: &str) -> String {
    content.chars().filter(|c| c.is_ascii()).collect()
}

fn copy_and_clean_package(package_name: &str) -> std::io::Result<()> {
    // Read package.json
    let package_json_path = format!("node_modules/{}/package.json", package_name);
    let package_json_content = fs::read_to_string(package_json_path)?;
    let package_json: PackageJson = serde_json::from_str(&package_json_content)?;

    // Get source path from module field, fallback to main field, then fallback to lib/index.mjs
    let source_path = format!(
        "node_modules/{}/{}",
        package_name,
        package_json
            .module
            .or(package_json.main)
            .unwrap_or_else(|| "lib/index.mjs".to_string())
    );

    let target_dir = Path::new("vendor").join(package_name);
    let target_path = target_dir.join("index.mjs");

    // Create vendor/<package> directory
    create_dir_all(&target_dir)?;

    // Clean comments and remove non-acsii characters
    let content = fs::read_to_string(source_path)?;
    let content = strip_comments(&content);
    let content = retain_ascii(&content);
    fs::write(target_path, content)?;

    Ok(())
}

fn clean_vendor_file(package_name: &str) -> std::io::Result<()> {
    let file_path = format!("vendor/{}/index.mjs", package_name);

    // Remove imports, clean comments and remove non-acsii characters
    let content = fs::read_to_string(&file_path)?
        .lines()
        .map(|line| {
            if line.trim().starts_with("import ") {
                ""
            } else {
                line
            }
        })
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<&str>>()
        .join("\n");
    let content = strip_comments(&content);
    let content = retain_ascii(&content);

    // Write back
    fs::write(file_path, content)?;

    Ok(())
}

fn check_output_files_exist() -> bool {
    // Check if all expected output files exist
    let expected_files = [
        "src/extensions/kv/kv-options.js",
        "src/extensions/kv/kv-runtime.js",
        "vendor/openai/index.mjs",
        "vendor/uuid/index.mjs",
        "vendor/zod/index.mjs",
        "vendor/@radixdlt/radix-engine-toolkit/index.mjs",
    ];

    expected_files.iter().all(|file| Path::new(file).exists())
}

fn main() {
    // Tell Cargo when to rerun - only when these specific files change
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=package-lock.json");
    println!("cargo:rerun-if-changed=rollup.config.js");
    println!("cargo:rerun-if-changed=src/import_replacements.rs");

    // Only rerun for TS files, not for any file in src
    println!("cargo:rerun-if-changed=src/**/*.ts");

    // Also track the vendor directory files
    println!("cargo:rerun-if-changed=vendor/openai/index.mjs");
    println!("cargo:rerun-if-changed=vendor/uuid/index.mjs");
    println!("cargo:rerun-if-changed=vendor/zod/index.mjs");
    println!("cargo:rerun-if-changed=vendor/@radixdlt/radix-engine-toolkit/index.mjs");

    // Add this to prevent rerunning for other files
    println!("cargo:rerun-if-changed=build.rs");

    // Tell Cargo to only rerun when explicitly tracked files change
    println!("cargo:rerun-if-env-changed=CARGO_FORCE_JS_REBUILD");

    // Check if all the output files exist and if npm dependencies are already installed
    let output_files_exist = check_output_files_exist();
    let node_modules_exists = Path::new("node_modules").exists();
    let force_rebuild = env::var("CARGO_FORCE_JS_REBUILD").is_ok();

    if !output_files_exist || !node_modules_exists || force_rebuild {
        // Create vendor directory if it doesn't exist
        if !Path::new("vendor").exists() {
            create_dir_all("vendor").expect("Failed to create vendor directory");
        }

        // Run npm install
        let npm_install_status = Command::new("npm")
            .arg("install")
            .status()
            .expect("Failed to run npm install");

        if !npm_install_status.success() {
            panic!("npm install failed");
        }

        // Run npm build
        let npm_build_status = Command::new("npm")
            .arg("run")
            .arg("build")
            .status()
            .expect("Failed to run npm run build");

        if !npm_build_status.success() {
            panic!("npm run build failed");
        }

        // Do ESM replacements on extensions
        [
            "src/extensions/kv/kv-options.js",
            "src/extensions/kv/kv-runtime.js",
        ]
        .iter()
        .for_each(|path| {
            let content = fs::read_to_string(path).expect("Failed to read file");
            let replaced = replace_esm_imports(&content);
            fs::write(path, replaced).expect("Failed to write file");
        });

        // Rollup deps
        let rollup_status = Command::new("npm")
            .arg("run")
            .arg("bundle")
            .status()
            .expect("Failed to run rollup");

        if !rollup_status.success() {
            panic!("rollup bundling failed");
        }
        clean_vendor_file("openai").expect("Failed to clean openai bundle");
        clean_vendor_file("uuid").expect("Failed to clean uuid bundle");

        // Copy and clean other packages
        copy_and_clean_package("zod").expect("Failed to process zod files");
        copy_and_clean_package("@radixdlt/radix-engine-toolkit")
            .expect("Failed to process @radixdlt/radix-engine-toolkit files");
    } else {
        println!("cargo:warning=Skipping JS build as all output files already exist");
    }
}
