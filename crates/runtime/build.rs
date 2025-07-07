use glob::glob;
use std::env;
use std::fs;
use std::process::Command;
use std::time::SystemTime;

include!("src/import_replacements.rs");

fn get_newest_source_time() -> Option<SystemTime> {
    // Find the newest modification time among all source files that could affect the build
    let mut newest_time = None;

    // Check TypeScript source files
    if let Ok(entries) = glob::glob("src/extensions/**/*.ts") {
        for entry in entries.flatten() {
            if let Ok(metadata) = fs::metadata(&entry) {
                if let Ok(modified) = metadata.modified() {
                    if newest_time.is_none_or(|current| modified > current) {
                        newest_time = Some(modified);
                    }
                }
            }
        }
    }

    // Check package files
    for file in &[
        "package.json",
        "package-lock.json",
        "tsconfig.json",
        "vite.config.ts",
    ] {
        if let Ok(metadata) = fs::metadata(file) {
            if let Ok(modified) = metadata.modified() {
                if newest_time.is_none_or(|current| modified > current) {
                    newest_time = Some(modified);
                }
            }
        }
    }

    newest_time
}

fn get_oldest_output_time() -> Option<SystemTime> {
    let output_files = [
        "src/extensions/console/console.js",
        "src/extensions/crypto/crypto.js",
        "src/extensions/handler/handler-options.js",
        "src/extensions/handler/handler-runtime.js",
        "src/extensions/kv/kv-options.js",
        "src/extensions/kv/kv-runtime.js",
        "src/extensions/session/session.js",
        "src/extensions/sql/sql-options.js",
        "src/extensions/sql/sql-runtime.js",
    ];

    let mut oldest_time = None;

    for file in &output_files {
        if let Ok(metadata) = fs::metadata(file) {
            if let Ok(modified) = metadata.modified() {
                if oldest_time.is_none_or(|current| modified < current) {
                    oldest_time = Some(modified);
                }
            }
        } else {
            // File doesn't exist, definitely need to rebuild
            return None;
        }
    }

    oldest_time
}

fn need_import_replacement() -> bool {
    let output_files = [
        "src/extensions/console/console.js",
        "src/extensions/crypto/crypto.js",
        "src/extensions/handler/handler-options.js",
        "src/extensions/handler/handler-runtime.js",
        "src/extensions/kv/kv-options.js",
        "src/extensions/kv/kv-runtime.js",
        "src/extensions/session/session.js",
        "src/extensions/sql/sql-options.js",
        "src/extensions/sql/sql-runtime.js",
    ];

    for file_path in output_files {
        if let Ok(content) = fs::read_to_string(file_path) {
            // Check if file contains any of the old import patterns
            if content.contains("@proven-network/") {
                println!("cargo:warning=Import replacement needed in {}", file_path);
                return true;
            }
        }
    }

    false
}

fn need_rebuild() -> bool {
    let force_rebuild = env::var("CARGO_FORCE_JS_REBUILD").is_ok();
    if force_rebuild {
        println!("cargo:warning=Force rebuild requested via CARGO_FORCE_JS_REBUILD");
        return true;
    }

    // Check if import replacement is needed
    if need_import_replacement() {
        return true;
    }

    let newest_source = get_newest_source_time();
    let oldest_output = get_oldest_output_time();

    match (newest_source, oldest_output) {
        (Some(source_time), Some(output_time)) => {
            let needs_rebuild = source_time > output_time;
            if needs_rebuild {
                println!("cargo:warning=Rebuild needed: source files are newer than outputs");
            }
            needs_rebuild
        }
        (Some(_), None) => {
            println!("cargo:warning=Rebuild needed: output files missing");
            true
        }
        (None, _) => {
            println!("cargo:warning=No source files found, skipping rebuild");
            false
        }
    }
}

fn main() {
    // Tell Cargo when to rerun - be more specific about dependencies
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=package-lock.json");
    println!("cargo:rerun-if-changed=tsconfig.json");
    println!("cargo:rerun-if-changed=vite.config.ts");
    println!("cargo:rerun-if-changed=src/import_replacements.rs");
    println!("cargo:rerun-if-changed=build.rs");

    // Track all TypeScript source files individually for more precise rebuilds
    if let Ok(entries) = glob("src/extensions/**/*.ts") {
        for entry in entries.flatten() {
            println!("cargo:rerun-if-changed={}", entry.display());
        }
    }

    // Tell Cargo to only rerun when explicitly tracked files change
    println!("cargo:rerun-if-env-changed=CARGO_FORCE_JS_REBUILD");

    // Use the new timestamp-based rebuild logic
    if need_rebuild() {
        // Run npm build to compile TypeScript extensions
        let npm_build_status = Command::new("npm")
            .arg("run")
            .arg("build")
            .status()
            .expect("Failed to run npm run build");

        if !npm_build_status.success() {
            panic!("npm run build failed");
        }

        // Do ESM replacements on extension files to convert @proven-network/* imports
        [
            "src/extensions/console/console.js",
            "src/extensions/crypto/crypto.js",
            "src/extensions/handler/handler-options.js",
            "src/extensions/handler/handler-runtime.js",
            "src/extensions/kv/kv-options.js",
            "src/extensions/kv/kv-runtime.js",
            "src/extensions/session/session.js",
            "src/extensions/sql/sql-options.js",
            "src/extensions/sql/sql-runtime.js",
        ]
        .iter()
        .for_each(|path| {
            let content = fs::read_to_string(path).expect("Failed to read file");
            let replaced = replace_esm_imports(&content);
            fs::write(path, replaced).expect("Failed to write file");
        });

        // Write a build cache file with current timestamp
        let build_info = format!(
            "built_at: {}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        fs::write(".build_cache", build_info).ok(); // Ignore errors

        println!("cargo:warning=TypeScript build and import replacement completed");
    } else {
        println!("cargo:warning=Skipping JS build - outputs are up to date");
    }
}
