use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

include!("src/import_replacements.rs");

fn check_output_files_exist() -> bool {
    // Check if all expected JS extension files exist
    let expected_files = [
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

    expected_files.iter().all(|file| Path::new(file).exists())
}

fn main() {
    // Tell Cargo when to rerun - only when these specific files change
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=package-lock.json");
    println!("cargo:rerun-if-changed=src/import_replacements.rs");

    // Only rerun for TS files in extensions
    println!("cargo:rerun-if-changed=src/extensions/**/*.ts");

    // Add this to prevent rerunning for other files
    println!("cargo:rerun-if-changed=build.rs");

    // Tell Cargo to only rerun when explicitly tracked files change
    println!("cargo:rerun-if-env-changed=CARGO_FORCE_JS_REBUILD");

    // Check if all the output files exist
    let output_files_exist = check_output_files_exist();
    let force_rebuild = env::var("CARGO_FORCE_JS_REBUILD").is_ok();

    if !output_files_exist || force_rebuild {
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
    } else {
        println!("cargo:warning=Skipping JS build as all output files already exist");
    }
}
