use std::process::Command;

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

    // Tell Cargo when to rerun
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=rollup.config.js");
}
