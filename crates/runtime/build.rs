use std::process::Command;

fn main() {
    // Run npm install
    let status = Command::new("npm")
        .arg("install")
        .status()
        .expect("Failed to run npm install");

    if !status.success() {
        panic!("npm install failed");
    }

    // Tell Cargo to rerun if package.json changes
    println!("cargo:rerun-if-changed=package.json");
}
