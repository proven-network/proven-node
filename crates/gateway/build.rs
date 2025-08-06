use std::process::Command;

fn main() {
    // Tell Cargo when to rerun
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=vite.config.ts");
    println!("cargo:rerun-if-changed=static/**/*.ts");
    println!("cargo:rerun-if-changed=../../packages/common/src/**/*.ts");

    // Run npm install
    let npm_install_status = Command::new("npm")
        .arg("install")
        .status()
        .expect("Failed to run npm install");

    if !npm_install_status.success() {
        panic!("npm install failed");
    }

    // Run npm build
    let npm_install_status = Command::new("npm")
        .arg("run")
        .arg("build")
        .status()
        .expect("Failed to run npm run build");

    if !npm_install_status.success() {
        panic!("npm run build failed");
    }
}
