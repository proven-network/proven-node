#[cfg(target_os = "linux")]
mod linux;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("This application is only supported on Linux.");
}

#[cfg(target_os = "linux")]
#[tokio::main(worker_threads = 12)]
async fn main() {
    let _ = linux::main().await;
}
