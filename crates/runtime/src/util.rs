pub fn run_in_thread<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(f).join().unwrap()
}
