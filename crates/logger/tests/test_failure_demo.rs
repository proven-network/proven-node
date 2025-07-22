//! Demonstrates log output on test failure

#[cfg(feature = "test-support")]
mod tests {
    use proven_logger::*;

    #[proven_logger::logged_test]
    fn test_that_will_fail() {
        info!("Starting test execution");
        debug!("Setting up test data");

        let x = 5;
        let y = 10;
        info!("x = {x}, y = {y}");

        warn!("About to perform assertion");
        assert_eq!(x + y, 20); // This will fail!
    }
}
