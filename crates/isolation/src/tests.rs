//! Tests for the isolation functionality.

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::{
        Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager,
        NamespaceOptions, Result,
    };

    struct TestApp {
        name: String,
        executable: String,
        args: Vec<String>,
        working_dir: Option<PathBuf>,
        memory_mb: usize,
    }

    impl TestApp {
        fn new(name: &str, executable: &str, args: Vec<String>) -> Self {
            Self {
                name: name.to_string(),
                executable: executable.to_string(),
                args,
                working_dir: None,
                memory_mb: 100, // Default memory limit
            }
        }

        fn with_working_dir<P: AsRef<Path>>(mut self, working_dir: P) -> Self {
            self.working_dir = Some(working_dir.as_ref().to_path_buf());
            self
        }

        #[cfg(target_os = "linux")]
        fn with_memory_limit(mut self, memory_mb: usize) -> Self {
            self.memory_mb = memory_mb;
            self
        }
    }

    #[async_trait]
    impl IsolatedApplication for TestApp {
        fn args(&self) -> Vec<String> {
            self.args.clone()
        }

        fn executable(&self) -> &str {
            &self.executable
        }

        fn memory_limit_mb(&self) -> usize {
            self.memory_mb
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn namespace_options(&self) -> NamespaceOptions {
            // For testing, we use a minimal set of namespaces
            let mut options = NamespaceOptions::none();
            options.use_uts = true; // UTS namespace is lightweight
            options
        }

        fn working_dir(&self) -> Option<PathBuf> {
            self.working_dir.clone()
        }

        // Add custom is_ready_check implementation for tests
        async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool> {
            // For test processes, we'll just assume they're ready
            Ok(true)
        }

        // Override to speed up tests
        fn is_ready_check_interval_ms(&self) -> u64 {
            100 // Much faster checks for tests
        }

        // Set a maximum number of checks for tests to prevent hanging
        fn is_ready_check_max(&self) -> Option<u32> {
            Some(5) // Maximum 5 checks for tests
        }
    }

    #[tokio::test]
    async fn test_basic_isolation() -> Result<()> {
        // Simple echo test
        let app = TestApp::new(
            "echo",
            "echo",
            vec!["Hello from isolated process".to_string()],
        );
        let isolation_manager = IsolationManager::new();

        let (process, _join_handle) = isolation_manager.spawn(app).await?;
        assert!(process.pid() > 0);

        // Echo should exit quickly
        let status = process.wait().await?;
        assert!(status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_hostname_isolation() -> Result<()> {
        // Test that hostname is isolated
        let app = TestApp::new("hostname", "hostname", vec![]);

        // Create an isolation manager with only UTS namespace enabled
        let mut options = NamespaceOptions::none();
        options.use_uts = true;

        let isolation_config = IsolationConfig {
            use_user_namespace: false,
            use_pid_namespace: false,
            use_network_namespace: false,
            use_mount_namespace: false,
            use_uts_namespace: true,
            use_ipc_namespace: false,
            use_chroot: false,
            use_memory_limits: false,
        };

        let isolation_manager = IsolationManager::with_config(isolation_config);

        let (process, _join_handle) = isolation_manager.spawn(app).await?;
        assert!(process.pid() > 0);

        // Hostname command should succeed
        let status = process.wait().await?;
        assert!(status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_env_vars() -> Result<()> {
        // Test that environment variables are passed correctly
        let app = TestApp::new("env", "env", vec![]);

        let isolation_manager = IsolationManager::new();

        let (process, _join_handle) = isolation_manager.spawn(app).await?;
        assert!(process.pid() > 0);

        // env command should succeed
        let status = process.wait().await?;
        assert!(status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_with_working_dir() -> Result<()> {
        // Create a temporary directory
        let temp_dir =
            TempDir::new().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

        // Test with working directory
        let app = TestApp::new("pwd", "pwd", vec![]).with_working_dir(temp_dir.path());

        let isolation_manager = IsolationManager::new();

        let (process, _join_handle) = isolation_manager.spawn(app).await?;
        assert!(process.pid() > 0);

        // pwd command should succeed
        let status = process.wait().await?;
        assert!(status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown() -> Result<()> {
        // Test shutdown of a long-running process
        let app = TestApp::new("sleep", "sleep", vec!["10".to_string()]);

        let isolation_manager = IsolationManager::new();

        let (process, _join_handle) = isolation_manager.spawn(app).await?;
        assert!(process.pid() > 0);

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Shutdown the process
        process.shutdown().await?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_memory_limits() -> Result<()> {
        // Create a test application with memory limits
        let app = TestApp::new(
            "memory-test",
            "/bin/bash",
            vec!["-c".to_string(), "sleep 2".to_string()],
        )
        .with_memory_limit(100); // 100MB limit

        // Create an isolation manager that uses memory limits
        let config = IsolationConfig {
            use_user_namespace: true,
            use_pid_namespace: true,
            use_network_namespace: true,
            use_mount_namespace: true,
            use_uts_namespace: true,
            use_ipc_namespace: true,
            use_chroot: false,
            use_memory_limits: true,
        };

        let manager = IsolationManager::with_config(config);

        // Spawn the process
        let process = manager.spawn(app).await?;

        // Check that memory control is active (may not be if cgroups v2 not available)
        if process.has_memory_control() {
            // Read the current memory usage
            let usage = process.current_memory_usage_mb()?;
            assert!(
                usage.is_some(),
                "Memory usage should be available when memory control is active"
            );

            println!("Current memory usage: {:?} MB", usage);
        } else {
            println!("Memory control not active, skipping memory usage checks");
        }

        // Shut down the process
        process.shutdown().await?;

        Ok(())
    }
}
