//! Functionality for setting up Linux namespaces.

/// Available Linux namespaces for isolation.
#[derive(Debug, Clone, Copy)]
pub enum IsolationNamespaces {
    /// IPC namespace - isolates System V IPC objects and POSIX message queues.
    Ipc,

    /// Mount namespace - isolates mount points.
    Mount,

    /// Network namespace - isolates network stack.
    Network,

    /// Process ID namespace - isolates process IDs.
    Pid,

    /// User namespace - isolates user and group IDs.
    User,

    /// UTS namespace - isolates hostname and domain name.
    Uts,
}

/// Options for namespace configuration
#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
pub struct NamespaceOptions {
    /// Whether to use an IPC namespace
    pub use_ipc: bool,

    /// Whether to use a mount namespace
    pub use_mount: bool,

    /// Whether to use a network namespace
    pub use_network: bool,

    /// Whether to use a PID namespace
    pub use_pid: bool,

    /// Whether to use a user namespace
    pub use_user: bool,

    /// Whether to use a UTS namespace
    pub use_uts: bool,
}

impl Default for NamespaceOptions {
    fn default() -> Self {
        Self {
            use_ipc: true,
            use_mount: true,
            use_network: true,
            use_pid: true,
            use_user: true,
            use_uts: true,
        }
    }
}

impl NamespaceOptions {
    /// Creates a new `NamespaceOptions` with all namespaces enabled.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Creates a new `NamespaceOptions` with no namespaces enabled.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            use_ipc: false,
            use_mount: false,
            use_network: false,
            use_pid: false,
            use_user: false,
            use_uts: false,
        }
    }

    /// Converts the options to unshare command-line arguments
    #[must_use]
    pub fn to_unshare_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        if self.use_ipc {
            args.push("--ipc".to_string());
        }

        if self.use_mount {
            args.push("--mount".to_string());
        }

        if self.use_network {
            args.push("--net".to_string());
        }

        if self.use_pid {
            args.push("--pid".to_string());
            args.push("--fork".to_string());
        }

        if self.use_user {
            args.push("--user".to_string());
            args.push("--map-root-user".to_string());
        }

        if self.use_uts {
            args.push("--uts".to_string());
        }

        args
    }
}
