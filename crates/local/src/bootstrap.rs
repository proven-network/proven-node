//! Bootstrap module for local node initialization.
//!
//! This module contains the main Bootstrap struct and orchestrates
//! the initialization of all services in the proper order.

pub mod step_01_network_cluster;
pub mod step_02_postgres;
pub mod step_03_bitcoin_mainnet_node;
pub mod step_04_bitcoin_testnet_node;
pub mod step_05_ethereum_mainnet_node;
pub mod step_06_ethereum_holesky_node;
pub mod step_07_ethereum_sepolia_node;
pub mod step_08_radix_mainnet_node;
pub mod step_09_radix_stokenet_node;
pub mod step_10_upgrade_core;

use super::error::Error;
use crate::NodeConfig;

use std::net::IpAddr;

use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_core::Core;
use proven_http_insecure::InsecureHttpServer;
use proven_logger::{error, info};
use proven_topology::Node;
use proven_topology::TopologyAdaptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use url::Url;

/// Bootstrap struct for local node initialization.
///
/// This struct contains the configuration for the node and the state of the node.
/// It also contains the network, governance, and other services that are needed for the node.
///
/// The bootstrap struct is used to initialize the node and start the services.
/// It is also used to shutdown the node and clean up the services.
pub struct Bootstrap<G: TopologyAdaptor> {
    config: NodeConfig<G>,

    attestor: MockAttestor,
    external_ip: IpAddr,
    node: Option<Node>,

    // Shared context fields that steps need access to
    postgres_ip_address: Option<IpAddr>,
    postgres_port: Option<u16>,

    // RPC endpoints that steps create and core needs
    bitcoin_mainnet_node_rpc_endpoint: Url,
    bitcoin_testnet_node_rpc_endpoint: Url,
    ethereum_mainnet_rpc_endpoint: Url,
    ethereum_holesky_rpc_endpoint: Url,
    ethereum_sepolia_rpc_endpoint: Url,
    radix_mainnet_rpc_endpoint: Url,
    radix_stokenet_rpc_endpoint: Url,

    // Special case - gets upgraded at end of bootstrap
    bootstrapping_core: Option<Core<MockAttestor, G, InsecureHttpServer>>,

    // Engine client for use in store creation
    engine_client: Option<
        proven_engine::Client<
            proven_transport_ws::WebsocketTransport<G, MockAttestor>,
            G,
            proven_storage_rocksdb::RocksDbStorage,
        >,
    >,

    // Collection of all bootable services
    bootables: Vec<Box<dyn Bootable>>,

    // State
    started: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<G: TopologyAdaptor> Bootstrap<G> {
    /// Add a bootable service to the collection
    pub fn add_bootable(&mut self, bootable: Box<dyn Bootable>) {
        self.bootables.push(bootable);
    }

    /// Take bootables for the supervisor (moves ownership)
    fn take_bootables_for_supervisor(&mut self) -> Vec<Box<dyn Bootable>> {
        std::mem::take(&mut self.bootables)
    }

    /// Clean up services if initialization fails
    #[allow(clippy::cognitive_complexity)]
    async fn cleanup_on_init_failure(&mut self) {
        info!("Cleaning up services due to initialization failure...");

        // Shutdown all bootable services in reverse order (LIFO)
        while let Some(bootable) = self.bootables.pop() {
            if let Err(e) = bootable.shutdown().await {
                error!("Error shutting down service during cleanup: {e:?}");
            }
        }

        // Special case: shutdown light_core if it exists
        if let Some(light_core) = self.bootstrapping_core.take()
            && let Err(e) = light_core.shutdown().await
        {
            error!("Error shutting down light_core during cleanup: {e:?}");
        }

        info!("Cleanup complete");
    }

    /// Create a new bootstrap instance with the given configuration.
    ///
    /// This function fetches the external IP address and creates a new bootstrap instance.
    /// It also initializes the network, governance, and other services that are needed for the node.
    ///
    /// # Errors
    ///
    /// This function returns an error if the external IP address cannot be fetched.
    pub async fn new(config: NodeConfig<G>) -> Result<Self, Error> {
        let external_ip = crate::net::fetch_external_ip().await?;

        Ok(Self {
            config: config.clone(),

            attestor: MockAttestor::new(),
            external_ip,
            node: None,

            postgres_ip_address: None,
            postgres_port: None,

            bitcoin_mainnet_node_rpc_endpoint: config.bitcoin_mainnet_fallback_rpc_endpoint.clone(),
            bitcoin_testnet_node_rpc_endpoint: config.bitcoin_testnet_fallback_rpc_endpoint.clone(),
            ethereum_mainnet_rpc_endpoint: config.ethereum_mainnet_fallback_rpc_endpoint.clone(),
            ethereum_holesky_rpc_endpoint: config.ethereum_holesky_fallback_rpc_endpoint.clone(),
            ethereum_sepolia_rpc_endpoint: config.ethereum_sepolia_fallback_rpc_endpoint.clone(),
            radix_mainnet_rpc_endpoint: config.radix_mainnet_fallback_rpc_endpoint.clone(),
            radix_stokenet_rpc_endpoint: config.radix_stokenet_fallback_rpc_endpoint.clone(),

            bootstrapping_core: None,
            engine_client: None,

            bootables: Vec::new(),

            started: false,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    /// Initialize the node.
    ///
    /// This function executes all bootstrap steps in order.
    /// It also sets up the service supervisor.
    ///
    /// # Errors
    ///
    /// This function returns an error if the node is already started or if a bootstrap step fails.
    #[allow(clippy::cognitive_complexity)]
    pub async fn initialize(
        mut self,
        shutdown_token: CancellationToken,
    ) -> Result<(Self, TaskTracker), Error> {
        if self.started {
            return Err(Error::AlreadyStarted);
        }

        self.started = true;

        // Use the provided shutdown token
        self.shutdown_token = shutdown_token;

        // Execute all bootstrap steps in order
        macro_rules! execute_step {
            ($step_name:expr, $step_fn:expr) => {
                // Check if shutdown was requested before starting this step
                if self.shutdown_token.is_cancelled() {
                    info!(
                        "Shutdown requested before starting {}, cleaning up...",
                        $step_name
                    );
                    self.cleanup_on_init_failure().await;
                    return Err(Error::Shutdown);
                }

                info!("Starting {}", $step_name);
                if let Err(e) = $step_fn(&mut self).await {
                    error!("failed to start {}: {:?}", $step_name, e);
                    self.cleanup_on_init_failure().await;
                    return Err(e);
                }
            };
        }

        execute_step!("network topology", step_01_network_cluster::execute);
        execute_step!("postgres", step_02_postgres::execute);
        execute_step!(
            "bitcoin mainnet node",
            step_03_bitcoin_mainnet_node::execute
        );
        execute_step!(
            "bitcoin testnet node",
            step_04_bitcoin_testnet_node::execute
        );
        execute_step!(
            "ethereum mainnet nodes",
            step_05_ethereum_mainnet_node::execute
        );
        execute_step!(
            "ethereum holesky nodes",
            step_06_ethereum_holesky_node::execute
        );
        execute_step!(
            "ethereum sepolia nodes",
            step_07_ethereum_sepolia_node::execute
        );
        execute_step!("radix mainnet node", step_08_radix_mainnet_node::execute);
        execute_step!("radix stokenet node", step_09_radix_stokenet_node::execute);
        execute_step!("core", step_10_upgrade_core::execute);

        info!(
            "All bootstrap steps completed successfully. Started {} bootable services.",
            self.bootables.len()
        );

        // Set up supervisor for all bootable services
        self.setup_service_supervisor();

        // Return both bootstrap and task_tracker so main can wait for supervisor
        let task_tracker = std::mem::replace(&mut self.task_tracker, TaskTracker::new());
        Ok((self, task_tracker))
    }

    /// Set up supervisor for all bootable services
    fn setup_service_supervisor(&mut self) {
        let shutdown_token = self.shutdown_token.clone();
        let bootables_for_supervisor = self.take_bootables_for_supervisor();

        self.task_tracker.spawn(async move {
            if bootables_for_supervisor.is_empty() {
                // No bootable services to supervise, just wait for shutdown signal
                shutdown_token.cancelled().await;
                info!("shutdown signal received");
                return;
            }

            // Create a future that waits for any service to exit unexpectedly
            let monitor_services = async {
                let mut futures = Vec::new();
                for bootable in &bootables_for_supervisor {
                    let service_name = bootable.bootable_name().to_string();
                    let future = Box::pin(async move {
                        bootable.wait().await;
                        error!("Bootable service '{service_name}' exited unexpectedly");
                    });
                    futures.push(future);
                }

                // Wait for the first service to exit
                if !futures.is_empty() {
                    futures::future::select_all(futures).await;
                }
            };

            // Wait for either a service to exit or shutdown signal
            let shutdown_reason = tokio::select! {
                () = monitor_services => {
                    // Cancel the shutdown token so main knows to exit
                    shutdown_token.cancel();
                    "service exited unexpectedly"
                }
                () = shutdown_token.cancelled() => {
                    "shutdown signal received"
                }
            };

            info!("{shutdown_reason}, supervisor shutting down services...");

            // Shutdown all bootable services in reverse order (LIFO)
            let mut services = bootables_for_supervisor;
            while let Some(bootable) = services.pop() {
                if let Err(e) = bootable.shutdown().await {
                    error!("Error shutting down service: {e:?}");
                }
            }

            info!("supervisor shutdown complete");
        });

        // Close the task tracker so it doesn't wait for other tasks
        self.task_tracker.close();
    }
}
