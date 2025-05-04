use std::sync::Arc;

use crate::file_system::StoredEntry;
use crate::{ExecutionRequest, ExecutionResult, ModuleLoader, Pool, PoolOptions, RpcEndpoints};

use super::Result;

use async_trait::async_trait;
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store, Store2, Store3};

/// Options for configuring a `RuntimePoolManager`.
pub struct RuntimePoolManagerOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    RNV: RadixNftVerifier,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

    /// Store used for file-system virtualisation.
    pub file_system_store: FSS,

    /// Max pool workers.
    pub max_workers: u32,

    /// NFT-scoped SQL store.
    pub nft_sql_store: NSS,

    /// NFT-scoped KV store.
    pub nft_store: NS,

    /// Persona-scoped SQL store.
    pub personal_sql_store: PSS,

    /// Persona-scoped KV store.
    pub personal_store: PS,

    /// Verifier for checking NFT ownership on the Radix Network.
    pub radix_nft_verifier: RNV,

    /// RPC endpoints for the runtime.
    pub rpc_endpoints: RpcEndpoints,
}

/// Trait for managing a pool.
#[async_trait]
pub trait RuntimePoolManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Application-scoped SQL store.
    type ApplicationSqlStore: SqlStore2;

    /// Application-scoped KV store.
    type ApplicationStore: Store2;

    /// Store used for file-system virtualisation.
    type FileSystemStore: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >;

    /// NFT-scoped SQL store.
    type NftSqlStore: SqlStore3;

    /// NFT-scoped KV store.
    type NftStore: Store3;

    /// Persona-scoped SQL store.
    type PersonalSqlStore: SqlStore3;

    /// Persona-scoped KV store.
    type PersonalStore: Store3;

    /// Radix NFT verifier.
    type RadixNftVerifier: RadixNftVerifier;

    /// Create a new pool manager.
    async fn new(
        applications_store: RuntimePoolManagerOptions<
            Self::ApplicationStore,
            Self::PersonalStore,
            Self::NftStore,
            Self::ApplicationSqlStore,
            Self::PersonalSqlStore,
            Self::NftSqlStore,
            Self::FileSystemStore,
            Self::RadixNftVerifier,
        >,
    ) -> Self;

    /// Execute a request.
    async fn execute(
        &self,
        module_loader: ModuleLoader,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult>;

    /// Execute a prehashed request.
    async fn execute_prehashed(
        &self,
        code_package_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult>;
}

/// Manages database of all currently deployed applications.
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct RuntimePoolManager<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    RNV: RadixNftVerifier,
{
    pool: Arc<Pool<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>>,
}

#[async_trait]
impl<AS, PS, NS, ASS, PSS, NSS, FSS, RNV> RuntimePoolManagement
    for RuntimePoolManager<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
    FSS: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    RNV: RadixNftVerifier,
{
    type ApplicationSqlStore = ASS;
    type ApplicationStore = AS;
    type FileSystemStore = FSS;
    type NftSqlStore = NSS;
    type NftStore = NS;
    type PersonalSqlStore = PSS;
    type PersonalStore = PS;
    type RadixNftVerifier = RNV;

    async fn new(
        RuntimePoolManagerOptions {
            application_sql_store,
            application_store,
            file_system_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_nft_verifier,
            rpc_endpoints,
        }: RuntimePoolManagerOptions<AS, PS, NS, ASS, PSS, NSS, FSS, RNV>,
    ) -> Self {
        let pool = Pool::new(PoolOptions {
            application_sql_store,
            application_store,
            file_system_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_nft_verifier,
            rpc_endpoints,
        })
        .await;

        Self { pool }
    }

    async fn execute(
        &self,
        module_loader: ModuleLoader,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        self.pool.clone().execute(module_loader, request).await
    }

    async fn execute_prehashed(
        &self,
        code_package_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        self.pool
            .clone()
            .execute_prehashed(code_package_hash, request)
            .await
    }
}
