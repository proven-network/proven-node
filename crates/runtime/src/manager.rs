use std::sync::Arc;

use crate::{ExecutionRequest, ExecutionResult, Pool, PoolOptions, PoolRuntimeOptions};

use super::Result;

use async_trait::async_trait;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use radix_common::network::NetworkDefinition;

/// Options for configuring a `RuntimePoolManager`.
pub struct RuntimePoolManagerOptions<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    /// Application-scoped SQL store.
    pub application_sql_store: ASS,

    /// Application-scoped KV store.
    pub application_store: AS,

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

    /// Origin for Radix Network gateway.
    pub radix_gateway_origin: String,

    /// Network definition for Radix Network.
    pub radix_network_definition: NetworkDefinition,
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

    /// NFT-scoped SQL store.
    type NftSqlStore: SqlStore3;

    /// NFT-scoped KV store.
    type NftStore: Store3;

    /// Persona-scoped SQL store.
    type PersonalSqlStore: SqlStore3;

    /// Persona-scoped KV store.
    type PersonalStore: Store3;

    /// Create a new pool manager.
    async fn new(
        applications_store: RuntimePoolManagerOptions<
            Self::ApplicationStore,
            Self::PersonalStore,
            Self::NftStore,
            Self::ApplicationSqlStore,
            Self::PersonalSqlStore,
            Self::NftSqlStore,
        >,
    ) -> Self;

    /// Execute a request.
    async fn execute(
        &self,
        runtime_options: PoolRuntimeOptions,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult>;

    /// Execute a prehashed request.
    async fn execute_prehashed(
        &self,
        options_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult>;
}

/// Manages database of all currently deployed applications.
#[derive(Clone)]
pub struct RuntimePoolManager<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    pool: Arc<Pool<AS, PS, NS, ASS, PSS, NSS>>,
}

#[async_trait]
impl<AS, PS, NS, ASS, PSS, NSS> RuntimePoolManagement
    for RuntimePoolManager<AS, PS, NS, ASS, PSS, NSS>
where
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    type ApplicationSqlStore = ASS;
    type ApplicationStore = AS;
    type NftSqlStore = NSS;
    type NftStore = NS;
    type PersonalSqlStore = PSS;
    type PersonalStore = PS;

    async fn new(
        RuntimePoolManagerOptions {
            application_sql_store,
            application_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin,
            radix_network_definition,
        }: RuntimePoolManagerOptions<AS, PS, NS, ASS, PSS, NSS>,
    ) -> Self {
        let pool = Pool::new(PoolOptions {
            application_sql_store,
            application_store,
            max_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin,
            radix_network_definition,
        })
        .await;

        Self { pool }
    }

    async fn execute(
        &self,
        runtime_options: PoolRuntimeOptions,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        self.pool.clone().execute(runtime_options, request).await
    }

    async fn execute_prehashed(
        &self,
        options_hash: String,
        request: ExecutionRequest,
    ) -> Result<ExecutionResult> {
        self.pool
            .clone()
            .execute_prehashed(options_hash, request)
            .await
    }
}
