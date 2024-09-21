//! [`LowLevelClient`](struct.LowLevelClient.html) is the main entry point for this library.
//!
//! Library created with [`libninja`](https://www.libninja.com).
#![allow(non_camel_case_types)]
#![allow(unused)]
pub mod model;
pub mod request;
pub use httpclient::{Error, Result, InMemoryResponseExt};
use std::sync::{Arc, OnceLock};
use std::borrow::Cow;
use crate::generated::model::*;
mod serde;
static SHARED_HTTPCLIENT: OnceLock<httpclient::Client> = OnceLock::new();
pub fn default_http_client() -> httpclient::Client {
    httpclient::Client::new()
        .base_url(
            std::env::var("LOW_LEVEL_BASE_URL")
                .expect("Missing environment variable LOW_LEVEL_BASE_URL")
                .as_str(),
        )
}
/// Use this method if you want to add custom middleware to the httpclient.
/// It must be called before any requests are made, otherwise it will have no effect.
/// Example usage:
///
/// ```
/// init_http_client(default_http_client()
///     .with_middleware(..)
/// );
/// ```
pub fn init_http_client(init: httpclient::Client) {
    let _ = SHARED_HTTPCLIENT.set(init);
}
fn shared_http_client() -> Cow<'static, httpclient::Client> {
    Cow::Borrowed(SHARED_HTTPCLIENT.get_or_init(default_http_client))
}
#[derive(Clone)]
pub struct FluentRequest<'a, T> {
    pub(crate) client: &'a LowLevelClient,
    pub params: T,
}
#[derive(Default, Debug)]
pub struct LowLevelClient {
    client: Cow<'static, httpclient::Client>,
}
impl LowLevelClient {
    pub fn from_env() -> Self {
        Self {
            client: shared_http_client(),
        }
    }
    pub fn new() -> Self {
        Self {
            client: shared_http_client(),
        }
    }
    pub fn new_with(client: httpclient::Client) -> Self {
        Self {
            client: Cow::Owned(client),
        }
    }
}
impl LowLevelClient {
    /**Get Gateway Status

Returns the Gateway API version and current ledger state.*/
    pub fn gateway_status(&self) -> FluentRequest<'_, request::GatewayStatusRequest> {
        FluentRequest {
            client: self,
            params: request::GatewayStatusRequest {},
        }
    }
    /**Get Network Configuration

Returns network identifier, network name and well-known network addresses.*/
    pub fn network_configuration(
        &self,
    ) -> FluentRequest<'_, request::NetworkConfigurationRequest> {
        FluentRequest {
            client: self,
            params: request::NetworkConfigurationRequest {
            },
        }
    }
    /**Get Construction Metadata

Returns information needed to construct a new transaction including current `epoch` number.*/
    pub fn transaction_construction(
        &self,
    ) -> FluentRequest<'_, request::TransactionConstructionRequest> {
        FluentRequest {
            client: self,
            params: request::TransactionConstructionRequest {
            },
        }
    }
    /**Preview Transaction

Previews transaction against the network.
This endpoint is effectively a proxy towards the Core API `/v0/transaction/preview` endpoint. See the Core API documentation for more details.*/
    pub fn transaction_preview(
        &self,
        args: request::TransactionPreviewRequired,
    ) -> FluentRequest<'_, request::TransactionPreviewRequest> {
        FluentRequest {
            client: self,
            params: request::TransactionPreviewRequest {
                blobs_hex: None,
                end_epoch_exclusive: args.end_epoch_exclusive,
                flags: args.flags,
                manifest: args.manifest.to_owned(),
                nonce: args.nonce,
                notary_is_signatory: None,
                notary_public_key: None,
                signer_public_keys: args.signer_public_keys,
                start_epoch_inclusive: args.start_epoch_inclusive,
                tip_percentage: args.tip_percentage,
            },
        }
    }
    /**Submit Transaction

Submits a signed transaction payload to the network.*/
    pub fn transaction_submit(
        &self,
        notarized_transaction_hex: &str,
    ) -> FluentRequest<'_, request::TransactionSubmitRequest> {
        FluentRequest {
            client: self,
            params: request::TransactionSubmitRequest {
                notarized_transaction_hex: notarized_transaction_hex.to_owned(),
            },
        }
    }
    /**Get Committed Transaction Details

Returns the committed details and receipt of the transaction for a given transaction identifier.
Transaction identifiers which don't correspond to a committed transaction will return a `TransactionNotFoundError`.*/
    pub fn transaction_committed_details(
        &self,
        intent_hash: &str,
        opt_ins: TransactionDetailsOptIns,
    ) -> FluentRequest<'_, request::TransactionCommittedDetailsRequest> {
        FluentRequest {
            client: self,
            params: request::TransactionCommittedDetailsRequest {
                at_ledger_state: None,
                intent_hash: intent_hash.to_owned(),
                opt_ins,
            },
        }
    }
    /**Get Transaction Status

Returns overall transaction status and all of its known payloads based on supplied intent hash.*/
    pub fn transaction_status(
        &self,
        intent_hash: &str,
    ) -> FluentRequest<'_, request::TransactionStatusRequest> {
        FluentRequest {
            client: self,
            params: request::TransactionStatusRequest {
                intent_hash: intent_hash.to_owned(),
            },
        }
    }
    /**PreValidate deposit of resources to an account

Helper endpoint that allows pre-validation if a deposit of certain resources to a given account can succeed or not. It is only meant for pre-validation usage, it does not guarantee that execution will succeed.*/
    pub fn account_deposit_pre_validation(
        &self,
        account_address: &str,
        badge: TransactionAccountDepositPreValidationAuthorizedDepositorBadge,
        resource_addresses: &[&str],
    ) -> FluentRequest<'_, request::AccountDepositPreValidationRequest> {
        FluentRequest {
            client: self,
            params: request::AccountDepositPreValidationRequest {
                account_address: account_address.to_owned(),
                badge,
                resource_addresses: resource_addresses
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
            },
        }
    }
    /**Get Transactions Stream

Returns transactions which have been committed to the ledger.
[Check detailed documentation for brief explanation](#section/Using-the-streamtransactions-endpoint)*/
    pub fn stream_transactions(
        &self,
        args: request::StreamTransactionsRequired,
    ) -> FluentRequest<'_, request::StreamTransactionsRequest> {
        FluentRequest {
            client: self,
            params: request::StreamTransactionsRequest {
                accounts_with_manifest_owner_method_calls: args
                    .accounts_with_manifest_owner_method_calls
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                accounts_without_manifest_owner_method_calls: args
                    .accounts_without_manifest_owner_method_calls
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                affected_global_entities_filter: args
                    .affected_global_entities_filter
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                at_ledger_state: None,
                cursor: None,
                events_filter: args.events_filter,
                from_ledger_state: None,
                kind_filter: args.kind_filter.to_owned(),
                limit_per_page: None,
                manifest_accounts_deposited_into_filter: args
                    .manifest_accounts_deposited_into_filter
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                manifest_accounts_withdrawn_from_filter: args
                    .manifest_accounts_withdrawn_from_filter
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                manifest_badges_presented_filter: args
                    .manifest_badges_presented_filter
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                manifest_class_filter: args.manifest_class_filter,
                manifest_resources_filter: args
                    .manifest_resources_filter
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                opt_ins: args.opt_ins,
                order: args.order.to_owned(),
            },
        }
    }
    /**Get Entity Details

Returns detailed information for collection of entities. Aggregate resources globally by default.*/
    pub fn state_entity_details(
        &self,
        addresses: &[&str],
        aggregation_level: ResourceAggregationLevel,
        opt_ins: StateEntityDetailsOptIns,
    ) -> FluentRequest<'_, request::StateEntityDetailsRequest> {
        FluentRequest {
            client: self,
            params: request::StateEntityDetailsRequest {
                addresses: addresses.iter().map(|&x| x.to_owned()).collect(),
                aggregation_level,
                at_ledger_state: None,
                opt_ins,
            },
        }
    }
    /**Get Entity Metadata Page

Returns all the metadata properties associated with a given global entity.
The returned response is in a paginated format, ordered by first appearance on the ledger.*/
    pub fn entity_metadata_page(
        &self,
        address: &str,
    ) -> FluentRequest<'_, request::EntityMetadataPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityMetadataPageRequest {
                address: address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
            },
        }
    }
    /**Get Entity Schema Page

Returns all the schemas associated with a given global entity.
The returned response is in a paginated format, ordered by first appearance on the ledger.*/
    pub fn entity_schema_page(
        &self,
        address: &str,
    ) -> FluentRequest<'_, request::EntitySchemaPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntitySchemaPageRequest {
                address: address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
            },
        }
    }
    /**Get page of Global Entity Fungible Resource Balances

Returns the total amount of each fungible resource owned by a given global entity.
Result can be aggregated globally or per vault.
The returned response is in a paginated format, ordered by the resource's first appearance on the ledger.*/
    pub fn entity_fungibles_page(
        &self,
        address: &str,
        aggregation_level: ResourceAggregationLevel,
        opt_ins: StateEntityFungiblesPageRequestOptIns,
    ) -> FluentRequest<'_, request::EntityFungiblesPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityFungiblesPageRequest {
                address: address.to_owned(),
                aggregation_level,
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                opt_ins,
            },
        }
    }
    /**Get page of Global Entity Fungible Resource Vaults

Returns vaults for fungible resource owned by a given global entity.
The returned response is in a paginated format, ordered by the resource's first appearance on the ledger.*/
    pub fn entity_fungible_resource_vault_page(
        &self,
        address: &str,
        resource_address: &str,
    ) -> FluentRequest<'_, request::EntityFungibleResourceVaultPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityFungibleResourceVaultPageRequest {
                address: address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                resource_address: resource_address.to_owned(),
            },
        }
    }
    /**Get page of Global Entity Non-Fungible Resource Balances

Returns the total amount of each non-fungible resource owned by a given global entity.
Result can be aggregated globally or per vault.
The returned response is in a paginated format, ordered by the resource's first appearance on the ledger.*/
    pub fn entity_non_fungibles_page(
        &self,
        address: &str,
        aggregation_level: ResourceAggregationLevel,
        opt_ins: StateEntityNonFungiblesPageRequestOptIns,
    ) -> FluentRequest<'_, request::EntityNonFungiblesPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityNonFungiblesPageRequest {
                address: address.to_owned(),
                aggregation_level,
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                opt_ins,
            },
        }
    }
    /**Get page of Global Entity Non-Fungible Resource Vaults

Returns vaults for non fungible resource owned by a given global entity.
The returned response is in a paginated format, ordered by the resource's first appearance on the ledger.*/
    pub fn entity_non_fungible_resource_vault_page(
        &self,
        address: &str,
        opt_ins: StateEntityNonFungibleResourceVaultsPageOptIns,
        resource_address: &str,
    ) -> FluentRequest<'_, request::EntityNonFungibleResourceVaultPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityNonFungibleResourceVaultPageRequest {
                address: address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                opt_ins,
                resource_address: resource_address.to_owned(),
            },
        }
    }
    /**Get page of Non-Fungibles in Vault

Returns all non-fungible IDs of a given non-fungible resource owned by a given entity.
The returned response is in a paginated format, ordered by the resource's first appearence on the ledger.*/
    pub fn entity_non_fungible_ids_page(
        &self,
        address: &str,
        resource_address: &str,
        vault_address: &str,
    ) -> FluentRequest<'_, request::EntityNonFungibleIdsPageRequest> {
        FluentRequest {
            client: self,
            params: request::EntityNonFungibleIdsPageRequest {
                address: address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                resource_address: resource_address.to_owned(),
                vault_address: vault_address.to_owned(),
            },
        }
    }
    /**Get page of Non-Fungible Ids in Resource Collection

Returns the non-fungible IDs of a given non-fungible resource.
Returned response is in a paginated format, ordered by their first appearance on the ledger.*/
    pub fn non_fungible_ids(
        &self,
        resource_address: &str,
    ) -> FluentRequest<'_, request::NonFungibleIdsRequest> {
        FluentRequest {
            client: self,
            params: request::NonFungibleIdsRequest {
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                resource_address: resource_address.to_owned(),
            },
        }
    }
    /**Get Non-Fungible Data

Returns data associated with a given non-fungible ID of a given non-fungible resource.*/
    pub fn non_fungible_data(
        &self,
        non_fungible_ids: &[&str],
        resource_address: &str,
    ) -> FluentRequest<'_, request::NonFungibleDataRequest> {
        FluentRequest {
            client: self,
            params: request::NonFungibleDataRequest {
                at_ledger_state: None,
                non_fungible_ids: non_fungible_ids
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                resource_address: resource_address.to_owned(),
            },
        }
    }
    /**Get Non-Fungible Location

Returns location of a given non-fungible ID.*/
    pub fn non_fungible_location(
        &self,
        non_fungible_ids: &[&str],
        resource_address: &str,
    ) -> FluentRequest<'_, request::NonFungibleLocationRequest> {
        FluentRequest {
            client: self,
            params: request::NonFungibleLocationRequest {
                at_ledger_state: None,
                non_fungible_ids: non_fungible_ids
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
                resource_address: resource_address.to_owned(),
            },
        }
    }
    /**Get KeyValueStore Keys

Allows to iterate over key value store keys.*/
    pub fn key_value_store_keys(
        &self,
        key_value_store_address: &str,
    ) -> FluentRequest<'_, request::KeyValueStoreKeysRequest> {
        FluentRequest {
            client: self,
            params: request::KeyValueStoreKeysRequest {
                at_ledger_state: None,
                cursor: None,
                key_value_store_address: key_value_store_address.to_owned(),
                limit_per_page: None,
            },
        }
    }
    /**Get KeyValueStore Data

Returns data (value) associated with a given key of a given key-value store.
[Check detailed documentation for explanation](#section/How-to-query-the-content-of-a-key-value-store-inside-a-component)*/
    pub fn key_value_store_data(
        &self,
        key_value_store_address: &str,
        keys: Vec<StateKeyValueStoreDataRequestKeyItem>,
    ) -> FluentRequest<'_, request::KeyValueStoreDataRequest> {
        FluentRequest {
            client: self,
            params: request::KeyValueStoreDataRequest {
                at_ledger_state: None,
                key_value_store_address: key_value_store_address.to_owned(),
                keys,
            },
        }
    }
    ///Get Validators List
    pub fn state_validators_list(
        &self,
    ) -> FluentRequest<'_, request::StateValidatorsListRequest> {
        FluentRequest {
            client: self,
            params: request::StateValidatorsListRequest {
                at_ledger_state: None,
                cursor: None,
            },
        }
    }
    /**Get Account resource preferences

Returns paginable collection of resource preference rules for given account.*/
    pub fn account_resource_preferences_page(
        &self,
        account_address: &str,
    ) -> FluentRequest<'_, request::AccountResourcePreferencesPageRequest> {
        FluentRequest {
            client: self,
            params: request::AccountResourcePreferencesPageRequest {
                account_address: account_address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
            },
        }
    }
    /**Get Account authorized depositors

Returns paginable collection of authorized depositors for given account.*/
    pub fn account_authorized_depositors_page(
        &self,
        account_address: &str,
    ) -> FluentRequest<'_, request::AccountAuthorizedDepositorsPageRequest> {
        FluentRequest {
            client: self,
            params: request::AccountAuthorizedDepositorsPageRequest {
                account_address: account_address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
            },
        }
    }
    /**Get Package Blueprints Page

Returns all the blueprints associated with a given package entity.
The returned response is in a paginated format, ordered by first appearance on the ledger.*/
    pub fn package_blueprint_page(
        &self,
        package_address: &str,
    ) -> FluentRequest<'_, request::PackageBlueprintPageRequest> {
        FluentRequest {
            client: self,
            params: request::PackageBlueprintPageRequest {
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                package_address: package_address.to_owned(),
            },
        }
    }
    /**Get Package Codes Page

Returns all the codes associated with a given package entity.
The returned response is in a paginated format, ordered by first appearance on the ledger.*/
    pub fn package_code_page(
        &self,
        package_address: &str,
    ) -> FluentRequest<'_, request::PackageCodePageRequest> {
        FluentRequest {
            client: self,
            params: request::PackageCodePageRequest {
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                package_address: package_address.to_owned(),
            },
        }
    }
    /**Get Account Locker Vaults Page

Returns all the resource vaults associated with a given account locker.
The returned response is in a paginated format, ordered by the most recent resource vault creation on the ledger.*/
    pub fn account_locker_vaults_page(
        &self,
        account_address: &str,
        locker_address: &str,
    ) -> FluentRequest<'_, request::AccountLockerVaultsPageRequest> {
        FluentRequest {
            client: self,
            params: request::AccountLockerVaultsPageRequest {
                account_address: account_address.to_owned(),
                at_ledger_state: None,
                cursor: None,
                limit_per_page: None,
                locker_address: locker_address.to_owned(),
            },
        }
    }
    /**Get Most Recent Touch of Account Lockers

Returns most recent state version given account locker has been touched. Touch refers to the creation of the account locker itself as well as any modification to its contents, such as
resource claim, airdrop or store.*/
    pub fn account_lockers_touched_at(
        &self,
        account_lockers: Vec<AccountLockerAddress>,
    ) -> FluentRequest<'_, request::AccountLockersTouchedAtRequest> {
        FluentRequest {
            client: self,
            params: request::AccountLockersTouchedAtRequest {
                account_lockers,
                at_ledger_state: None,
            },
        }
    }
    /**Get Validators Uptime

Returns validators uptime data for time range limited by `from_state_version` and `at_state_version`.*/
    pub fn validators_uptime(
        &self,
        validator_addresses: &[&str],
    ) -> FluentRequest<'_, request::ValidatorsUptimeRequest> {
        FluentRequest {
            client: self,
            params: request::ValidatorsUptimeRequest {
                at_ledger_state: None,
                from_ledger_state: None,
                validator_addresses: validator_addresses
                    .iter()
                    .map(|&x| x.to_owned())
                    .collect(),
            },
        }
    }
}
