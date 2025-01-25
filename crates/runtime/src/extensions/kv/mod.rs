mod application;
mod nft;
mod personal;

pub use application::{
    op_application_keys, op_get_application_bytes, op_get_application_key,
    op_get_application_string, op_set_application_bytes, op_set_application_key,
    op_set_application_string,
};
pub use nft::{
    op_get_nft_bytes, op_get_nft_key, op_get_nft_string, op_nft_keys, op_set_nft_bytes,
    op_set_nft_key, op_set_nft_string,
};
pub use personal::{
    op_get_personal_bytes, op_get_personal_key, op_get_personal_string, op_personal_keys,
    op_set_personal_bytes, op_set_personal_key, op_set_personal_string,
};

use deno_core::extension;
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_store::{Store1, Store2};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
enum StoredKey {
    Ed25519(Vec<u8>),
}

extension!(
    kv_options_ext,
    esm_entry_point = "proven:kv",
    esm = [
        dir "src/extensions/kv",
        "proven:kv" = "kv-options.js",
    ],
    docs = "Functions for accessing secure storage"
);

extension!(
    kv_runtime_ext,
    parameters = [ AS: Store1, PS: Store1, NS: Store2, RNV: RadixNftVerifier ],
    ops = [
        op_application_keys<AS>,
        op_get_application_bytes<AS>,
        op_set_application_bytes<AS>,
        op_get_application_key<AS>,
        op_set_application_key<AS>,
        op_get_application_string<AS>,
        op_set_application_string<AS>,
        op_nft_keys<NS, RNV>,
        op_get_nft_bytes<NS, RNV>,
        op_set_nft_bytes<NS, RNV>,
        op_get_nft_key<NS, RNV>,
        op_set_nft_key<NS, RNV>,
        op_get_nft_string<NS, RNV>,
        op_set_nft_string<NS, RNV>,
        op_personal_keys<PS>,
        op_get_personal_bytes<PS>,
        op_set_personal_bytes<PS>,
        op_get_personal_key<PS>,
        op_set_personal_key<PS>,
        op_get_personal_string<PS>,
        op_set_personal_string<PS>,
    ],
    esm_entry_point = "proven:kv",
    esm = [
        dir "src/extensions/kv",
        "proven:kv" = "kv-runtime.js",
    ],
    docs = "Functions for accessing secure storage"
);
