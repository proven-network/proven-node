//! Integration tests for the engine store

use proven_store_engine::{EngineStore, EngineStore1, EngineStore2};

#[tokio::test]
async fn test_store_types_compile() {
    // This test verifies that the store types compile correctly
    // Full integration tests would require a running engine

    // Test that the types exist and have the expected type parameters
    let _store_type = std::marker::PhantomData::<EngineStore<bytes::Bytes>>;
    let _store1_type = std::marker::PhantomData::<EngineStore1<bytes::Bytes>>;
    let _store2_type = std::marker::PhantomData::<EngineStore2<bytes::Bytes>>;

    // Test the default store types work with bytes
    let _vec_store = std::marker::PhantomData::<EngineStore<Vec<u8>>>;
}

// TODO: Add full integration tests once we have a test engine setup
// These would test:
// - Creating streams
// - Putting and getting values
// - Scoped stores
// - Key listing
// - Error handling
