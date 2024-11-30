CREATE TABLE dapp_definitions (
    application_id TEXT NOT NULL,
    dapp_definition_address TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(application_id, dapp_definition_address)
);
