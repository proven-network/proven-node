CREATE TABLE linked_passkeys (
    prf_public_key  BLOB        PRIMARY KEY,
    identity_id     BLOB        NOT NULL,
    created_at      INTEGER     NOT NULL,
    updated_at      INTEGER     NOT NULL,
    FOREIGN KEY (identity_id) REFERENCES identities (id)
);
