CREATE TABLE applications (
    id                  BLOB    PRIMARY KEY,
    owner_identity_id   BLOB    NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);
