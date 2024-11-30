CREATE TABLE applications (
    id TEXT PRIMARY KEY,
    owner_identity TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
);
