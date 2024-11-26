CREATE TABLE __proven_migrations (
    id INTEGER PRIMARY KEY,
    query_hash TEXT NOT NULL UNIQUE,
    query TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
