INSERT INTO __proven_migrations(id, query_hash, query, applied_at)
VALUES(?1, ?2, ?3, strftime('%s','now'));
