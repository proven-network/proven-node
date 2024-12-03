INSERT INTO __proven_migrations(query_hash, query, applied_at)
VALUES(?1, ?2, strftime('%s','now'));
