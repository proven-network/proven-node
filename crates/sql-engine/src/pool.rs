//! Connection pool for `libsql` databases.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use libsql::{Builder, Connection};
use tokio::sync::Mutex;

use crate::error::Error;

/// Configuration for the connection pool.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
pub struct PoolConfig {
    /// Number of read connections
    pub read_connections: usize,
    /// Number of write connections (usually 1 for `libsql`)
    pub write_connections: usize,
    /// Number of transaction connections
    pub transaction_connections: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            read_connections: 5,
            write_connections: 1,
            transaction_connections: 3,
        }
    }
}

/// A pool of database connections.
pub struct ConnectionPool {
    /// Path to the database file
    path: PathBuf,
    /// Available read connections
    read_pool: Arc<Mutex<VecDeque<Connection>>>,
    /// Available write connections
    write_pool: Arc<Mutex<VecDeque<Connection>>>,
    /// Available transaction connections
    transaction_pool: Arc<Mutex<VecDeque<Connection>>>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub async fn new(path: PathBuf, config: PoolConfig) -> Result<Self, Error> {
        // Create the migrations table with the first connection
        let first_conn = Self::create_connection(&path).await?;
        first_conn
            .execute(
                "CREATE TABLE IF NOT EXISTS __proven_migrations (
                    query_hash TEXT PRIMARY KEY,
                    query TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )",
                (),
            )
            .await
            .map_err(|e| Error::Libsql(e.into()))?;

        let mut read_connections = VecDeque::new();
        let mut write_connections = VecDeque::new();
        let mut transaction_connections = VecDeque::new();

        // Add the first connection to write pool
        write_connections.push_back(first_conn);

        // Create remaining write connections
        for _ in 1..config.write_connections {
            let conn = Self::create_connection(&path).await?;
            write_connections.push_back(conn);
        }

        // Create read connections
        for _ in 0..config.read_connections {
            let conn = Self::create_connection(&path).await?;
            read_connections.push_back(conn);
        }

        // Create transaction connections
        for _ in 0..config.transaction_connections {
            let conn = Self::create_connection(&path).await?;
            transaction_connections.push_back(conn);
        }

        Ok(Self {
            path,
            read_pool: Arc::new(Mutex::new(read_connections)),
            write_pool: Arc::new(Mutex::new(write_connections)),
            transaction_pool: Arc::new(Mutex::new(transaction_connections)),
        })
    }

    /// Create a new database connection with WAL mode enabled.
    async fn create_connection(path: &PathBuf) -> Result<Connection, Error> {
        let conn = Builder::new_local(path)
            .build()
            .await
            .map_err(|e| Error::Libsql(e.into()))?
            .connect()
            .map_err(|e| Error::Libsql(e.into()))?;

        // Enable WAL mode for better concurrency
        // PRAGMA statements return results, so we use query and just consume the result
        let mut _rows = conn
            .query("PRAGMA journal_mode=WAL", ())
            .await
            .map_err(|e| Error::Libsql(e.into()))?;

        let mut _rows = conn
            .query("PRAGMA synchronous=NORMAL", ())
            .await
            .map_err(|e| Error::Libsql(e.into()))?;

        Ok(conn)
    }

    /// Get a read connection from the pool.
    pub async fn get_read_connection(&self) -> Result<PooledConnection, Error> {
        let mut pool = self.read_pool.lock().await;

        if let Some(conn) = pool.pop_front() {
            Ok(PooledConnection {
                connection: Some(conn),
                pool: Arc::clone(&self.read_pool),
            })
        } else {
            // If no connections available, create a new one
            let conn = Self::create_connection(&self.path).await?;
            Ok(PooledConnection {
                connection: Some(conn),
                pool: Arc::clone(&self.read_pool),
            })
        }
    }

    /// Get a write connection from the pool.
    pub async fn get_write_connection(&self) -> Result<PooledConnection, Error> {
        loop {
            let mut pool = self.write_pool.lock().await;

            if let Some(conn) = pool.pop_front() {
                return Ok(PooledConnection {
                    connection: Some(conn),
                    pool: Arc::clone(&self.write_pool),
                });
            }

            // For write connections, wait until one is available
            // since we typically only have one
            drop(pool);
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Get a transaction connection from the pool.
    pub async fn get_transaction_connection(&self) -> Result<PooledConnection, Error> {
        let mut pool = self.transaction_pool.lock().await;

        if let Some(conn) = pool.pop_front() {
            Ok(PooledConnection {
                connection: Some(conn),
                pool: Arc::clone(&self.transaction_pool),
            })
        } else {
            // If no connections available, create a new one
            let conn = Self::create_connection(&self.path).await?;
            Ok(PooledConnection {
                connection: Some(conn),
                pool: Arc::clone(&self.transaction_pool),
            })
        }
    }
}

/// A connection from the pool that returns itself when dropped.
pub struct PooledConnection {
    connection: Option<Connection>,
    pool: Arc<Mutex<VecDeque<Connection>>>,
}

impl PooledConnection {
    /// Get a reference to the connection.
    pub fn get(&self) -> Result<&Connection, Error> {
        self.connection.as_ref().ok_or(Error::BackupInProgress)
    }

    /// Take the connection out of the pooled wrapper.
    /// The connection will NOT be returned to the pool.
    pub fn take(mut self) -> Option<Connection> {
        self.connection.take()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            let pool = Arc::clone(&self.pool);
            // Return connection to pool in background
            tokio::spawn(async move {
                let mut pool = pool.lock().await;
                pool.push_back(conn);
            });
        }
    }
}
