//! Manages user sessions (created via ROLA).
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod session;

pub use error::Error;
pub use session::{ApplicationSession, ManagementSession, Session};

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_logger::info;
use proven_store::{Store, Store1};
use rand::thread_rng;
use uuid::Uuid;

/// Options for creating a new `SessionManager`
pub struct SessionManagerOptions<A, SS>
where
    A: Attestor,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The attestor to use for remote attestation.
    pub attestor: A,

    /// The KV store to use for storing sessions.
    pub sessions_store: SS,
}

/// Options for creating a new anonymous application session.
pub struct CreateAnonymousSessionOptions<'a> {
    /// The application ID.
    pub application_id: &'a Uuid,

    /// Challenge used in remote attestation.
    pub nonce: &'a Bytes,

    /// The origin of the request.
    pub origin: &'a str,

    /// The verifying key of the client.
    pub verifying_key: &'a VerifyingKey,
}

/// Options for creating a new management session.
pub struct CreateManagementSessionOptions<'a> {
    /// Challenge used in remote attestation.
    pub nonce: &'a Bytes,

    /// The origin of the request.
    pub origin: &'a str,

    /// The verifying key of the client.
    pub verifying_key: &'a VerifyingKey,
}

/// Trait for managing user sessions.
#[async_trait]
pub trait SessionManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Attestor type.
    type Attestor: Attestor;

    /// Session store type.
    type SessionStore: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Creates a new instance of the session manager.
    fn new(options: SessionManagerOptions<Self::Attestor, Self::SessionStore>) -> Self;

    // ** Application Session Methods **

    /// Anonymizes an application session.
    async fn anonymize_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Session, Error>;

    /// Creates a new anonymous application session.
    async fn create_anonymous_session(
        &self,
        options: CreateAnonymousSessionOptions<'_>,
    ) -> Result<Bytes, Error>;

    /// Gets an application session by ID.
    async fn get_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Option<Session>, Error>;

    /// Identifies an application session
    async fn identify_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error>;

    // ** Management Session Methods **

    /// Creates a new management session.
    async fn create_management_session(
        &self,
        options: CreateManagementSessionOptions<'_>,
    ) -> Result<Bytes, Error>;

    /// Gets a management session by ID.
    async fn get_management_session(&self, session_id: &Uuid) -> Result<Option<Session>, Error>;

    /// Identifies a management session
    async fn identify_management_session(
        &self,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error>;

    /// Anonymizes a management session.
    async fn anonymize_management_session(&self, session_id: &Uuid) -> Result<Session, Error>;
}

/// Manages user sessions (created via ROLA).
#[derive(Clone)]
pub struct SessionManager<A, SS>
where
    A: Attestor,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    attestor: A,
    sessions_store: SS,
}

#[async_trait]
impl<A, SS> SessionManagement for SessionManager<A, SS>
where
    A: Attestor,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type Attestor = A;
    type SessionStore = SS;

    fn new(
        SessionManagerOptions {
            attestor,
            sessions_store,
        }: SessionManagerOptions<A, SS>,
    ) -> Self {
        Self {
            attestor,
            sessions_store,
        }
    }

    async fn anonymize_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Session, Error> {
        let new_session = match self.get_session(application_id, session_id).await? {
            Some(session) => match session {
                Session::Application(ApplicationSession::Identified {
                    application_id,
                    origin,
                    session_id,
                    signing_key,
                    verifying_key,
                    ..
                }) => {
                    let new_session = Session::Application(ApplicationSession::Anonymous {
                        application_id,
                        origin,
                        session_id,
                        signing_key,
                        verifying_key,
                    });

                    self.sessions_store
                        .scope(application_id.to_string())
                        .put(session_id.to_string(), new_session.clone())
                        .await
                        .map_err(|e| Error::SessionStore(e.to_string()))?;

                    new_session
                }
                Session::Application(ApplicationSession::Anonymous { .. }) => {
                    return Err(Error::SessionStore("Session already anonymous".to_string()));
                }
                Session::Management(_) => {
                    return Err(Error::SessionStore(
                        "Cannot anonymize management session through application endpoint"
                            .to_string(),
                    ));
                }
            },
            None => {
                return Err(Error::SessionStore("Session not found".to_string()));
            }
        };

        Ok(new_session)
    }

    async fn create_anonymous_session(
        &self,
        CreateAnonymousSessionOptions {
            application_id,
            nonce,
            origin,
            verifying_key,
        }: CreateAnonymousSessionOptions<'_>,
    ) -> Result<Bytes, Error> {
        let session_id = Uuid::new_v4();

        let server_signing_key = SigningKey::generate(&mut thread_rng());
        let server_public_key = server_signing_key.verifying_key();

        let session = Session::Application(ApplicationSession::Anonymous {
            application_id: *application_id,
            origin: origin.to_string(),
            session_id,
            signing_key: server_signing_key.clone(),
            verifying_key: *verifying_key,
        });

        info!(
            "Creating anonymous application session (id: {}) for application: {}",
            session_id, application_id
        );

        self.sessions_store
            .scope(application_id.to_string())
            .put(session_id.to_string(), session.clone())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce.clone()),
                user_data: Some(Bytes::from(session_id.as_bytes().to_vec())),
                public_key: Some(Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e.to_string())),
        }
    }

    async fn get_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Option<Session>, Error> {
        self.sessions_store
            .scope(application_id.to_string())
            .get(session_id.to_string())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))
    }

    async fn identify_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error> {
        let new_session = match self.get_session(application_id, session_id).await? {
            Some(session) => match session {
                Session::Application(ApplicationSession::Anonymous {
                    application_id,
                    origin,
                    session_id,
                    signing_key,
                    verifying_key,
                }) => {
                    let new_session = Session::Application(ApplicationSession::Identified {
                        application_id,
                        identity_id: *identity_id,
                        origin,
                        session_id,
                        signing_key,
                        verifying_key,
                    });

                    self.sessions_store
                        .scope(application_id.to_string())
                        .put(session_id.to_string(), new_session.clone())
                        .await
                        .map_err(|e| Error::SessionStore(e.to_string()))?;

                    new_session
                }
                Session::Application(ApplicationSession::Identified { .. }) => {
                    return Err(Error::SessionStore(
                        "Session already identified".to_string(),
                    ));
                }
                Session::Management(_) => {
                    return Err(Error::SessionStore(
                        "Cannot identify management session through application endpoint"
                            .to_string(),
                    ));
                }
            },
            None => {
                return Err(Error::SessionStore("Session not found".to_string()));
            }
        };

        Ok(new_session)
    }

    async fn create_management_session(
        &self,
        CreateManagementSessionOptions {
            nonce,
            origin,
            verifying_key,
        }: CreateManagementSessionOptions<'_>,
    ) -> Result<Bytes, Error> {
        let session_id = Uuid::new_v4();

        let server_signing_key = SigningKey::generate(&mut thread_rng());
        let server_public_key = server_signing_key.verifying_key();

        let session = Session::Management(ManagementSession::Anonymous {
            origin: origin.to_string(),
            session_id,
            signing_key: server_signing_key.clone(),
            verifying_key: *verifying_key,
        });

        info!("Creating anonymous management session (id: {})", session_id);

        // Store management sessions under a special "__management__" scope
        self.sessions_store
            .scope("__management__".to_string())
            .put(session_id.to_string(), session.clone())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce.clone()),
                user_data: Some(Bytes::from(session_id.as_bytes().to_vec())),
                public_key: Some(Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e.to_string())),
        }
    }

    async fn get_management_session(&self, session_id: &Uuid) -> Result<Option<Session>, Error> {
        self.sessions_store
            .scope("__management__".to_string())
            .get(session_id.to_string())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))
    }

    async fn identify_management_session(
        &self,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error> {
        let new_session = match self.get_management_session(session_id).await? {
            Some(session) => match session {
                Session::Management(ManagementSession::Anonymous {
                    origin,
                    session_id,
                    signing_key,
                    verifying_key,
                }) => {
                    let new_session = Session::Management(ManagementSession::Identified {
                        identity_id: *identity_id,
                        origin,
                        session_id,
                        signing_key,
                        verifying_key,
                    });

                    self.sessions_store
                        .scope("__management__".to_string())
                        .put(session_id.to_string(), new_session.clone())
                        .await
                        .map_err(|e| Error::SessionStore(e.to_string()))?;

                    new_session
                }
                Session::Management(ManagementSession::Identified { .. }) => {
                    return Err(Error::SessionStore(
                        "Management session already identified".to_string(),
                    ));
                }
                Session::Application(_) => {
                    return Err(Error::SessionStore(
                        "Cannot identify application session through management endpoint"
                            .to_string(),
                    ));
                }
            },
            None => {
                return Err(Error::SessionStore(
                    "Management session not found".to_string(),
                ));
            }
        };

        Ok(new_session)
    }

    async fn anonymize_management_session(&self, session_id: &Uuid) -> Result<Session, Error> {
        let new_session = match self.get_management_session(session_id).await? {
            Some(session) => match session {
                Session::Management(ManagementSession::Identified {
                    origin,
                    session_id,
                    signing_key,
                    verifying_key,
                    ..
                }) => {
                    let new_session = Session::Management(ManagementSession::Anonymous {
                        origin,
                        session_id,
                        signing_key,
                        verifying_key,
                    });

                    self.sessions_store
                        .scope("__management__".to_string())
                        .put(session_id.to_string(), new_session.clone())
                        .await
                        .map_err(|e| Error::SessionStore(e.to_string()))?;

                    new_session
                }
                Session::Management(ManagementSession::Anonymous { .. }) => {
                    return Err(Error::SessionStore(
                        "Management session already anonymous".to_string(),
                    ));
                }
                Session::Application(_) => {
                    return Err(Error::SessionStore(
                        "Cannot anonymize application session through management endpoint"
                            .to_string(),
                    ));
                }
            },
            None => {
                return Err(Error::SessionStore(
                    "Management session not found".to_string(),
                ));
            }
        };

        Ok(new_session)
    }
}
