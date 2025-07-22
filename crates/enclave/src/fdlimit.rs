use fdlimit::{Outcome, raise_fd_limit};
use proven_logger::{error, info, warn};

/// Increase soft fd limit to hard limit if possible.
#[allow(clippy::cognitive_complexity)]
pub fn raise_fdlimit() {
    info!("raising fdlimit...");

    let limit = match raise_fd_limit() {
        // New fd limit
        Ok(Outcome::LimitRaised { from, to }) => {
            info!("raised fd limit from {from} to {to}");
            to
        }
        // Current soft limit
        Err(e) => {
            error!("failed to raise fd limit: {e:?}");
            rlimit::getrlimit(rlimit::Resource::NOFILE)
                .unwrap_or((256, 0))
                .0
        }
        Ok(Outcome::Unsupported) => {
            warn!("fd limit raising is not supported on this platform");
            rlimit::getrlimit(rlimit::Resource::NOFILE)
                .unwrap_or((256, 0))
                .0
        }
    };

    info!("fd limit: {limit}");
}
