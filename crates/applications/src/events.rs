#![allow(dead_code)]

use std::collections::HashSet;

/// Domain lifecycle events to be emitted by the applications manager which other sub-systems can listen to.
/// E.g. `DomainAdded` might reconfigure HTTP routers, while `DraftPromoted` might flush any runtime caches.
pub enum ApplicationEvent {
    Archived {
        application_id: String,
    },

    CorsSettingUpdated {
        application_id: String,
        cors_domains: HashSet<String>, // TODO: Use an enum for this.
    },

    Created {
        application_id: String,
    },

    DomainAdded {
        application_id: String,
        domain: String,
    },

    DomainRemoved {
        application_id: String,
        domain: String,
    },

    DraftAdded {
        application_id: String,
        draft_id: String,
    },

    DraftPromoted {
        application_id: String,
        draft_id: String,
        version_id: String,
    },

    OwnerChanged {
        application_id: String,
        new_owner: String,
        old_owner: String,
    },

    UpgradePolicyChanged {
        application_id: String,
        upgrade_policy: String, // TODO: Use an enum for this.
    },
}
