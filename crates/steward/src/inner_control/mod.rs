// SPDX-License-Identifier: Apache-2.0

//! The pond control table: lifecycle records, settings, and the commit
//! spine cache.
//!
//! Absorbed from the former `sync-steward` crate.  Only the control-table
//! schema and its access API were load-bearing for the D9 steward; the
//! crate's bundle-based replication `Steward` API was dead (superseded by
//! the content-addressed push/pull path in [`crate::content_push`] /
//! [`crate::content_pull`]) and was dropped in the absorption.
//!
//! [`crate::control_table::ControlTable`] is the public wrapper over
//! [`table::ControlTable`] that the rest of `steward` and `cmd` use.

mod error;
pub mod table;

pub use error::StewardError;
pub use table::{
    CommitKind, ControlRecord, ControlTable, DataCommittedMetadata, RecordKind, new_txn_id,
};
