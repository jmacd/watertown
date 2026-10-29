// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git-Ingest Factory
//!
//! Mounts a git repository branch as a dynamic directory in the pond.
//! Maintains a bare repo cache at `{POND}/git/{node-id}.git/` for
//! efficient incremental fetches. The git tree is served lazily --
//! no files are copied into the pond.

mod factory;
#[cfg(feature = "documentation")]
pub use factory::GitIngestConfig;
pub(crate) mod git;
pub mod tree;
