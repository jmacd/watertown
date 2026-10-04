// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! The `exec` dynamic factory: run an external program against declared
//! pond paths inside a sandbox, committing its declared outputs as one
//! transaction.
//!
//! See `docs/exec-factory-design.md` for the full design.

pub mod config;
pub mod factory;
pub mod sandbox;
pub mod stage;
