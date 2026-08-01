// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

pub mod sql_executor;

pub use sql_executor::{execute_sql_on_file, get_file_schema};
