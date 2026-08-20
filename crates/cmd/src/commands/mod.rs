// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

pub mod apply;
pub mod capsule;
pub mod cat;
pub mod control;
pub mod copy;
pub mod describe;
pub mod emergency;
pub mod export;
pub mod fsck;
// pub mod hydrovu;  // Removed: replaced by factory-based `pond run` command
pub mod init;
pub mod limits;
pub mod list;
pub mod list_factories;
pub mod maintain;
pub mod mkdir;
pub mod mknod;
pub mod pull;
pub mod push;
pub mod rebuild_control;
pub mod recover;
pub mod remote;
pub mod restore;
pub mod run;
pub mod run_summary;
pub mod show;
pub mod status;
pub mod tlog;
pub mod verify;

pub use apply::apply_command;
pub use capsule::{
    CapsuleGcAction, capsule_gc_command, capsule_inspect_command, capsule_list_command,
    capsule_publish_command,
};
pub use cat::cat_command;
pub use control::control_command;
pub use copy::{CopyOptions, copy_command};
pub use describe::describe_command;
pub use export::export_command;
pub use fsck::fsck_command;
pub use init::init_command;
pub use limits::limits_command;
pub use list::list_command;
pub use list_factories::list_factories_command;
pub use maintain::maintain_command;
pub use mkdir::mkdir_command;
pub use mknod::mknod_command;
pub use pull::{pull_command, pull_command_with_rebuild};
pub use push::push_command;
pub use rebuild_control::rebuild_control_command;
pub use recover::recover_command;
pub use remote::{
    RemoteListFilter, add_backup_command, add_remote_command, list_remotes_command,
    remove_remote_command,
};
pub use restore::restore_command;
pub use run::run_command;
pub use show::show_command;
pub use status::status_command;
pub use tlog::{tlog_show_command, tlog_verify_command};
pub use verify::verify_command;
