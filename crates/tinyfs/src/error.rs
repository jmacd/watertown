// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::{entry_type::EntryType, node::FileID};
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

/// Represents errors that can occur in filesystem operations
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("AlreadyExists: {0}")]
    AlreadyExists(PathBuf),
    #[error("EmptyPath")]
    EmptyPath,
    #[error("EntryTypeMismatch: {path} (requested {requested}, existing {existing})")]
    EntryTypeMismatch {
        path: PathBuf,
        requested: EntryType,
        existing: EntryType,
    },
    #[error("IDNotFound: {0}")]
    IDNotFound(FileID),
    #[error("Immutable: {0}")]
    Immutable(PathBuf),
    #[error("Internal: {0}")]
    Internal(String),
    #[error("InvalidComponent: {0}")]
    InvalidComponent(PathBuf),
    #[error("InvalidConfig: {0}")]
    InvalidConfig(String),
    #[error("InvalidRange: {start}..{end} for content of size {size}")]
    InvalidRange { start: u64, end: u64, size: u64 },
    #[error("MultipleWildcards: {0}")]
    MultipleWildcards(String),
    #[error("NotADirectory: {0}")]
    NotADirectory(PathBuf),
    #[error("NotAFile: {0}")]
    NotAFile(PathBuf),
    #[error("NotASymlink: {0}")]
    NotASymlink(PathBuf),
    #[error("NotFound: {0}")]
    NotFound(PathBuf),
    #[error("Other: {0}")]
    Other(String),
    #[error("ParentPathInvalid: {0}")]
    ParentPathInvalid(PathBuf),
    #[error("PrefixNotSupported: {0}")]
    PrefixNotSupported(PathBuf),
    #[error("RootPathFromNonRoot: {0}")]
    RootPathFromNonRoot(PathBuf),
    #[error("SymlinkLoop: {0}")]
    SymlinkLoop(PathBuf),
    #[error("VisitLoop: {0}")]
    VisitLoop(PathBuf),
    #[error("ReadOnlyImport: cannot modify imported foreign content at {0}")]
    ReadOnlyImport(PathBuf),
}

impl Error {
    pub fn not_found<P: AsRef<Path>>(path: P) -> Self {
        Error::NotFound(path.as_ref().to_path_buf())
    }

    pub fn not_a_directory<P: AsRef<Path>>(path: P) -> Self {
        Error::NotADirectory(path.as_ref().to_path_buf())
    }

    pub fn not_a_symlink<P: AsRef<Path>>(path: P) -> Self {
        Error::NotASymlink(path.as_ref().to_path_buf())
    }

    pub fn not_a_file<P: AsRef<Path>>(path: P) -> Self {
        Error::NotAFile(path.as_ref().to_path_buf())
    }

    pub fn entry_type_mismatch<P: AsRef<Path>>(
        path: P,
        requested: EntryType,
        existing: EntryType,
    ) -> Self {
        Error::EntryTypeMismatch {
            path: path.as_ref().to_path_buf(),
            requested,
            existing,
        }
    }

    #[must_use]
    pub fn invalid_range(start: u64, end: u64, size: u64) -> Self {
        Error::InvalidRange { start, end, size }
    }

    pub fn prefix_not_supported<P: AsRef<Path>>(path: P) -> Self {
        Error::PrefixNotSupported(path.as_ref().to_path_buf())
    }

    pub fn root_path_from_non_root<P: AsRef<Path>>(path: P) -> Self {
        Error::RootPathFromNonRoot(path.as_ref().to_path_buf())
    }

    pub fn parent_path_invalid<P: AsRef<Path>>(path: P) -> Self {
        Error::ParentPathInvalid(path.as_ref().to_path_buf())
    }

    #[must_use]
    pub fn empty_path() -> Self {
        Error::EmptyPath
    }

    pub fn immutable<P: AsRef<Path>>(path: P) -> Self {
        Error::Immutable(path.as_ref().to_path_buf())
    }

    pub fn already_exists<P: AsRef<Path>>(path: P) -> Self {
        Error::AlreadyExists(path.as_ref().to_path_buf())
    }

    pub fn symlink_loop<P: AsRef<Path>>(path: P) -> Self {
        Error::SymlinkLoop(path.as_ref().to_path_buf())
    }

    pub fn visit_loop<P: AsRef<Path>>(path: P) -> Self {
        Error::VisitLoop(path.as_ref().to_path_buf())
    }

    pub fn read_only_import<P: AsRef<Path>>(path: P) -> Self {
        Error::ReadOnlyImport(path.as_ref().to_path_buf())
    }

    /// Create a MultipleWildcards error from a string-like value
    pub fn multiple_wildcards<S: AsRef<str>>(s: S) -> Self {
        Error::MultipleWildcards(s.as_ref().into())
    }

    /// Create an InvalidComponent error from a path-like value
    pub fn invalid_component<P: AsRef<Path>>(p: P) -> Self {
        Error::InvalidComponent(p.as_ref().into())
    }

    /// Create an InvalidComponent error from a path-like value
    pub fn internal<S: Into<String>>(s: S) -> Self {
        Error::Internal(s.into())
    }
}

// Convert glob errors from utilities
impl From<utilities::glob::GlobError> for Error {
    fn from(e: utilities::glob::GlobError) -> Self {
        Error::Internal(format!("Glob error: {}", e))
    }
}

/// Convenience conversions for turning an arbitrary error into
/// [`Error::Other`].
///
/// These replace the two repetitive idioms that appear hundreds of times
/// across the workspace:
///
/// ```ignore
/// foo().map_err(|e| Error::Other(e.to_string()))?;            // -> foo().map_other()?
/// bar().map_err(|e| Error::Other(format!("ctx: {e}")))?;      // -> bar().map_other_context("ctx")?
/// ```
pub trait ResultExt<T> {
    /// Map any error into [`Error::Other`] using its `Display` representation.
    fn map_other(self) -> Result<T>;

    /// Map any error into [`Error::Other`], prefixed with `context` as
    /// `"{context}: {error}"`.
    fn map_other_context(self, context: impl std::fmt::Display) -> Result<T>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for std::result::Result<T, E> {
    fn map_other(self) -> Result<T> {
        self.map_err(|e| Error::Other(e.to_string()))
    }

    fn map_other_context(self, context: impl std::fmt::Display) -> Result<T> {
        self.map_err(|e| Error::Other(format!("{context}: {e}")))
    }
}

// impl std::fmt::Display for Error {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
// 	    Error::IDNotFound(id) => write!(f, "ID not found: {id:?}"),
//             Error::NotFound(path) => write!(f, "Path not found: {}", path.display()),
//             Error::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
//             Error::NotASymlink(path) => write!(f, "Not a symlink: {}", path.display()),
//             Error::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
//             Error::PrefixNotSupported(path) => {
//                 write!(f, "Path prefix not supported: {}", path.display())
//             }
//             Error::RootPathFromNonRoot(path) => {
//                 write!(f, "Can't resolve root path: {}", path.display())
//             }
//             Error::ParentPathInvalid(path) => {
//                 write!(f, "Parent path invalid: {}", path.display())
//             }
//             Error::Immutable(path) => {
//                 write!(f, "Immutable path: {}", path.display())
//             }
//             Error::EmptyPath => write!(f, "Path is empty"),
//             Error::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
//             Error::SymlinkLoop(path) => write!(f, "Too many symbolic links: {}", path.display()),
//             Error::VisitLoop(path) => write!(f, "Recursive visit to self: {}", path.display()),
//             Error::Other(msg) => write!(f, "Error: {}", msg),
//             Error::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
//             Error::MultipleWildcards(part) => write!(f, "Multiple wildcards: {}", part),
//             Error::InvalidComponent(path) => write!(f, "Invalid component: {}", path.display()),
//             Error::Internal(msg) => write!(f, "Internal error: {}", msg),
//         }
//     }
// }
