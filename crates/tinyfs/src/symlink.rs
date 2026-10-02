// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::error;
use super::metadata::Metadata;

pub const SYMLINK_LOOP_LIMIT: u32 = 10;

/// Represents a symlink that points to another path
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Symlink: Metadata + Send + Sync {
    async fn readlink(&self) -> error::Result<PathBuf>;
}

/// A handle for a refcounted symlink.
#[derive(Clone)]
pub struct Handle(Arc<Mutex<Box<dyn Symlink>>>);

impl Handle {
    pub fn new(r: Arc<Mutex<Box<dyn Symlink>>>) -> Self {
        Self(r)
    }

    pub async fn readlink(&self) -> error::Result<PathBuf> {
        let symlink = self.0.lock().await;
        symlink.readlink().await
    }

    /// Get metadata through the symlink handle
    pub async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        let symlink = self.0.lock().await;
        symlink.metadata().await
    }
}

impl Deref for Handle {
    type Target = Arc<Mutex<Box<dyn Symlink>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
