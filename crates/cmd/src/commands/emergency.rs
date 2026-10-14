// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Emergency operations for destructive maintenance tasks.

use anyhow::{Result, anyhow};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

/// Erase all objects in an S3 bucket.
///
/// Lists every object and deletes them. Does not delete the bucket itself
/// (bucket lifecycle is managed by the cloud provider or MinIO admin).
pub async fn erase_bucket(
    url: &str,
    endpoint: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    allow_http: bool,
    dangerous: bool,
) -> Result<()> {
    if !dangerous {
        return Err(anyhow!(
            "erase-bucket is a destructive operation.\n\
            Add --dangerous to confirm you want to delete all objects in '{}'.",
            url
        ));
    }

    let bucket = url
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("URL must start with s3://, got '{}'", url))?;

    log::warn!("ERASING all objects in bucket '{}'", bucket);

    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );

    // List all objects
    let objects: Vec<_> = store
        .list(None)
        .try_collect()
        .await
        .map_err(|e| anyhow!("failed to list objects in '{}': {}", bucket, e))?;

    let count = objects.len();
    if count == 0 {
        log::info!("Bucket '{}' is already empty", bucket);
        return Ok(());
    }

    log::info!("Deleting {} object(s) from '{}'...", count, bucket);

    // Delete in batches
    for obj in &objects {
        store
            .delete(&obj.location)
            .await
            .map_err(|e| anyhow!("failed to delete '{}': {}", obj.location, e))?;
    }

    log::info!("Erased {} object(s) from '{}'", count, bucket);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_erase_bucket_requires_dangerous_flag() {
        let result = erase_bucket(
            "s3://test-bucket",
            "http://localhost:9000",
            "us-east-1",
            "key",
            "secret",
            true,
            false, // dangerous = false
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--dangerous"));
    }

    #[tokio::test]
    async fn test_erase_bucket_rejects_non_s3_url() {
        let result = erase_bucket(
            "file:///tmp/test",
            "http://localhost:9000",
            "us-east-1",
            "key",
            "secret",
            true,
            true,
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3://"));
    }
}
