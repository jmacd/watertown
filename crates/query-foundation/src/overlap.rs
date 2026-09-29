// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Explicit overlap and logical row-identity contracts.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};

use crate::snapshot::{ChunkDescriptor, EventTimeContract};

/// Columns that identify revisions of one logical row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowIdentity {
    columns: Arc<[Arc<str>]>,
}

impl RowIdentity {
    /// Construct a non-empty identity with unique, non-empty column names.
    pub fn try_new<I, S>(columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "row identity must contain at least one column".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for column in &columns {
            if column.is_empty() {
                return Err(DataFusionError::Plan(
                    "row identity column must not be empty".to_owned(),
                ));
            }
            if !unique.insert(column.as_ref()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate row identity column '{column}'"
                )));
            }
        }
        Ok(Self {
            columns: columns.into(),
        })
    }

    /// Ordered logical-key columns.
    #[must_use]
    pub fn columns(&self) -> &[Arc<str>] {
        &self.columns
    }

    fn validate_schema(&self, schema: &SchemaRef) -> Result<()> {
        for column in self.columns() {
            _ = schema.field_with_name(column).map_err(|_| {
                DataFusionError::Plan(format!(
                    "row identity column '{column}' is absent from the snapshot schema"
                ))
            })?;
        }
        Ok(())
    }
}

/// Declared treatment of overlapping immutable chunks or composed sources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OverlapPolicy {
    /// Preserve every row without inferring duplicate identity.
    PreserveAll,
    /// Require complete metadata proving event-time ranges are disjoint.
    RequireDisjoint,
    /// Permit range overlap but reject duplicate logical keys.
    RejectDuplicateKey {
        /// Columns forming the logical row key.
        key: RowIdentity,
    },
    /// Select the row from the greatest deterministic chunk sequence per key.
    PreferBySequence {
        /// Columns forming the logical row key.
        key: RowIdentity,
    },
}

impl OverlapPolicy {
    /// Reject duplicate logical keys.
    #[must_use]
    pub fn reject_duplicate_key(key: RowIdentity) -> Self {
        Self::RejectDuplicateKey { key }
    }

    /// Select the greatest-sequence revision for each logical key.
    #[must_use]
    pub fn prefer_by_sequence(key: RowIdentity) -> Self {
        Self::PreferBySequence { key }
    }

    pub(crate) fn validate(
        &self,
        schema: &SchemaRef,
        chunks: &[ChunkDescriptor],
        event_time: Option<&EventTimeContract>,
    ) -> Result<()> {
        match self {
            Self::PreserveAll => Ok(()),
            Self::RequireDisjoint => validate_disjoint(chunks, event_time),
            Self::RejectDuplicateKey { key } | Self::PreferBySequence { key } => {
                key.validate_schema(schema)
            }
        }
    }
}

fn validate_disjoint(
    chunks: &[ChunkDescriptor],
    event_time: Option<&EventTimeContract>,
) -> Result<()> {
    if event_time.is_none() {
        return Err(DataFusionError::Plan(
            "RequireDisjoint overlap policy requires an event-time contract".to_owned(),
        ));
    }
    let mut non_empty = chunks
        .iter()
        .filter(|chunk| chunk.logical_count() != 0)
        .map(|chunk| {
            chunk.event_time_bounds().map_or_else(
                || {
                    Err(DataFusionError::Plan(format!(
                        "RequireDisjoint cannot prove chunk '{}' is disjoint without event-time bounds",
                        chunk.chunk_id()
                    )))
                },
                |bounds| Ok((chunk, bounds)),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    non_empty.sort_by_key(|(_, bounds)| (bounds.min(), bounds.max()));

    let Some((mut prior_chunk, mut prior)) = non_empty.first().copied() else {
        return Ok(());
    };
    for (chunk, bounds) in non_empty.into_iter().skip(1) {
        if bounds.min() <= prior.max() {
            return Err(DataFusionError::Plan(format!(
                "RequireDisjoint found overlapping chunks '{}' [{}..={}] and '{}' [{}..={}]",
                prior_chunk.chunk_id(),
                prior.min(),
                prior.max(),
                chunk.chunk_id(),
                bounds.min(),
                bounds.max()
            )));
        }
        if bounds.max() > prior.max() {
            prior_chunk = chunk;
            prior = bounds;
        }
    }
    Ok(())
}
