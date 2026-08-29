# Native input: `dp.commit.3`

This migration recipe reads the frozen legacy Delta key/value backup format
documented by the original `dp.commit.3` recovery kit. It verifies live Delta
rows, `value_blake3`, `dp.commit.3`, `dp.manifest.2`, `dp.tree.2`,
`dp.series.1`, `dp.recipe.1`, the node-manifest Merkle root, and every raw
object BLAKE3.

The extractor uses Delta metadata only to select active Parquet data files and
DuckDB to read the fixed key/value rows. It never imports PyArrow and never
opens a physical table payload as Parquet. Table payloads are copied as opaque
bytes.
