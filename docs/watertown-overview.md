## Repository Overview

Watertown is a **query-native filesystem** that treats all filesystem operations as queryable data. Built on Apache Arrow, DataFusion, and Delta Lake, it creates a unified platform where files, directories, and metadata are first-class SQL citizens, enabling sophisticated data transformations and time-series analytics through familiar filesystem operations.

**Core Innovation**: Every filesystem object can be queried with SQL, and SQL queries can create new filesystem objects that appear as native files and directories.

**Primary Use Case**: Storage, archival, and analysis of multi-variate time-series data for small-scale telemetry and industrial systems.

## File System Architecture

Watertown implements a transactional filesystem with these distinguishing features:

- **ACID Transactions**: Atomic filesystem operations backed by Delta Lake
- **SQL-Queryable Everything**: Files, directories, and metadata accessible via DataFusion SQL
- **Dynamic Factories**: SQL queries and transformations appear as native filesystem objects
- **Time-Series Native**: Built-in support for versioned, time-bucketed data
- **Backup & Replication**: Streaming backup to S3-compatible storage with replica pond creation

The filesystem provides a hierarchical namespace with pattern matching, glob support, and wildcard variable capture for flexible data access and manipulation

## Command-Line Interface

Watertown is primarily accessed through the `pond` CLI, which provides Unix-like commands for filesystem operations:

### Basic Operations

```bash
# Initialize a new pond
pond init /data/mypond

# Create directories
pond mkdir /sensors

# Copy files into the pond
pond copy data.csv /sensors/temperature.csv

# List files with glob patterns
pond list /sensors/**/*.series

# Query files with SQL
pond cat /sensors/temperature.csv --sql "SELECT * FROM source WHERE temp > 25"

# Describe file schemas
pond describe /sensors/*.csv
```

### Dynamic Node Creation

Create computed objects using YAML factory configurations:

```bash
# Create SQL-derived view
cat > filter.yaml << EOF
factory: "sql-derived-table"
config:
  patterns:
    source: "table:///raw/measurements.parquet"
  query: "SELECT timestamp, temperature FROM source WHERE temperature > 20"
EOF

pond mknod --config filter.yaml /processed/filtered.series

# Create temporal aggregation
cat > hourly.yaml << EOF
factory: "temporal-reduce"
config:
  in_pattern: "series:///sensors/*"
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]
  aggregations:
    - type: "avg"
      columns: ["temperature", "pressure"]
EOF

pond mknod --config hourly.yaml /aggregated/hourly
```

### Executable Factories

Some factories create executable nodes that can be run with `pond run`:

```bash
# Configure data collection
cat > hydrovu.yaml << EOF
factory: "hydrovu"
config:
  client_id: "xxx"
  client_secret: "yyy"
  devices:
    - name: "Station A"
      id: 12345
      scope: "StationA"
EOF

pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml

# Execute data collection
pond run /system/etc/20-hydrovu collect

# Configure remote backup
pond mknod --config remote.yaml /system/run/10-remote
pond run /system/run/10-remote push
```

### Control Table Operations

Query transaction history and pond configuration:

```bash
# Show recent transactions
pond control recent --limit 20

# Show detailed transaction lifecycle
pond control detail --txn-seq 42

# Query with SQL
pond control --sql "
  SELECT txn_seq, cli_args, duration_ms
  FROM control_table
  WHERE record_category = 'transaction'
    AND record_type = 'completed'
  ORDER BY txn_seq DESC"

# Show pond configuration
pond control show-config
```

## Built-in Factory Types

Watertown provides several factory types for data transformation and management:

### Data Transformation Factories

Built-in timeseries composition uses typed DataFusion logical plans from the
`query-foundation` crate. SQL text is reserved for user-authored domain
queries; unrestricted SQL is conservatively global unless it passes the
validated timestamp-local contract.

- **`sql-derived-table`**: Apply SQL transformations to single files
  - Query individual Parquet/CSV files with SELECT, WHERE, JOIN
  - Uses DataFusion's native ListingTable for predicate pushdown
  - Errors if pattern matches multiple files (use sql-derived-series for that)

- **`sql-derived-series`**: Apply SQL to versioned time-series files
  - Operates on FileSeries (multiple files, multiple versions)
  - Automatically unions data across files and versions
  - Ideal for time-series aggregation and multi-file analytics

- **`timeseries-join`**: Join multiple time series by timestamp
  - Typed accumulated FULL OUTER JOIN on timestamp
  - Per-input time range filtering
  - Scope prefixing for column disambiguation
  - Optional per-input transform pipelines

- **`temporal-reduce`**: Time-bucketed aggregations
  - Creates multiple resolution levels (1h, 6h, 1d, etc.)
  - Aggregation types: avg, min, max, count, sum
  - Outputs directory with `res={duration}.series` files
  - Typed partial aggregation, bounded repair, and persistent sealed/hot state

- **`temporal-reduce-series`**: One directly addressable reduced series
  - Requires one exact logical source
  - Uses the same persistent partial state as `temporal-reduce`
  - Supports stable output aliases for public schemas

- **`timeseries-pivot`**: Long-to-wide format conversion
  - Pivots row-based data into columnar time series
  - Configurable pivot columns and value columns
  - Uses a typed timestamp-spine plan with explicit null padding

- **`pump-state-series`**: Adaptive well-pump episode classification
  - Streams ordered depth samples into pumping, recovering, and static phases
  - Persists provisional episode and repair boundaries
  - Exposes recursive bounded lineage to downstream reductions

- **`column-rename`**: Rename or drop columns
  - Glob pattern support for bulk renaming
  - Used as transform in factory pipelines

- **`template`**: Render Tera templates with file pattern variables
  - Django-style template syntax
  - Wildcard capture for dynamic content generation

- **`dynamic-dir`**: Compose multiple factories into one directory
  - Each entry uses a different factory
  - Enables complex hierarchical transformations

### Executable Factories

- **`hydrovu`**: HydroVu API data collection
  - OAuth2 authentication with automatic token refresh
  - Device-based data collection with parameter mapping
  - Wide-format time series storage (timestamp as primary key)
  - Commands: `collect`

- **`remote`**: Backup and replication management
  - Streaming backup to S3-compatible storage
  - Bundle-based transaction replication
  - Primary/replica pond modes
  - Commands: `push`, `pull`, `replicate`, `list-files`, `verify`

A complete example using multiple factories can be found in `${REPO_ROOT}/noyo`.

## Project Crates

Watertown is organized into workspace crates with clear separation of concerns:

### Core Filesystem Crates

#### `tinyfs` - Type-Safe Filesystem Abstraction

Provides a pure, pluggable filesystem API with strong typing and async operations:

- **Core Types**: `FS`, `WD` (working directory), `Node`, `File`, `Directory`, `Symlink`
- **Persistence Trait**: `PersistenceLayer` for pluggable storage backends
- **Node Identity**: `NodeID` and `FileID` for unique object identification
- **Navigation**: Path-based operations with `Pathed<Handle>` preserving access context
- **Memory Backend**: Built-in `MemoryPersistence` for testing
- **Arrow Integration**: Native support for Arrow schemas and data types

TinyFS is storage-agnostic - it defines the filesystem API without committing to any particular storage implementation.

#### `tlogfs` - Delta Lake Backed Implementation

Implements TinyFS using Delta Lake for ACID storage with queryability:

- **OpLogPersistence**: Implements `PersistenceLayer` using Delta Lake operations log
- **Transaction Guards**: ACID guarantees with automatic rollback on panic/error
- **Query Integration**: Files expose DataFusion `TableProvider` interfaces
- **Dynamic Factory System**: Plugin architecture for computed objects (SQL views, aggregations)
- **Schema Management**: Arrow schema validation and evolution
- **Large Files**: Content-addressed storage for files >10MB with SHA256 deduplication
- **Data Taxonomy**: Type-safe wrappers for sensitive configuration (API keys, secrets)

**Key Files**:
- `persistence.rs`: State management and OpLogPersistence implementation
- `transaction_guard.rs`: ACID transaction lifecycle
- `query/`: DataFusion integration for SQL operations
- `file.rs`, `directory.rs`, `symlink.rs`: Filesystem object implementations
- `delta/`: Delta Lake table operations and schema definitions

#### `steward` - Transaction Coordination & Audit

Manages transaction lifecycle with separate control table for audit and recovery:

- **Ship**: Main orchestrator managing data filesystem and control table
- **StewardTransactionGuard**: Wraps TLogFS transactions with lifecycle tracking
- **ControlTable**: Delta Lake audit log with transaction metadata
- **Post-Commit Actions**: Sequences factory execution after data commits
- **Recovery**: Restores incomplete transactions after crashes

**Architecture**:
- Data Filesystem: `{pond_path}/data` - user data and dynamic objects (TLogFS)
- Control Table: `{pond_path}/control` - transaction metadata (separate Delta Lake table)
- Pond Identity: UUID-based identity preserved across replicas
- Factory Modes: Configurable execution modes (push vs pull for remote factory)

### Data Access & Transformation

#### `query-foundation` - Typed Query Semantics

Defines storage-independent query contracts and typed DataFusion plans:

- **Snapshots and Chunks**: Exact immutable membership and physical statistics
- **Overlap Policies**: Preserve-all, precedence, and key-conflict semantics
- **Locality and Frontier**: Append, unsealed disorder, repair, and rejection
- **Typed Plans**: Transform, combine, join, pivot, fixed-window reduction, and
  pump-state classification
- **Materialization**: Streaming, failure-atomic output and progress publication
- **Metrics**: Result, plan, I/O, memory, and failure evidence

TinyFS, TLogFS, and provider code adapt storage into these contracts. The
foundation does not depend on those production crates.

#### `provider` - URL-Based Access & Factory Infrastructure

Provides unified data access and the factory plugin system:

- **URL-Based Access**: `scheme:///path/pattern` for format conversion and filtering
- **Format Registry**: Pluggable format providers (CSV, Parquet, JSON, Excel HTML)
- **Factory Registry**: Compile-time registration with `linkme` distributed slices
- **TinyFS ObjectStore**: DataFusion ObjectStore implementation for seamless integration
- **TableProvider Creation**: Unified interface for creating queryable tables
- **Transform Pipeline**: Composable table transformations (column rename, scope prefix)

**Supported URL Schemes**:
- `series:///pattern` - FileSeries (Parquet time series)
- `table:///pattern` - FileTable (single files)
- `csv:///pattern` - CSV files with format conversion
- `csv+gzip:///pattern` - Compressed CSV with automatic decompression
- `excelhtml:///pattern` - HydroVu Excel HTML exports

**Factory Subdirectories**:
- `factory/sql_derived.rs`: SQL transformation factories (table and series modes)
- `factory/temporal_reduce.rs`: Time-bucketed aggregation factory
- `factory/pump_state.rs`: Streaming pump-state classification factory
- `factory/timeseries_join.rs`: Multi-series join factory
- `factory/timeseries_pivot.rs`: Long-to-wide pivot factory
- `factory/dynamic_dir.rs`: Composite directory factory
- `factory/column_rename.rs`: Column transformation factory
- `factory/template.rs`: Tera template rendering factory
- `query_foundation_adapter.rs`: TinyFS/TLogFS snapshot and lineage adapter

### Domain-Specific Crates

#### `cmd` - Command-Line Interface

Comprehensive `pond` CLI built with clap:

- **Initialization**: `init`, `recover` - pond lifecycle management
- **Filesystem Operations**: `mkdir`, `list`, `cat`, `describe`, `copy`
- **Dynamic Nodes**: `mknod` - create factory-based computed objects
- **Execution**: `run` - execute factory commands
- **Control Table**: `control` - query transaction history and configuration
- **Temporal Operations**: `temporal` - set/detect time bounds for files

**Output Formats**: CSV, JSON, Parquet, Arrow IPC, Table (pretty-print)

#### `hydrovu` - Water Quality Data Collection

HydroVu API integration with factory-based execution:

- **OAuth2 Authentication**: Automatic token management with data taxonomy protection
- **Device Configuration**: Per-device parameter mapping and scopes
- **Wide-Format Storage**: Timestamp as primary key with parameterized columns
- **Automatic Schema**: Arrow schema generation from API metadata
- **Incremental Collection**: Tracks last timestamps to avoid duplicates

**Factory Configuration**: YAML-based device and parameter setup
**Execution**: Via `pond run /system/etc/20-hydrovu collect`

#### `remote` - Backup & Replication System

Streaming backup and restore using chunked parquet in Delta Lake:

- **Chunked Format**: Large files split into ~16MB chunks with BLAKE3 Merkle verification
- **Object Store**: S3-compatible storage (AWS S3, MinIO, Cloudflare R2, local files)
- **Transaction-Based**: One bundle per transaction for incremental backups
- **Replica Support**: Create replica ponds with preserved identity
- **Factory Modes**: `push` (primary) vs `pull` (replica) execution

**Schema Structure**:
- `bundle_id`: Partition key (UUID per backup)
- `chunk_data`: Binary blob (~16MB per chunk)
- `chunk_hash`: Cumulative BLAKE3 bao-root for verification
- `chunk_outboard`: SeriesOutboard for streaming validation

#### `utilities` - Shared Helpers

Common utilities used across crates:
- Banner formatting for CLI output
- Shared error types and result helpers.
