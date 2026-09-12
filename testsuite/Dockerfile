# Watertown Experiment Container
# 
# A sandbox environment for testing pond CLI operations safely.
# Built with the pre-compiled pond binary for fast iteration.
#
# Supports:
# - Multi-pond setups (POND1, POND2, etc.)
# - Cron scheduling for periodic tasks
# - Local S3 via MinIO for replication testing
# - Systemd-like supervision (using supervisord)

# Vendor stage — download JS/WASM dependencies for sitegen.
# Same mechanism as the production Dockerfile.
FROM node:18-slim AS vendor
WORKDIR /vendor
COPY vendor-download.sh download.sh
RUN bash download.sh

FROM quay.io/minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 AS minio-client

FROM debian:bookworm-slim

# Install runtime dependencies and useful debugging tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
        # Debugging tools
        less \
        tree \
        jq \
        file \
        xxd \
        # Text processing
        grep \
        sed \
        gawk \
        # For generating test data
        uuid-runtime \
        # Process supervision (systemd-like)
        supervisor \
        # Cron for scheduled tasks
        cron \
        # Network tools
        curl \
        wget \
        netcat-openbsd \
        # For log generation testing
        rsyslog \
        # Process tools (pgrep, ps, etc)
        procps \
        # For extracting archives
        unzip \
        # Git for git-ingest factory testing
        git \
    && rm -rf /var/lib/apt/lists/*

# The legacy dl.min.io binary endpoint is gone. Copy the multi-arch client
# from its immutable official image instead.
COPY --from=minio-client /usr/bin/mc /usr/local/bin/mc

# Install DuckDB for backup verification and emergency recovery
# Detect architecture and download appropriate version
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then \
        DUCKDB_ARCH="arm64"; \
    else \
        DUCKDB_ARCH="amd64"; \
    fi && \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v1.4.4/duckdb_cli-linux-${DUCKDB_ARCH}.zip" -o /tmp/duckdb.zip && \
    unzip -o /tmp/duckdb.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm /tmp/duckdb.zip

# Install b3sum (BLAKE3) for checksum verification
# Note: Pre-built binaries only available for x64, ARM64 needs cargo build
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then \
        apt-get update && apt-get install -y --no-install-recommends build-essential && \
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && \
        . /root/.cargo/env && \
        cargo install b3sum && \
        cp /root/.cargo/bin/b3sum /usr/local/bin/b3sum && \
        rm -rf /root/.cargo /root/.rustup && \
        apt-get remove -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*; \
    else \
        curl -fsSL https://github.com/BLAKE3-team/BLAKE3/releases/download/1.5.0/b3sum_linux_x64_bin -o /usr/local/bin/b3sum && \
        chmod +x /usr/local/bin/b3sum; \
    fi

# Set up multi-pond environment
# POND is the default, POND1/POND2 for multi-pond experiments
ENV POND=/pond
ENV POND1=/pond1
ENV POND2=/pond2

# Create all pond directories
RUN mkdir -p /pond /pond1 /pond2

# Create log directories for testing
RUN mkdir -p /var/log/watertown /var/log/experiment

# Copy the pond binary (built separately)
COPY pond /usr/local/bin/pond
RUN chmod +x /usr/local/bin/pond

# Copy vendor assets (DuckDB-WASM, Observable Plot, D3) for sitegen
COPY --from=vendor /vendor/dist/ /usr/local/share/watertown/vendor/

# Create experiment workspace
RUN mkdir -p /experiment /results /etc/supervisor/conf.d

# Supervisor configuration for multi-service experiments
COPY supervisord.conf /etc/supervisor/supervisord.conf

# Default crontab template (empty, experiments add entries)
RUN touch /etc/cron.d/experiment && chmod 0644 /etc/cron.d/experiment

# Helper scripts
COPY helpers/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*

# Default to bash for interactive experiments
WORKDIR /experiment
ENTRYPOINT ["/bin/bash"]
