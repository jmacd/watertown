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

FROM golang:1.24.8-bookworm AS minio-builder
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG MINIO_SOURCE_SHA256=45521908307306e925c98d629e1c17d78c8b72b6ee242b1bfb1409f7d8ee5841
ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
WORKDIR /src
RUN curl -fsSL --retry 5 \
        "https://github.com/minio/minio/archive/${MINIO_COMMIT}.tar.gz" \
        -o /tmp/minio.tar.gz && \
    echo "${MINIO_SOURCE_SHA256}  /tmp/minio.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/minio.tar.gz --strip-components=1 && \
    rm /tmp/minio.tar.gz
RUN CGO_ENABLED=0 go build -tags kqueue -trimpath \
    -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z \
        -X github.com/minio/minio/cmd.CopyrightYear=2025 \
        -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_RELEASE} \
        -X github.com/minio/minio/cmd.CommitID=${MINIO_COMMIT} \
        -X github.com/minio/minio/cmd.ShortCommitID=9e49d5e7a648" \
    -o /usr/local/bin/minio .

FROM debian:bookworm-slim AS minio-server
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=minio-builder /usr/local/bin/minio /usr/local/bin/minio
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data"]

FROM debian:bookworm-slim AS watertown-test

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

# Install the last open-source MinIO client release from its immutable GitHub
# release asset. MinIO's former public container image is no longer pullable.
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
RUN ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
        amd64) MC_ARCH="amd64"; MC_SHA256="01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891" ;; \
        arm64) MC_ARCH="arm64"; MC_SHA256="14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c" ;; \
        *) echo "Unsupported architecture for MinIO client: $ARCH" >&2; exit 1 ;; \
    esac && \
    MC_BINARY="mc.linux-${MC_ARCH}.${MC_RELEASE}" && \
    curl -fsSL --retry 5 \
        "https://github.com/minio/mc/releases/download/${MC_RELEASE}/${MC_BINARY}" \
        -o /usr/local/bin/mc && \
    echo "${MC_SHA256}  /usr/local/bin/mc" | sha256sum -c - && \
    chmod +x /usr/local/bin/mc

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
