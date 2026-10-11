# Vendor stage — download JS/WASM dependencies for sitegen (DuckDB-WASM,
# Observable Plot, D3).  Runs in parallel with the Rust build.
FROM node:25-slim AS vendor
WORKDIR /vendor
COPY crates/sitegen/vendor/download.sh .
RUN bash download.sh

# Build stage
FROM rust:1.99 AS builder
WORKDIR /app

# Copy source
COPY . .

# Version baked into the binary so `pond --version` matches the deb version,
# image tag, and GitHub release. Defaults to the Cargo.toml version when the
# build arg is not supplied.
ARG WATERTOWN_VERSION=""
ENV WATERTOWN_VERSION=${WATERTOWN_VERSION}

# Build the pond binary
RUN cargo build --release --bin pond

# Runtime stage
FROM debian:bookworm-slim AS runtime

ARG WATERTOWN_VERSION=""
LABEL org.opencontainers.image.version=${WATERTOWN_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/pond /usr/local/bin/pond
COPY --from=vendor /vendor/dist/ /usr/local/share/watertown/vendor/

ENTRYPOINT ["/usr/local/bin/pond"]
