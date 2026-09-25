#!/bin/bash
# Build the experiment container image
#
# This script:
# 1. Cross-compiles pond for linux (matching host arch)
# 2. Builds the experiment Docker image
#
# Options:
#   --release    Build in release mode (default: debug)
#   --quiet      Suppress build output
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Detect container runtime (docker or podman)
source "${SCRIPT_DIR}/container-runtime.sh"

# Parse arguments
BUILD_MODE="debug"
QUIET=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --release)
            BUILD_MODE="release"
            shift
            ;;
        --quiet|-q)
            QUIET=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo "=== Watertown Experiment Image Builder ==="
echo "Repository: ${REPO_ROOT}"

# Determine target architecture
HOST_OS=$(uname -s)
HOST_ARCH=$(uname -m)

echo "Host: ${HOST_OS} ${HOST_ARCH}"

# Build pond binary for the container target
# Match the host architecture for native container execution
if [[ "${HOST_ARCH}" == "arm64" ]] || [[ "${HOST_ARCH}" == "aarch64" ]]; then
    CONTAINER_TARGET="aarch64-unknown-linux-gnu"
    DOCKER_PLATFORM="linux/arm64"
else
    CONTAINER_TARGET="x86_64-unknown-linux-gnu"
    DOCKER_PLATFORM="linux/amd64"
fi

if [[ "${BUILD_MODE}" == "release" ]]; then
    BINARY_PATH="${REPO_ROOT}/target/${CONTAINER_TARGET}/release/pond"
    CARGO_PROFILE="--release"
else
    BINARY_PATH="${REPO_ROOT}/target/${CONTAINER_TARGET}/debug/pond"
    CARGO_PROFILE=""
fi
echo "Target: ${CONTAINER_TARGET} (Docker: ${DOCKER_PLATFORM}, mode: ${BUILD_MODE})"

if [[ "${HOST_OS}" == "Darwin" ]]; then
    echo ""
    echo "=== Cross-compiling for Linux (${CONTAINER_TARGET}) ==="
    
    # Ensure target is installed
    rustup target add "${CONTAINER_TARGET}" 2>/dev/null || true
    
    # Try zigbuild if available (works best for cross-compilation)
    # cargo-zigbuild finds zig via PATH or via the Python ziglang package.
    if command -v cargo-zigbuild &> /dev/null && cargo-zigbuild zig cc -- --version &> /dev/null; then
        echo "Using cargo-zigbuild (${BUILD_MODE})..."
        cd "${REPO_ROOT}"
        if [[ "${QUIET}" == "true" ]]; then
            cargo zigbuild ${CARGO_PROFILE} --bin pond --target ${CONTAINER_TARGET} 2>&1 | tail -5
        else
            cargo zigbuild ${CARGO_PROFILE} --bin pond --target ${CONTAINER_TARGET}
        fi
    else
        echo "ERROR: cargo-zigbuild and/or zig not found."
        echo "Install with: cargo install cargo-zigbuild && pip3 install ziglang"
        exit 1
    fi
else
    echo ""
    echo "=== Building for native Linux (${BUILD_MODE}) ==="
    cd "${REPO_ROOT}"

    # Ensure target is installed
    rustup target add "${CONTAINER_TARGET}" 2>/dev/null || true

    # Use zigbuild if available to target the container's glibc (bookworm = 2.36).
    # A plain cargo build links against the host glibc which may be too new.
    # cargo-zigbuild finds zig via PATH or via the Python ziglang package.
    if command -v cargo-zigbuild &> /dev/null && cargo-zigbuild zig cc -- --version &> /dev/null; then
        echo "Using cargo-zigbuild targeting glibc 2.36 (${BUILD_MODE})..."
        if [[ "${QUIET}" == "true" ]]; then
            cargo zigbuild ${CARGO_PROFILE} --bin pond --target ${CONTAINER_TARGET}.2.36 2>&1 | tail -5
        else
            cargo zigbuild ${CARGO_PROFILE} --bin pond --target ${CONTAINER_TARGET}.2.36
        fi
    else
        echo "WARNING: cargo-zigbuild/zig not found, using plain cargo build."
        echo "  Binary may not run in debian:bookworm-slim if host glibc is newer."
        echo "  Install with: cargo install cargo-zigbuild && pip3 install ziglang"
        if [[ "${QUIET}" == "true" ]]; then
            cargo build ${CARGO_PROFILE} --bin pond 2>&1 | tail -5
        else
            cargo build ${CARGO_PROFILE} --bin pond
        fi
        if [[ "${BUILD_MODE}" == "release" ]]; then
            BINARY_PATH="${REPO_ROOT}/target/release/pond"
        else
            BINARY_PATH="${REPO_ROOT}/target/debug/pond"
        fi
    fi
fi

# Verify binary exists
if [[ ! -f "${BINARY_PATH}" ]]; then
    echo "ERROR: Binary not found at ${BINARY_PATH}"
    exit 1
fi

echo ""
echo "=== Binary info ==="
file "${BINARY_PATH}"
ls -lh "${BINARY_PATH}"

# Copy binary to experiments directory for Docker build
cp "${BINARY_PATH}" "${SCRIPT_DIR}/pond"

# Copy vendor download script for Docker build (downloaded inside Docker, not from host)
cp "${REPO_ROOT}/crates/sitegen/vendor/download.sh" "${SCRIPT_DIR}/vendor-download.sh"

echo ""
echo "=== Building Docker image ==="
cd "${SCRIPT_DIR}"

# Ensure helpers are executable
chmod +x "${SCRIPT_DIR}/helpers/"* 2>/dev/null || true

# Create pond2 symlink for helper
ln -sf pond1 "${SCRIPT_DIR}/helpers/pond2" 2>/dev/null || true

${CONTAINER_RT} build \
    -f Dockerfile \
    -t watertown-test:latest \
    .

echo ""
echo "=== Building MinIO test image ==="
${CONTAINER_RT} build \
    -f Dockerfile \
    --target minio-server \
    -t watertown-minio-test:latest \
    .

# Clean up copied binary and scripts
rm -f "${SCRIPT_DIR}/pond"
rm -f "${SCRIPT_DIR}/vendor-download.sh"

echo ""
echo "=== Build complete ==="
echo "Image: watertown-test:latest"
echo "Image: watertown-minio-test:latest"
echo ""
echo "Test with:"
echo "  ${CONTAINER_RT} run --rm -it watertown-test:latest"
echo "  pond --help"
echo ""
echo "For S3 experiments:"
echo "  docker-compose up -d minio"
echo "  docker-compose run --rm watertown"
