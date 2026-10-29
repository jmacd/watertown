# Watertown Makefile
#
# Quick reference:
#   make              - show this help
#   make build        - build the pond binary (debug)
#   make test         - run unit tests
#   make integration  - build test image + run integration tests
#   make vendor       - download JS vendor dependencies (one-time, needs network)
#
# The three routine tasks: build, test, integration.
# Everything else is for maintenance or CI.

.DEFAULT_GOAL := help

.PHONY: help build release test clippy fmt check \
        vendor \
        test-image integration integration-all \
        check-headers add-headers \
        clean

POND_BIN ?= $(CURDIR)/target/debug/pond
DOCS_OUT ?= $(CURDIR)/.docs-preview/watertown
DOCS_PORT ?= 8000

.PHONY: docs-build docs-serve docs-check docs-examples

docs-build:
	POND_BIN="$(POND_BIN)" DOCS_OUT="$(DOCS_OUT)" bash scripts/docs-build.sh

docs-serve: override DOCS_OUT := $(CURDIR)/.docs-preview/watertown
docs-serve: docs-build
	@echo "Preview: http://127.0.0.1:$(DOCS_PORT)/watertown/ (no browser is opened)"
	python3 -m http.server $(DOCS_PORT) --bind 127.0.0.1 --directory .docs-preview

docs-check:
	python3 scripts/check-docs.py

docs-examples:
	bash testsuite/run-test.sh --no-rebuild 057
	bash testsuite/run-test.sh --no-rebuild 100
	bash testsuite/run-test.sh --no-rebuild 213

# ── Default ──────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  Development"
	@echo "    make build          Build pond (debug)"
	@echo "    make release        Build pond (release)"
	@echo "    make test           Run unit tests (cargo test)"
	@echo "    make clippy         Run clippy lints"
	@echo "    make fmt            Check formatting"
	@echo "    make check          All of: fmt, clippy, test"
	@echo "    make docs-build     Strict manual build using POND_BIN (no Cargo)"
	@echo "    make docs-serve     Build + serve on localhost, without opening a browser"
	@echo "    make docs-check     Check pilot metadata (compiled coverage starts at D06)"
	@echo "    make docs-examples  Run 057, 100, 213 in the existing test image"
	@echo ""
	@echo "  Vendor (one-time, requires network)"
	@echo "    make vendor         Download JS deps (DuckDB-WASM, Plot, D3)"
	@echo ""
	@echo "  Integration tests (requires Docker)"
	@echo "    make test-image     Build the test Docker image"
	@echo "    make integration    Build image + run all integration tests"
	@echo ""
	@echo "  Maintenance"
	@echo "    make check-headers  Check SPDX license headers"
	@echo "    make add-headers    Add SPDX headers to Rust files"
	@echo "    make clean          Remove build artifacts"
	@echo ""
	@echo "  Demo sites (local-only, no network required)"
	@echo "    make site-noyo      Rebuild noyo pond"
	@echo "    make site-septic    Rebuild septic pond"
	@echo "    make site-water     Rebuild water pond"
	@echo "    make sites          Rebuild all three source ponds"
	@echo "    make site-cross     Full cross-pond demo (needs S3 backups)"
	@echo ""

# ── Development ──────────────────────────────────────────────────────────

build:
	cargo build --bin pond

release:
	cargo build --release --bin pond

test:
	cargo test

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --check

check: fmt clippy test

# ── Vendor dependencies ──────────────────────────────────────────────────

vendor:
	@echo "=== Downloading vendor dependencies ==="
	cd crates/sitegen/vendor && bash download.sh

# ── Integration tests ────────────────────────────────────────────────────

test-image:
	cd testsuite && bash build-image.sh

integration: test-image
	cd testsuite && bash run-all.sh --no-rebuild --skip-browser

integration-all: test-image
	cd testsuite && bash run-all.sh --no-rebuild

# ── License headers ──────────────────────────────────────────────────────

check-headers:
	reuse lint

add-headers:
	reuse annotate --copyright="Caspar Water Company" \
		--license="Apache-2.0" \
		--skip-existing \
		--skip-unrecognised \
		--recursive \
		crates

# ── Demo sites (local-only, no network) ──────────────────────────────────
#
# Regenerate the demo pond data from local files. Each site's setup.sh
# wipes and re-creates ./pond/, then ingests local data.  The cross
# demo imports from the three source ponds' S3 backups.
#
# Individual sites:
#   make site-noyo      Rebuild noyo pond from local HydroVu + LakeTech data
#   make site-septic    Rebuild septic pond from local JSON logs
#   make site-water     Rebuild water pond from local JSON logs
#
# Combined:
#   make sites          Rebuild all three source ponds
#   make site-cross     Setup + import + generate the cross-pond demo
#                       (requires the three source ponds to have been
#                        backed up to S3 at least once)

.PHONY: sites site-noyo site-septic site-water site-cross

site-noyo: release
	cd noyo && bash setup.sh

site-septic: release
	cd septic && bash setup.sh
	cd septic && bash run.sh

site-water: release
	cd water && bash setup-local.sh
	cd water && bash run-local.sh

sites: site-noyo site-septic site-water

site-cross: release sites
	cd cross && bash setup.sh
	cd cross && bash import.sh
	cd cross && bash generate.sh

# ── Cleanup ──────────────────────────────────────────────────────────────

clean:
	cargo clean
	rm -rf crates/sitegen/vendor/dist crates/sitegen/vendor/.work
	rm -f testsuite/pond testsuite/watertown-emergency
