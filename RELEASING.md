# Releasing Watertown

This document describes the release process for Watertown and addresses software supply chain security considerations.

## Current Release Process

### Automated Builds

All builds are performed via GitHub Actions (see `.github/workflows/rust-ci.yml`):

1. **On Pull Requests**: Run tests, clippy, and format checks only.  The
   container image and `.deb` artifact jobs run on a PR only when the
   `build-container` label is present.
2. **On Main Branch**: Run tests + build and publish the multi-arch container
   image (`build-container`) + build and publish the `.deb` as an OCI artifact
   (`build-deb`)
3. **On `workflow_dispatch`**: Same as main.

### Artifacts Produced

For each successful build on `main` or `workflow_dispatch`:

- **Version**: `0.<commit-count>.<PR-number>`. The commit count is first so the
  Debian version is monotonic even when PRs merge out of numeric order. CI
  verifies the computed value is greater than the nearest prior `v*` release
  with `dpkg --compare-versions`.

- **Container Image**: `ghcr.io/jmacd/watertown/watertown:latest-<arch>`
  (plus `sha-<short>-<arch>`)
  - Built with Podman on GitHub's Ubuntu runners (amd64 + arm64)
  - Based on Debian Bookworm (glibc 2.36)
  - Includes the pond binary at `/usr/bin/pond`
  - Cosign-signed via Sigstore keyless signing

- **Debian Package (OCI artifact)**:
  `ghcr.io/jmacd/watertown/pond-deb:latest-<arch>` (plus `sha-<short>-<arch>`)
  - Built with `cargo deb -p cmd` on amd64 + arm64 runners
  - Pushed as an OCI artifact with `oras` (media type
    `application/vnd.debian.binary-package`); the artifact carries the real
    `watertown_<version>_<arch>.deb` filename
  - Package name `watertown`, installs `/usr/bin/pond` plus the sitegen vendor
    blobs to `/usr/share/watertown/vendor/`
  - Cosign-signed via Sigstore keyless signing
  - Consumed by the natively-installed `watershop-selfmon` pond, which pulls
    it hourly (see caspar.water `config/scripts/update-selfmon.sh`)

### Promotion to prod

`.github/workflows/promote.yml` (manual `workflow_dispatch`) retags an existing
`latest-<arch>` (or `sha-…` / `prod-…`) tag to `prod-<arch>` for **both** the
container image and the `pond-deb` artifact in lockstep, using `crane cp` (a
digest-preserving retag, not a rebuild), plus a dated `prod-<stamp>-<arch>`
rollback handle, then cosign-signs each. Container hosts select `latest-` for
`*-staging` instances and `prod-` otherwise; the selfmon host selects its deb
channel via the `DEB_CHANNEL` env (default `latest`).

### Creating a Release

Every successful push to `main` creates `v0.<commit-count>.<PR-number>` at the
exact built commit and generates GitHub release notes. Re-running a workflow for
an existing release is idempotent. Do not manually edit the workspace crate
version or create a competing tag for routine builds.

## Software Supply Chain Security

### Current Security Measures

✅ **Build Isolation**: All builds run in ephemeral GitHub-hosted runners  
✅ **Dependency Pinning**: `Cargo.lock` is committed to the repository  
✅ **Audit Trail**: All builds are associated with specific commits and workflow runs  
✅ **Container Registry**: Uses GitHub Container Registry with authentication  
✅ **Artifact Signing**: Container and Debian OCI tags are signed with keyless Cosign

### Security Gaps (TODO)

❌ **No host-side signature enforcement**: Hosts do not yet require Cosign verification before install
❌ **No SBOM generation**: No Software Bill of Materials  
❌ **No provenance attestation**: No SLSA provenance  
❌ **Limited reproducibility**: Builds may not be bit-for-bit reproducible  

## Recommendations for Production Use

### 1. Sign Releases with GPG

Install GPG key in GitHub Actions:

```yaml
- name: Import GPG key
  uses: crazy-max/ghaction-import-gpg@v6
  with:
    gpg_private_key: ${{ secrets.GPG_PRIVATE_KEY }}
    passphrase: ${{ secrets.GPG_PASSPHRASE }}

- name: Sign .deb package
  run: |
    dpkg-sig --sign builder artifacts/*.deb
```

### 2. Generate SBOM with cargo-sbom

Add to workflow:

```yaml
- name: Generate SBOM
  run: |
    cargo install cargo-sbom
    cargo sbom > artifacts/pond-sbom.json
```

### 3. Add SLSA Provenance

Use the SLSA GitHub Generator:

```yaml
- uses: slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v1.9.0
  with:
    provenance-name: pond-provenance.intoto.jsonl
```

### 4. Sign Container Images with Cosign

```yaml
- name: Sign container image
  run: |
    cosign sign --key env://COSIGN_KEY ${{ steps.meta.outputs.image_id }}:${{ steps.meta.outputs.version }}
  env:
    COSIGN_KEY: ${{ secrets.COSIGN_PRIVATE_KEY }}
```

### 5. Enable Dependabot

Create `.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: "cargo"
    directory: "/"
    schedule:
      interval: "weekly"
```

## Verifying Releases (For Users)

### Current State (No Signing)

**Trust model**: Trust GitHub Actions + repository access control

Verification steps:
1. Verify the artifact came from the official repository
2. Check the commit hash matches the release tag
3. Review the workflow run logs for any anomalies

### Future State (With Signing)

Users will be able to verify signatures:

```bash
# Verify .deb signature
dpkg-sig --verify pond_0.16.0_amd64.deb

# Verify container image
cosign verify ghcr.io/jmacd/watertown:v0.16.0

# Verify SLSA provenance
slsa-verifier verify-artifact pond \
  --provenance-path pond-provenance.intoto.jsonl \
  --source-uri github.com/jmacd/watertown
```

## Deployment to Production

### Target Environment

- **Platform**: Debian 12 (Bookworm)
- **Architecture**: x86_64 (amd64)
- **Minimum RAM**: 1GB (monitor memory usage)
- **Dependencies**: libssl3, ca-certificates

### Installation via .deb Package (OCI artifact)

The `.deb` is published as an OCI artifact rather than a GitHub Release asset.
Pull it with `oras` (the package is public, no login required) and install:

```bash
# Pull the newest deb for this arch (e.g. arm64) into the current dir
oras pull ghcr.io/jmacd/watertown/pond-deb:latest-arm64

# Install (auto-supersedes a legacy `duckpond` package via Conflicts/Replaces)
sudo dpkg -i watertown_*_arm64.deb
sudo apt-get install -f  # Install any missing dependencies

# Verify
pond --version

# Uninstall
sudo dpkg -r watertown
```

On the `watershop-selfmon` host this is automated: an hourly
`pond-selfmon-update@.timer` runs `update-selfmon.sh`, which pulls the deb for
its `DEB_CHANNEL` and installs it only when the pulled version is newer.
`tools/build-on-watershop.sh` remains a dev-only fallback for fast local
iteration without waiting for CI.

### Running in Production

Create systemd service `/etc/systemd/system/pond.service`:

```ini
[Unit]
Description=Watertown Service
After=network.target

[Service]
Type=simple
User=pond
Group=pond
ExecStart=/usr/bin/pond <your-arguments>
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo useradd -r -s /bin/false pond
sudo systemctl daemon-reload
sudo systemctl enable pond
sudo systemctl start pond
sudo journalctl -u pond -f
```

## Security Considerations

### Runtime Security

- Run as non-root user (demonstrated in systemd service above)
- Use minimal container images (Debian bookworm-slim)
- Keep dependencies up to date (use Dependabot)
- Monitor for CVEs in dependencies

### Access Control

- Container registry requires authentication for writes
- GitHub Actions uses OIDC tokens (no long-lived credentials)
- Release artifacts require repo admin access to create

### Audit Trail

All releases are traceable:
- Git commit hash
- GitHub Actions workflow run ID
- Build logs (retained for 90 days)
- Container image layers and manifest

## Future Improvements

1. **Reproducible builds**: Investigate cargo-careful or similar tools
2. **Multi-architecture builds**: Add ARM64 support
3. **Automated security scanning**: Integrate trivy or grype
4. **FIPS compliance**: If required for regulated environments
5. **Release automation**: Auto-create GitHub releases from tags

## Questions or Issues?

For release-related questions, open an issue in the repository with the `release` label.
