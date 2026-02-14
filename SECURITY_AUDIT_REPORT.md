# SLSA Security Audit Report

**Date:** 2026-02-14  
**Repository:** jsuereth/otlp-mmap  
**Auditor:** SLSA Security Agent

## Executive Summary

This security audit identified multiple SLSA compliance issues in the otlp-mmap repository. **All critical and high-priority issues have been addressed** through the following changes:

1. ✅ **GitHub Actions pinned to commit SHA hashes** - All actions now use immutable commit references
2. ✅ **Dependabot configured** - Automated dependency updates for GitHub Actions, Docker, Cargo, Maven, and pip
3. ✅ **Docker base images pinned to digests** - All Dockerfiles now use immutable image references
4. ✅ **Non-root users added to all containers** - Improved container security posture
5. ✅ **Binary download verification improved** - Protoc download process hardened
6. ✅ **Automated security scanning** - New CI workflow for cargo-audit and Trivy scanning

The repository now meets SLSA Level 2 requirements and has significantly improved its supply chain security posture.

---

## 🔴 Critical Issues

### 1. GitHub Actions Not Pinned to Commit SHA (Supply Chain Attack Risk)

**Status:** ✅ **FIXED**  
**Severity:** HIGH  
**Impact:** Allows potential supply chain attacks via compromised action versions  
**Affected File:** `.github/workflows/docker-build.yml`

**Details:**
All GitHub Actions use mutable tag references (v3, v4, v5) instead of immutable commit SHA hashes. This violates SLSA Level 3 requirements and exposes the build process to supply chain attacks.

**Resolution:**
All GitHub Actions have been pinned to specific commit SHA hashes:
- `actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2`
- `docker/setup-buildx-action@6524bf65af31da8d45b59e8c27de4bd072b392f5 # v3.8.0`
- `docker/login-action@9780b0c442fbb1117ed29e0efdff1e18412f7567 # v3.3.0`
- `docker/metadata-action@369eb591f429131d6889c46b94e711f089e6ca96 # v5.6.1`
- `docker/build-push-action@48aba3b46d1b1fec4febb7c5d0c644b249a11355 # v6.10.0`

**Additional Actions Taken:**
- Created `.github/dependabot.yml` to automatically update pinned actions weekly
- Dependabot will create PRs with new commit SHAs when updates are available

---

## 🟡 High Priority Issues

### 2. Docker Base Images Not Pinned to Digest (Reproducibility Risk)

**Status:** ✅ **FIXED**  
**Severity:** MEDIUM-HIGH  
**Impact:** Non-reproducible builds, potential for base image tampering  

**Resolution:**
All Docker base images have been pinned to their SHA256 digests:

#### `specification/Dockerfile`
- **Fixed:** `FROM rust:1-alpine3.22@sha256:3c06253e433c1b2ac2c279a98226d385d25c5f324138ab2861a5414bfa6855f9`

#### `java/otlp-mmap/Dockerfile`
- **Fixed:** `FROM sbtscala/scala-sbt:eclipse-temurin-25_36_1.11.7_3.7.3@sha256:4b3c50ee0f31825fae62bdd590c7363affe18be4b902d450b773a7fe5461e8b3 AS build`
- **Fixed:** `FROM eclipse-temurin:25@sha256:ddd55eda5ad0ef851a6c6b5169a83d6f9c9481449de77ae511a3118a3cf8fe91`

#### `python/Dockerfile`
- **Fixed:** `FROM python:3.11-alpine@sha256:303398d5c9f110790bce60d64f902e51e1a061e33292985c72bf6cd07960bf09`

#### `python/otlp-mmap-example-server/Dockerfile`
- **Fixed:** Both base images now use `FROM python:3.11-alpine@sha256:303398d5c9f110790bce60d64f902e51e1a061e33292985c72bf6cd07960bf09`

**Additional Actions Taken:**
- Dependabot configured to monitor and update Docker base images weekly

---

### 3. Missing Checksum Verification for Downloaded Binaries

**Status:** ✅ **FIXED**  
**Severity:** MEDIUM-HIGH  
**Impact:** Risk of downloading tampered or malicious binaries  
**Affected File:** `specification/Dockerfile`

**Resolution:**
The protoc download process now includes SHA256 checksum verification:
```dockerfile
ARG PROTOBUF_SHA256=5cc0cf75a9c7d0ab6340d59fda3ca05d2efb2dcd7a323a5ba2b7211c2aba9d0f
RUN mkdir -p /protoc && \
    cd /protoc && \
    curl -sSL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    echo "${PROTOBUF_SHA256}  protoc.zip" | sha256sum -c - && \
    unzip protoc.zip && \
    rm protoc.zip && \
    chmod a+x /protoc/bin/protoc
```

The checksum is verified before extraction, ensuring the downloaded binary hasn't been tampered with. If the checksum doesn't match, the build will fail immediately.

---

## 🟠 Medium Priority Issues

### 4. Dockerfiles Running as Root User

**Status:** ✅ **FIXED**  
**Severity:** MEDIUM  
**Impact:** Increased attack surface if container is compromised  

**Resolution:**
All Dockerfiles now include non-root users:

#### `specification/Dockerfile`
```dockerfile
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser && \
    chown -R appuser:appuser /mmap /protoc
USER appuser
```

#### `java/otlp-mmap/Dockerfile`
```dockerfile
RUN groupadd -r -g 10001 appuser && \
    useradd -r -u 10001 -g appuser appuser
COPY --from=build --chown=appuser:appuser /build/target/scala-3.7.3/demo.jar /otel/demo.jar
USER appuser
```

#### `python/Dockerfile`
```dockerfile
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser
USER appuser
```

#### `python/otlp-mmap-example-server/Dockerfile`
```dockerfile
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser
USER appuser
```

All containers now run as UID 10001 (non-root), improving security posture.

---

## 🔵 Low Priority / Best Practice Issues

### 5. Dependency Management Best Practices

**Status:** ✅ **IMPROVED**  
**Severity:** LOW  
**Impact:** Improved supply chain security and reproducibility

**Actions Taken:**

#### GitHub Actions
- ✅ **Completed:** Dependabot configured for automated weekly updates
- ✅ **Completed:** All actions pinned to commit SHAs with version comments

#### Rust Dependencies (`rust/Cargo.toml`)
- ✅ **Completed:** Added automated security scanning with cargo-audit
- ✅ **Completed:** New CI workflow `.github/workflows/security-audit.yml` runs weekly
- ℹ️ **Info:** Cargo.lock exists and dependencies use specific versions

#### Docker Images
- ✅ **Completed:** Added Trivy vulnerability scanning in CI
- ✅ **Completed:** Trivy results uploaded to GitHub Security tab
- ✅ **Completed:** All images scanned on push and weekly schedule

#### Java Dependencies (`java/otlp-mmap/build.sbt`)
- ℹ️ **Note:** Dependencies have specific versions
- ℹ️ **Observation:** Version `1.54.1` for opentelemetry-exporter-otlp is older than other deps at `1.56.0`
- 💡 **Recommendation:** Consider aligning versions and adding `sbt-dependency-check` plugin in future

#### Python Dependencies
- ℹ️ **Current:** Dependencies installed via pip in Dockerfiles
- 💡 **Recommendation:** Consider adding `pip-audit` to CI pipeline in future
- ℹ️ **Note:** Dependabot configured for Python dependencies

---

## Recommended Implementation Priority

### ✅ Completed

1. **Critical Issues:**
   - [x] Pin all GitHub Actions to commit SHAs
   - [x] Set up Dependabot for automated updates

2. **High Priority:**
   - [x] Pin Docker base images to digests
   - [x] Improve binary download security (protoc)
   - [x] Add non-root users to all Dockerfiles

3. **Medium Priority:**
   - [x] Add cargo-audit to CI pipeline
   - [x] Add Trivy Docker scanning to CI
   - [x] Set up automated security workflows

### 💡 Future Recommendations

1. **Java Security (Low Priority):**
   - [ ] Add `sbt-dependency-check` plugin for Maven Central vulnerability scanning
   - [ ] Align all OpenTelemetry dependency versions to latest (1.56.0)

2. **Python Security (Low Priority):**
   - [ ] Consider adding `pip-audit` or `safety` to CI for Python dependency scanning
   - [ ] Add version pinning in pyproject.toml files

3. **Ongoing Maintenance:**
   - [ ] Monitor Dependabot PRs and merge security updates promptly
   - [ ] Review Trivy scan results in GitHub Security tab
   - [ ] Consider OSSF Scorecard for continuous SLSA compliance monitoring

---

## Automated Tools Recommendations

### For Continuous Monitoring:
1. **Dependabot/Renovate**: Automated dependency updates
   - Supports GitHub Actions, Docker, Cargo, npm, pip, Maven, sbt
   - Can auto-merge security patches

2. **OSSF Scorecard**: Supply chain security scoring
   - Checks for SLSA compliance
   - Provides actionable recommendations

3. **Trivy/Grype**: Container vulnerability scanning
   - Scans Docker images for CVEs
   - Can be integrated into CI/CD

4. **cargo-audit**: Rust vulnerability scanning
   - Checks Cargo dependencies against RustSec advisory DB

### Sample Dependabot Configuration:
```yaml
# .github/dependabot.yml
version: 2
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
    open-pull-requests-limit: 10

  - package-ecosystem: "docker"
    directory: "/specification"
    schedule:
      interval: "weekly"

  - package-ecosystem: "docker" 
    directory: "/java/otlp-mmap"
    schedule:
      interval: "weekly"

  - package-ecosystem: "docker"
    directory: "/python"
    schedule:
      interval: "weekly"

  - package-ecosystem: "docker"
    directory: "/python/otlp-mmap-example-server"
    schedule:
      interval: "weekly"

  - package-ecosystem: "cargo"
    directory: "/rust"
    schedule:
      interval: "weekly"

  - package-ecosystem: "maven"
    directory: "/java/otlp-mmap"
    schedule:
      interval: "weekly"

  - package-ecosystem: "pip"
    directory: "/python"
    schedule:
      interval: "weekly"
```

---

## Compliance Status

### SLSA Level Requirements:
- **Level 1** (Documentation of build): ✅ **Met** - CI/CD workflows documented and pinned
- **Level 2** (Build service): ✅ **Met** - GitHub Actions with proper permissions and pinned dependencies
- **Level 3** (Hardened builds): ⚠️ **Partially Met** - Actions pinned to SHA, provenance enabled, but additional hardening possible
- **Level 4** (Two-party review): ⚠️ **Unknown** - Depends on repository settings and PR review policies

### CIS Docker Benchmark:
- **4.1** (Create user for container): ✅ **Passed** - All containers run as non-root (UID 10001)
- **4.2** (Use trusted base images): ✅ **Passed** - Official images pinned to digests
- **4.7** (Do not install unnecessary packages): ✅ **Passed** - Minimal package installations
- **4.9** (Use COPY instead of ADD): ✅ **Passed** - COPY used throughout

### Security Scanning:
- **Rust Dependencies:** ✅ **Automated** - cargo-audit runs weekly and on every push
- **Docker Images:** ✅ **Automated** - Trivy scans all images weekly and on every push
- **GitHub Actions:** ✅ **Automated** - Dependabot monitors and updates weekly

### Overall Security Posture:
**Significantly Improved** - All critical and high-priority issues addressed. The repository now follows SLSA best practices and has automated security monitoring in place.

---

## Additional Resources

- [SLSA Framework](https://slsa.dev/)
- [OpenSSF Best Practices](https://www.bestpractices.dev/)
- [CIS Docker Benchmark](https://www.cisecurity.org/benchmark/docker)
- [GitHub Actions Security Hardening](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
- [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)

---

## Contact

For questions about this audit or remediation steps, please contact the security team or open an issue in the repository.
