# SLSA Security Audit Report

**Date:** 2026-02-14  
**Repository:** jsuereth/otlp-mmap  
**Auditor:** SLSA Security Agent

## Executive Summary

This security audit identified multiple SLSA compliance issues in the otlp-mmap repository. The issues range from unpinned GitHub Actions to insecure Dockerfile practices. All issues have been categorized by severity and include specific remediation steps.

---

## 🔴 Critical Issues

### 1. GitHub Actions Not Pinned to Commit SHA (Supply Chain Attack Risk)

**Severity:** HIGH  
**Impact:** Allows potential supply chain attacks via compromised action versions  
**Affected File:** `.github/workflows/docker-build.yml`

**Details:**
All GitHub Actions use mutable tag references (v3, v4, v5) instead of immutable commit SHA hashes. This violates SLSA Level 3 requirements and exposes the build process to supply chain attacks.

**Affected Lines:**
- Line 42: `actions/checkout@v4` 
- Line 45: `docker/setup-buildx-action@v3`
- Line 48: `docker/login-action@v3`
- Line 56: `docker/metadata-action@v5`
- Line 67: `docker/build-push-action@v5`

**Remediation:**
Pin all actions to specific commit SHA hashes. Use Dependabot or Renovate bot to keep them updated.

```yaml
# BEFORE:
- uses: actions/checkout@v4

# AFTER:
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2
```

**Recommended Actions:**
1. Pin all actions to commit SHAs
2. Add comments with the version tag for human readability
3. Set up Renovate or Dependabot to automatically update pinned actions
4. Consider using `.github/dependabot.yml` configuration:

```yaml
version: 2
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
```

---

## 🟡 High Priority Issues

### 2. Docker Base Images Not Pinned to Digest (Reproducibility Risk)

**Severity:** MEDIUM-HIGH  
**Impact:** Non-reproducible builds, potential for base image tampering  

**Affected Files:**

#### `specification/Dockerfile`
- **Line 4:** `FROM rust:1-alpine3.22`
  - **Risk:** Mutable tag, image can change without notice
  - **Fix:** Pin to digest: `FROM rust:1-alpine3.22@sha256:<digest>`

#### `java/otlp-mmap/Dockerfile`
- **Line 1:** `FROM sbtscala/scala-sbt:eclipse-temurin-25_36_1.11.7_3.7.3 AS build`
  - **Risk:** Mutable tag, image can change without notice
  - **Fix:** Pin to digest: `FROM sbtscala/scala-sbt:eclipse-temurin-25_36_1.11.7_3.7.3@sha256:<digest>`
- **Line 15:** `FROM eclipse-temurin:25`
  - **Risk:** Uses generic tag without specific version or digest
  - **Fix:** Pin to digest: `FROM eclipse-temurin:25@sha256:<digest>`

#### `python/Dockerfile`
- **Line 2:** `FROM python:3.11-alpine`
  - **Risk:** Mutable tag, image can change without notice
  - **Fix:** Pin to digest: `FROM python:3.11-alpine@sha256:<digest>`

#### `python/otlp-mmap-example-server/Dockerfile`
- **Line 2:** `FROM python:3.11-alpine AS builder`
- **Line 35:** `FROM python:3.11-alpine`
  - **Risk:** Mutable tag, image can change without notice
  - **Fix:** Pin to digest: `FROM python:3.11-alpine@sha256:<digest>`

**How to get digests:**
```bash
# For Docker Hub images
docker pull rust:1-alpine3.22
docker inspect --format='{{index .RepoDigests 0}}' rust:1-alpine3.22

# Or use crane
crane digest rust:1-alpine3.22
```

---

### 3. Missing Checksum Verification for Downloaded Binaries

**Severity:** MEDIUM-HIGH  
**Impact:** Risk of downloading tampered or malicious binaries  
**Affected File:** `specification/Dockerfile`

**Details:**
Line 11 downloads the protoc binary without verifying its checksum:

```dockerfile
curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip | unzip -
```

**Remediation:**
Always verify checksums for downloaded binaries:

```dockerfile
RUN PROTOBUF_SHA256="expected_sha256_here" && \
    cd /protoc && \
    curl -sSL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    echo "${PROTOBUF_SHA256}  protoc.zip" | sha256sum -c - && \
    unzip protoc.zip && \
    rm protoc.zip
```

Alternatively, use the official protoc image or install from apk with:
```dockerfile
RUN apk add --no-cache protoc
```

---

## 🟠 Medium Priority Issues

### 4. Dockerfiles Running as Root User

**Severity:** MEDIUM  
**Impact:** Increased attack surface if container is compromised  
**Affected Files:** All Dockerfiles

**Details:**
None of the Dockerfiles specify a non-root user. All containers run as root (UID 0) by default.

**Affected Dockerfiles:**
1. `specification/Dockerfile` - No USER directive
2. `java/otlp-mmap/Dockerfile` - No USER directive  
3. `python/Dockerfile` - No USER directive
4. `python/otlp-mmap-example-server/Dockerfile` - No USER directive

**Remediation:**
Add a non-root user to each Dockerfile:

```dockerfile
# Create non-root user
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser

# ... rest of dockerfile ...

# Switch to non-root user before CMD/ENTRYPOINT
USER appuser

CMD ["your-command"]
```

**Special Considerations:**
- Ensure the non-root user has access to any required directories/files
- Update WORKDIR ownership: `RUN chown -R appuser:appuser /app`
- Some operations (like binding to ports <1024) may require capabilities or higher ports

---

## 🔵 Low Priority / Best Practice Issues

### 5. Dependency Management Best Practices

**Severity:** LOW  
**Impact:** Improved supply chain security and reproducibility

**Recommendations:**

#### For GitHub Actions
- **Current State:** Good - permissions are explicitly defined with least privilege
- **Improvement:** Consider adding Dependabot/Renovate for automated dependency updates

#### For Rust Dependencies (`rust/Cargo.toml`)
- **Current State:** Good - Cargo.lock exists and dependencies use specific versions
- **Improvement:** Consider using `cargo-audit` in CI to check for known vulnerabilities
- **Action:** Add a CI job:
  ```yaml
  - name: Security audit
    run: |
      cargo install cargo-audit
      cargo audit
  ```

#### For Java Dependencies (`java/otlp-mmap/build.sbt`)
- **Current State:** Dependencies have specific versions
- **Concern:** Version `1.54.1` for opentelemetry-exporter-otlp is older than other deps at `1.56.0`
- **Action:** Align versions and consider using `sbt-dependency-check` plugin

#### For Python Dependencies
- **Current State:** Dependencies installed via pip without version constraints
- **Risk:** Non-reproducible builds
- **Action:** Use requirements.txt with pinned versions or update pyproject.toml with version constraints

---

## Recommended Implementation Priority

1. **Immediate (Critical):**
   - [ ] Pin all GitHub Actions to commit SHAs
   - [ ] Set up Dependabot/Renovate for automated updates

2. **High Priority (Within 1 Sprint):**
   - [ ] Pin Docker base images to digests
   - [ ] Add checksum verification for downloaded binaries
   - [ ] Add non-root users to all Dockerfiles

3. **Medium Priority (Within 2 Sprints):**
   - [ ] Add cargo-audit to CI pipeline
   - [ ] Set up dependency scanning for Java/Python
   - [ ] Document security practices in SECURITY.md

4. **Ongoing:**
   - [ ] Monitor and update dependencies regularly
   - [ ] Review new Dockerfiles/Actions for compliance
   - [ ] Consider OSSF Scorecard for continuous monitoring

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
- **Level 1** (Documentation of build): ✅ Partially Met (CI documented)
- **Level 2** (Build service): ⚠️ Partially Met (GitHub Actions used, but not fully pinned)
- **Level 3** (Hardened builds): ❌ Not Met (Actions not pinned to SHA, no provenance verification)
- **Level 4** (Two-party review): ⚠️ Unknown (depends on repository settings)

### CIS Docker Benchmark:
- **4.1** (Create user for container): ❌ Failed (all run as root)
- **4.2** (Use trusted base images): ⚠️ Partial (official images but not pinned)
- **4.7** (Do not install unnecessary packages): ✅ Passed
- **4.9** (Use COPY instead of ADD): ✅ Passed

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
