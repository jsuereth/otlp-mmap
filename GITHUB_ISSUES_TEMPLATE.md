# SLSA Security Audit - GitHub Issues Summary

This document provides pre-formatted GitHub issue templates for the remaining security recommendations from the SLSA audit.

---

## Issue 1: Add sbt-dependency-check Plugin for Maven Security Scanning

**Title:** Add sbt-dependency-check plugin for Java dependency vulnerability scanning

**Labels:** `security`, `dependencies`, `java`, `enhancement`

**Priority:** Low

**Description:**

As part of our SLSA compliance improvements, we should add automated vulnerability scanning for Java/Scala dependencies.

### Current State
- Dependencies in `java/otlp-mmap/build.sbt` use specific versions
- No automated vulnerability checking for Maven/SBT dependencies
- Minor version inconsistency: `opentelemetry-exporter-otlp` at 1.54.1 vs other OpenTelemetry deps at 1.56.0

### Proposed Solution
Add the [sbt-dependency-check](https://github.com/albuch/sbt-dependency-check) plugin to scan dependencies against the National Vulnerability Database (NVD).

#### Implementation Steps:
1. Add to `project/plugins.sbt`:
```scala
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "5.1.0")
```

2. Run `sbt dependencyCheck` to scan for vulnerabilities

3. Consider adding to CI workflow for automated scanning

4. Align OpenTelemetry dependency versions to 1.56.0

### Benefits
- Automated detection of known vulnerabilities in dependencies
- Continuous monitoring as part of CI/CD pipeline
- Compliance with SLSA best practices

### References
- [SLSA Security Audit Report](./SECURITY_AUDIT_REPORT.md)
- [sbt-dependency-check Plugin](https://github.com/albuch/sbt-dependency-check)

---

## Issue 2: Add Python Dependency Security Scanning

**Title:** Add pip-audit or safety for Python dependency vulnerability scanning

**Labels:** `security`, `dependencies`, `python`, `enhancement`

**Priority:** Low

**Description:**

To further improve our supply chain security posture, we should add automated vulnerability scanning for Python dependencies.

### Current State
- Python dependencies installed via pip in Dockerfiles
- Dependabot configured to monitor Python dependencies
- No automated vulnerability scanning in CI

### Proposed Solution
Add [pip-audit](https://github.com/pypa/pip-audit) or [safety](https://github.com/pyupio/safety) to scan Python dependencies for known vulnerabilities.

#### Option 1: pip-audit (Recommended)
```yaml
- name: Python Security Audit
  run: |
    pip install pip-audit
    pip-audit --requirement python/requirements.txt
```

#### Option 2: safety
```yaml
- name: Python Security Audit
  run: |
    pip install safety
    safety check --file python/requirements.txt
```

#### Implementation Steps:
1. Create `requirements.txt` files with pinned versions for reproducibility
2. Add pip-audit job to `.github/workflows/security-audit.yml`
3. Configure to run on push, pull request, and weekly schedule
4. Consider adding to pre-commit hooks

### Benefits
- Detect known vulnerabilities in Python dependencies
- Early warning for security issues
- Improved SLSA compliance

### References
- [SLSA Security Audit Report](./SECURITY_AUDIT_REPORT.md)
- [pip-audit](https://github.com/pypa/pip-audit)
- [safety](https://github.com/pyupio/safety)

---

## Issue 3: Consider OSSF Scorecard for Continuous SLSA Monitoring

**Title:** Implement OSSF Scorecard for continuous supply chain security monitoring

**Labels:** `security`, `slsa`, `monitoring`, `enhancement`

**Priority:** Low

**Description:**

The [OpenSSF Scorecard](https://securityscorecards.dev/) provides automated security assessments for open source projects, checking for SLSA compliance and best practices.

### What is OSSF Scorecard?
OSSF Scorecard evaluates a project across multiple security checks including:
- Pinned dependencies
- Code review practices
- Vulnerability disclosure
- Branch protection
- And many more...

### Benefits
- Automated SLSA compliance monitoring
- Security badge for README
- Actionable recommendations
- Community trust signal

### Implementation
Add the OSSF Scorecard action to a new workflow:

```yaml
name: Scorecard Analysis
on:
  branch_protection_rule:
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday
  push:
    branches: [main]

permissions: read-all

jobs:
  analysis:
    name: Scorecard analysis
    runs-on: ubuntu-latest
    permissions:
      security-events: write
      id-token: write
      
    steps:
      - name: "Checkout code"
        uses: actions/checkout@v4
        with:
          persist-credentials: false
          
      - name: "Run analysis"
        uses: ossf/scorecard-action@v2
        with:
          results_file: results.sarif
          results_format: sarif
          publish_results: true
          
      - name: "Upload to code-scanning"
        uses: github/codeql-action/upload-sarif@v2
        with:
          sarif_file: results.sarif
```

### Next Steps
1. Review scorecard checks at https://securityscorecards.dev/
2. Add the workflow above
3. Review initial results and address any findings
4. Add badge to README.md

### References
- [SLSA Security Audit Report](./SECURITY_AUDIT_REPORT.md)
- [OSSF Scorecard](https://securityscorecards.dev/)
- [Scorecard GitHub Action](https://github.com/ossf/scorecard-action)

---

## Summary of Completed Improvements

The following security improvements have been completed:

### ✅ Critical Issues Fixed
1. **GitHub Actions Pinned to SHA** - All actions use immutable commit references
2. **Dependabot Configured** - Automated updates for all ecosystems
3. **Docker Images Pinned** - All base images use SHA256 digests
4. **Non-Root Users Added** - All containers run as UID 10001
5. **Automated Security Scanning** - cargo-audit and Trivy in CI

### 📊 Current SLSA Compliance
- **Level 1:** ✅ Met
- **Level 2:** ✅ Met
- **Level 3:** ⚠️ Partially Met (further hardening possible)

### 🔒 CIS Docker Benchmark
- All critical checks passing
- Non-root users in all containers
- Trusted, pinned base images
- Minimal attack surface

For full details, see [SECURITY_AUDIT_REPORT.md](./SECURITY_AUDIT_REPORT.md).
