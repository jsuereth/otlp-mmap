This project is built for OpenTelemetry.  All contributions will happen in that space after evaluating this concept.

## Dependency Management

This repository uses [Renovate](https://docs.renovatebot.com/) for automated dependency updates across all supported ecosystems:

- **SBT**: Java/Scala dependencies, plugins, and build properties in `java/otlp-mmap`.
- **Docker**: Base image tags and digests across Dockerfiles and Compose files in `scenarios/`.
- **Rust**: Cargo packages and workspace lockfiles in `rust/`.
- **Python**: Dependencies defined in PEP 621 `pyproject.toml` files.
- **GitHub Actions**: Actions pinned in `.github/workflows/`.

### Grouped Updates

To avoid PR noise and keep integration seamless, Renovate is configured to batch dependency bumps into **a single combined pull request** rather than creating separate PRs for each individual dependency.

The configuration file is located at [`.github/renovate.json`](.github/renovate.json).