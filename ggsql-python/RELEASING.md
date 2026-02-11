# Releasing the Python Package

## To release a new version

1. Bump `version` in `pyproject.toml` (this is the source of truth for the
   Python package version; the `Cargo.toml` version is independent).
2. Update `CHANGELOG.md`: move items from `[Unreleased]` to a new version heading.
3. Commit the version bump and changelog update.
4. Tag the commit: `git tag py/v0.2.0`
5. Push the commit and tag: `git push && git push origin py/v0.2.0`
6. CI builds wheels for all platforms and publishes to PyPI.
