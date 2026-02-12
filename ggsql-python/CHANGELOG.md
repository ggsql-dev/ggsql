# Changelog

All notable changes to the ggsql Python package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Python package releases are tagged as `py/v<version>` (e.g., `py/v0.1.0`).
See [RELEASING.md](RELEASING.md) for release instructions.

## [Unreleased]

## [0.1.1] - 2026-02-11

### Fixed

- `DuckDBReader.register` no longer panics on DataFrames with more than 2048 rows. The method now chunks large DataFrames to work around a bug in `duckdb-rs`'s Arrow virtual table implementation.

## [0.1.0]

Initial version of ggsql.
