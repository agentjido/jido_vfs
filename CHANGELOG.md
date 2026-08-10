# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Hardened the public API contract to return deterministic shapes only:
  - `:ok`
  - `{:ok, value}`
  - `{:error, %Jido.VFS.Errors.*{}}`
- Standardized unsupported operation errors to `%Jido.VFS.Errors.UnsupportedOperation{operation, adapter}`.
- Added `Jido.VFS.supports?/2` for explicit adapter capability checks.
- Standardized `Jido.VFS.revisions/3` output to `%Jido.VFS.Revision{}` across versioned adapters.
- Removed legacy `:hako` runtime config reads; adapters now use `:jido_vfs` only.
- Hardened cross-filesystem copy fallback to use capability checks and tempfile spooling for bounded memory.

### Fixed
- Normalized adapter error mapping to avoid raw string/atom leaks from public API paths.
- Fixed `InvalidPath` construction in Local adapter to use the `invalid_path:` field.
- Hardened ETS version storage to avoid dynamic atom creation.
- Improved S3 edge behavior:
  - paginated list/delete for large object sets
  - multipart upload abort on halted/error flows
  - prefix-scoped clear semantics
  - path-scoped visibility resolution
- Hardened GitHub API error mapping for malformed content and API failures.

### Migration Notes
- `{:error, :unsupported}` has been replaced by `%Jido.VFS.Errors.UnsupportedOperation{}`.
- Callers that inferred adapter capabilities from error payloads should use `Jido.VFS.supports?/2`.
- `revisions/3` consumers should expect `%Jido.VFS.Revision{}` values (with `sha` populated for all versioned adapters).
- Runtime configuration for Git/GitHub must use `:jido_vfs` app env keys.

## [v1.0.1](https://github.com/agentjido/jido_vfs/compare/v1.0.0...v1.0.1) (2026-08-10)




### Bug Fixes:

* release: configure git_ops by mikehostetler

* release: add git_ops dependency by mikehostetler

* deps: update Mint for CVE-2026-59249 by mikehostetler

* deps: resolve hex audit advisories by dependabot[bot]

* harden vfs wrapper and streaming edge cases by mikehostetler

## [1.0.0] - 2024-12-24

### Added
- Git adapter with full versioning support (commit, revisions, rollback, read_revision)
- GitHub adapter for remote repository access via GitHub API
- ETS adapter with versioning support
- InMemory adapter with versioning support
- Polymorphic versioning interface in main Jido.VFS module
- Comprehensive integration test suites for all adapters
- GitHub Actions CI/CD workflows

### Changed
- Updated Elixir version requirement to ~> 1.11
- Updated mix.exs to follow Jido ecosystem package standards
- Replaced Briefly test dependency with ExUnit's :tmp_dir

### Fixed
- Path traversal security checks

## [0.5.2] - 2021-09-16

### Added
- Added WIP visibility handling with callbacks and converters
- Runtime configuration for filesystems via the app env

## [0.5.1] - 2021-09-12

### Changed
- `Jido.VFS.RelativePath.join_prefix` does make sure trailing slashes are retained

## [0.5.0] - 2020-08-16

### Added
- New `Jido.VFS.Filesystem` callback `copy/4` to implement copy between filesystems
- New `Jido.VFS.Filesystem` callback `file_exists/2`
- New `Jido.VFS.Filesystem` callback `list_contents/2`
- New `Jido.VFS.Filesystem` callback `create_directory/2`
- New `Jido.VFS.Filesystem` callback `delete_directory/2`
- New `Jido.VFS.Filesystem` callback `clear/1`

## [0.4.0] - 2020-07-31

### Added
- New `Jido.VFS.Filesystem` callback `copy/4` to implement copy between filesystems

## [0.3.0] - 2020-07-29

### Added
- New `Jido.VFS.Filesystem` callback `read_stream/2`
- Added `:otp_app` key to `use Jido.VFS.Filesystem` macro to be able to store settings in config files
