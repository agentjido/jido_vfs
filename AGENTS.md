# Agent Guide for Jido.VFS

## Commands
- **Test all**: `mix test`
- **Test single file**: `mix test test/jido_vfs_test.exs`  
- **Test single test**: `mix test test/jido_vfs_test.exs:123` (line number)
- **Quality check**: `mix quality` (format, compile, dialyzer, credo)
- **Format code**: `mix format`
- **Type check**: `mix dialyzer`
- **Lint**: `mix credo`
- **Coverage**: `mix coveralls`
- **Docs**: `mix docs`
- **Download Minio**: `MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest` (for ARM Macs)

## Architecture
- **Core**: Filesystem abstraction library with adapter pattern
- **Adapters**: `lib/jido_vfs/adapter/` - Local, InMemory, ETS, S3, Git, GitHub adapters
- **Main API**: `lib/jido_vfs.ex` - Unified filesystem operations (read, write, copy, move, delete)
- **Versioning**: Git, ETS, and InMemory adapters support versioning (commit, revisions, rollback)
- **Support**: Virtual filesystem, stat structs, visibility controls, relative path handling
- **Test setup**: Minio server for S3 testing, tmp_dir fixtures, async tests

## Adapters

| Adapter | Description | Features |
|---------|-------------|----------|
| `Jido.VFS.Adapter.Local` | Local filesystem | Standard file operations |
| `Jido.VFS.Adapter.InMemory` | In-memory storage | Testing, ephemeral data, versioning |
| `Jido.VFS.Adapter.ETS` | ETS-backed storage | Persistence, versioning |
| `Jido.VFS.Adapter.S3` | AWS S3 / Minio | Cloud storage, streaming |
| `Jido.VFS.Adapter.Git` | Git repository | Version control, commit history |
| `Jido.VFS.Adapter.GitHub` | GitHub API | Remote repo access, commits via API |

## Code Style
- Use `mix format` for consistent formatting (configured in `.formatter.exs`)
- Follow Elixir naming: snake_case for functions/variables, PascalCase for modules  
- Pattern match with `{:ok, result}` | `{:error, reason}` tuples
- Prefer `with` statements for error handling chains
- Use `@spec` type annotations for public functions
- Test with ExUnit, use `assert_in_list/2` macro for list assertions
- Group related tests in `describe` blocks with setup context

## Release Hygiene

- Do not modify `CHANGELOG.md`; release notes are generated from Git history during release, so keep changes focused on proper Conventional Commits.
