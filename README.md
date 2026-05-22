# Jido.VFS

[![Hex.pm](https://img.shields.io/hexpm/v/jido_vfs.svg)](https://hex.pm/packages/jido_vfs)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/jido_vfs/)
[![CI](https://github.com/agentjido/jido_vfs/actions/workflows/ci.yml/badge.svg)](https://github.com/agentjido/jido_vfs/actions/workflows/ci.yml)
[![License](https://img.shields.io/hexpm/l/jido_vfs.svg)](https://github.com/agentjido/jido_vfs/blob/main/LICENSE.md)
[![Website](https://img.shields.io/badge/website-jido.run-0f172a.svg)](https://jido.run)
[![Ecosystem](https://img.shields.io/badge/ecosystem-jido.run-0ea5e9.svg)](https://jido.run/ecosystem)
[![Discord](https://img.shields.io/badge/discord-join-5865F2.svg?logo=discord&logoColor=white)](https://jido.run/discord)

<!-- MDOC !-->

Jido.VFS is a filesystem abstraction for Elixir providing a unified interface over many storage backends. It allows you to swap out filesystems on the fly without needing to rewrite your application code. Eliminate vendor lock-in, reduce technical debt, and improve testability.

## Features

- **Unified API** - Same operations work across all adapters
- **Multiple Backends** - Local, S3, Sprite, Git, GitHub, ETS, and InMemory storage
- **Version Control** - Git, Sprite, ETS, and InMemory adapters support versioning
- **Streaming** - Efficient handling of large files
- **Cross-Filesystem Operations** - Copy files between different storage backends
- **Visibility Controls** - Public/private file permissions

## Contract Guarantees (V1)

- Public API return shape is deterministic: `:ok`, `{:ok, value}`, or `{:error, %Jido.VFS.Errors.*{}}`
- Unsupported operations always return `%Jido.VFS.Errors.UnsupportedOperation{operation, adapter}`
- Paths are normalized before adapter calls and traversal/absolute paths are rejected with typed errors
- Cross-filesystem copy is memory-bounded: native copy if available, then streaming, then tempfile spooling fallback
- Versioned adapters (`Git`, `ETS`, `InMemory`) return `%Jido.VFS.Revision{}` values from `revisions/3`

## Adapters

| Adapter | Use Case | Streaming | Versioning | Notes |
|---------|----------|-----------|------------|-------|
| `Jido.VFS.Adapter.Local` | Local filesystem | Yes | No | Full local filesystem operations |
| `Jido.VFS.Adapter.S3` | AWS S3 / Minio | Yes | No | Prefix-scoped clear/listing, multipart stream uploads |
| `Jido.VFS.Adapter.Sprite` | Fly.io Sprites | Yes | Yes | Shell-command filesystem + checkpoint-backed versioning |
| `Jido.VFS.Adapter.Git` | Git repositories | Yes | Yes | Deterministic bootstrap commit, rollback support |
| `Jido.VFS.Adapter.GitHub` | GitHub API | No | No | Remote read/write via API, typed unsupported ops |
| `Jido.VFS.Adapter.ETS` | ETS tables | Yes | Yes | Version storage hardened without dynamic atoms |
| `Jido.VFS.Adapter.InMemory` | Testing | Yes | Yes | Ephemeral storage with version snapshots |

## Capability Checks

Use `supports?/2` before optional operations:

```elixir
if Jido.VFS.supports?(filesystem, :write_stream) do
  {:ok, stream} = Jido.VFS.write_stream(filesystem, "large.bin")
  Stream.into(data_stream, stream) |> Stream.run()
else
  :ok = Jido.VFS.write(filesystem, "large.bin", IO.iodata_to_binary(Enum.to_list(data_stream)))
end
```

This avoids relying on matching adapter error payloads to determine capabilities.

## Quick Start

```elixir
# Direct filesystem configuration
filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")

# Write and read files
:ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
{:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")

# Module-based filesystem (recommended for reuse)
defmodule MyStorage do
  use Jido.VFS.Filesystem,
    adapter: Jido.VFS.Adapter.Local,
    prefix: "/home/user/storage"
end

MyStorage.write("test.txt", "Hello World")
{:ok, "Hello World"} = MyStorage.read("test.txt")
```

## Local Adapter

The Local adapter provides standard filesystem operations:

```elixir
filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/path/to/storage")

# Basic operations
:ok = Jido.VFS.write(filesystem, "file.txt", "content")
{:ok, content} = Jido.VFS.read(filesystem, "file.txt")
:ok = Jido.VFS.delete(filesystem, "file.txt")

# Copy and move
:ok = Jido.VFS.copy(filesystem, "source.txt", "dest.txt")
:ok = Jido.VFS.move(filesystem, "old.txt", "new.txt")

# Directory operations
:ok = Jido.VFS.create_directory(filesystem, "new-folder")
{:ok, entries} = Jido.VFS.list_contents(filesystem, "folder/")
:ok = Jido.VFS.delete_directory(filesystem, "old-folder")

# File info
{:ok, stat} = Jido.VFS.stat(filesystem, "file.txt")
{:ok, :exists} = Jido.VFS.file_exists(filesystem, "file.txt")
```

## S3 Adapter

The S3 adapter works with AWS S3, Minio, and S3-compatible storage:

```elixir
# Configure S3 filesystem
filesystem = Jido.VFS.Adapter.S3.configure(
  bucket: "my-bucket",
  prefix: "uploads/",
  region: "us-east-1"
)

# For Minio or custom S3-compatible storage
filesystem = Jido.VFS.Adapter.S3.configure(
  bucket: "my-bucket",
  host: "localhost",
  port: 9000,
  scheme: "http://",
  access_key_id: "minioadmin",
  secret_access_key: "minioadmin"
)

# All standard operations work
:ok = Jido.VFS.write(filesystem, "document.pdf", pdf_binary)
{:ok, content} = Jido.VFS.read(filesystem, "document.pdf")

# Streaming for large files
{:ok, stream} = Jido.VFS.read_stream(filesystem, "large-file.bin", chunk_size: 65536)
Enum.each(stream, fn chunk -> process(chunk) end)
```

## Sprite Adapter

The Sprite adapter executes shell commands on a [Fly.io Sprite](https://sprites.dev)
through `sprites-ex`.

```elixir
# Add sprites-ex in your project if needed
# {:sprites, github: "superfly/sprites-ex"}

filesystem =
  Jido.VFS.Adapter.Sprite.configure(
    sprite_name: "my-sprite",
    token: System.fetch_env!("SPRITES_TOKEN"),
    root: "/workspace",
    encoding: :base64
  )

:ok = Jido.VFS.write(filesystem, "notes/hello.txt", "hello sprite")
{:ok, "hello sprite"} = Jido.VFS.read(filesystem, "notes/hello.txt")
{:ok, entries} = Jido.VFS.list_contents(filesystem, "notes")
```

- `encoding: :base64` is binary-safe and default.
- `encoding: :raw` is text-oriented and avoids base64 overhead.
- Pass `create_on_demand: true` to call Sprite create during configure.

Sprite versioning is backed by Sprite checkpoints:

```elixir
:ok = Jido.VFS.write(filesystem, "doc.txt", "v1")
:ok = Jido.VFS.commit(filesystem, "checkpoint v1")

:ok = Jido.VFS.write(filesystem, "doc.txt", "v2")
:ok = Jido.VFS.commit(filesystem, "checkpoint v2")

{:ok, revisions} = Jido.VFS.revisions(filesystem, "doc.txt")
old_revision = List.last(revisions)

{:ok, "v1"} = Jido.VFS.read_revision(filesystem, "doc.txt", old_revision.sha)
:ok = Jido.VFS.rollback(filesystem, old_revision.sha, path: "doc.txt")
```

## Git Adapter

The Git adapter provides version-controlled filesystem operations:

```elixir
# Manual commit mode - you control when commits happen
filesystem = Jido.VFS.Adapter.Git.configure(
  path: "/path/to/repo",
  mode: :manual,
  author: [name: "Bot", email: "bot@example.com"]
)

# Write files and commit manually
Jido.VFS.write(filesystem, "document.txt", "Version 1")
Jido.VFS.write(filesystem, "notes.txt", "Some notes")
:ok = Jido.VFS.commit(filesystem, "Add initial documents")

# Auto-commit mode - each write creates a commit
filesystem = Jido.VFS.Adapter.Git.configure(
  path: "/path/to/repo",
  mode: :auto
)
Jido.VFS.write(filesystem, "file.txt", "content")  # Automatically committed

# View revision history
{:ok, revisions} = Jido.VFS.revisions(filesystem, "document.txt")

# Read historical versions
{:ok, old_content} = Jido.VFS.read_revision(filesystem, "document.txt", revision_sha)

# Rollback to a previous revision
:ok = Jido.VFS.rollback(filesystem, revision_sha)
```

## GitHub Adapter

The GitHub adapter allows you to interact with GitHub repositories as a virtual filesystem:

```elixir
# Read-only access to public repos
filesystem = Jido.VFS.Adapter.GitHub.configure(
  owner: "octocat",
  repo: "Hello-World",
  ref: "main"
)

{:ok, content} = Jido.VFS.read(filesystem, "README.md")
{:ok, files} = Jido.VFS.list_contents(filesystem, "src/")

# Authenticated access for write operations
filesystem = Jido.VFS.Adapter.GitHub.configure(
  owner: "your-username",
  repo: "your-repo",
  ref: "main",
  auth: %{access_token: "ghp_your_token"},
  commit_info: %{
    message: "Update via Jido.VFS",
    committer: %{name: "Your Name", email: "you@example.com"},
    author: %{name: "Your Name", email: "you@example.com"}
  }
)

# Write files (creates commits)
Jido.VFS.write(filesystem, "new_file.txt", "Hello GitHub!", 
  message: "Add new file via Jido.VFS")
```

## ETS and InMemory Adapters

These adapters are ideal for testing and caching:

```elixir
# ETS adapter - persists to ETS table
filesystem = Jido.VFS.Adapter.ETS.configure(name: :my_cache)

# InMemory adapter - ephemeral storage
filesystem = Jido.VFS.Adapter.InMemory.configure(name: :test_fs)

# Both support versioning
Jido.VFS.write(filesystem, "file.txt", "v1")
:ok = Jido.VFS.commit(filesystem, "Version 1")

Jido.VFS.write(filesystem, "file.txt", "v2")
:ok = Jido.VFS.commit(filesystem, "Version 2")

{:ok, revisions} = Jido.VFS.revisions(filesystem, "file.txt")
{:ok, "v1"} = Jido.VFS.read_revision(filesystem, "file.txt", first_revision_id)
```

## Cross-Filesystem Operations

Copy files between different storage backends:

```elixir
local_fs = Jido.VFS.Adapter.Local.configure(prefix: "/local/storage")
s3_fs = Jido.VFS.Adapter.S3.configure(bucket: "my-bucket")

# Copy from local to S3
:ok = Jido.VFS.copy_between_filesystem(
  {local_fs, "document.pdf"},
  {s3_fs, "uploads/document.pdf"}
)

# Copy from S3 to local
:ok = Jido.VFS.copy_between_filesystem(
  {s3_fs, "backup.zip"},
  {local_fs, "downloads/backup.zip"}
)
```

`copy_between_filesystem/3` prefers native adapter copy, then stream-based copy, and finally tempfile spooling for bounded-memory fallback behavior.

## Streaming

Efficiently handle large files with streaming:

```elixir
# Read stream
{:ok, stream} = Jido.VFS.read_stream(filesystem, "large-file.bin", chunk_size: 65536)
Enum.each(stream, fn chunk -> process(chunk) end)

# Write stream
{:ok, stream} = Jido.VFS.write_stream(filesystem, "output.bin")
data |> Stream.into(stream) |> Stream.run()
```

## Visibility

Control file permissions with visibility settings:

```elixir
# Write with visibility
:ok = Jido.VFS.write(filesystem, "public-file.txt", "content", visibility: :public)
:ok = Jido.VFS.write(filesystem, "private-file.txt", "secret", visibility: :private)

# Get/set visibility
{:ok, :public} = Jido.VFS.visibility(filesystem, "public-file.txt")
:ok = Jido.VFS.set_visibility(filesystem, "file.txt", :private)
```

<!-- MDOC !-->

## Installation

### Igniter Installation
If your project has [Igniter](https://hexdocs.pm/igniter/readme.html) available, 
you can install Jido VFS using the command 

```bash
mix igniter.install jido_vfs
```

### Manual Installation

Add `jido_vfs` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:jido_vfs, "~> 1.0"}
  ]
end
```

## Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/jido_vfs).

Additional guides:

- [Jido Agent Integration](docs/jido-agent-integration.md)

## Contributing

Adapter authors should follow the
[Adapter Onboarding Checklist](docs/adapter-onboarding-checklist.md)
to ensure metadata callbacks, typed errors, and contract coverage are complete.

## License

Apache-2.0 - see [LICENSE.md](LICENSE.md) for details.

## Package Purpose

`jido_vfs` is the filesystem abstraction layer for Jido packages, with adapters for local and remote storage backends.

For Jido agents, prefer wrapping `jido_vfs` with `Jido.Action` modules in a higher-level package or application rather than adding `jido_action` as a required dependency here. See the
[Jido Agent Integration](docs/jido-agent-integration.md) guide.

## Testing Paths

- Unit/adapter tests: `mix test`
- Full quality gate: `mix quality`
- Release preflight: `mix release.ready`
- Optional flaky cases: `mix test --include flaky`
