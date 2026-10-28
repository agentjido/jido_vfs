# Contributing to Jido.VFS

Welcome to the Jido.VFS contributor's guide! We're excited that you're interested in contributing to Jido.VFS, a filesystem abstraction library for Elixir.

## Getting Started

### Development Environment

1. **Elixir Version Requirements**
   - Jido.VFS requires Elixir ~> 1.18
   - We recommend using asdf or similar version manager

2. **Initial Setup**
   ```bash
   # Clone the repository
   git clone https://github.com/agentjido/jido_vfs.git
   cd jido_vfs

   # Install dependencies
   mix deps.get

   # Run tests to verify your setup
   mix test
   ```

3. **S3 Testing Setup (Optional)**
   ```bash
   # Download Minio for S3 adapter testing
   MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest
   ```

4. **Git Test Setup (Optional)**
   ```bash
   # Required for git adapter integration tests
   git config --global user.name "Your Name"
   git config --global user.email "you@example.com"
   ```

5. **Quality Checks**
   ```bash
   # Run the full quality check suite
   mix quality
   mix release.ready

   # Or individual checks
   mix format                    # Format code
   mix compile --warnings-as-errors  # Check compilation
   mix dialyzer                  # Type checking
   mix credo                     # Static analysis
   ```

## V1 Hardening Gates

Before merging release-sensitive changes, run this full matrix:

```bash
mix test
mix test --include integration
mix test --include git
mix test --include s3 --include integration
mix quality
```

Notes:
- `mix test` excludes `:integration` and `:s3` by default in `test/test_helper.exs`.
- S3 tests require Minio. If Minio is unavailable, S3 modules are tagged with `skip`.
- Integration runs should remain deterministic and network-independent except tagged adapter suites.

## Code Organization

### Project Structure
```
.
├── lib/
│   ├── jido_vfs.ex            # Main entry point
│   └── jido_vfs/
│       ├── adapter/           # Filesystem adapters
│       │   ├── local.ex       # Local filesystem
│       │   ├── in_memory.ex   # In-memory storage
│       │   ├── ets.ex         # ETS-backed storage
│       │   ├── s3.ex          # AWS S3 / Minio
│       │   ├── git.ex         # Git repositories
│       │   └── github.ex      # GitHub API
│       ├── errors.ex          # Typed error classes
│       ├── filesystem.ex      # Filesystem behaviour
│       ├── revision.ex        # Unified revision struct
│       └── stat/              # File stat structures
├── test/
│   ├── jido_vfs/
│   │   └── adapter/           # Adapter tests
│   ├── support/               # Test helpers
│   └── test_helper.exs
└── mix.exs
```

### Core Components
- **Adapters**: Backend implementations for different storage systems
- **Filesystem**: The behaviour that all adapters implement
- **Stat**: File metadata structures
- **Visibility**: File permission handling

### Adapter Onboarding
- Use the [Adapter Onboarding Checklist](docs/adapter-onboarding-checklist.md) before opening a PR for a new adapter or metadata callback changes.

## Development Guidelines

### Code Style

1. **Formatting**
   - Run `mix format` before committing
   - Follow standard Elixir style guide
   - Use `snake_case` for functions and variables
   - Use `PascalCase` for module names

2. **Documentation**
   - Add `@moduledoc` to every module
   - Document all public functions with `@doc`
   - Include examples when helpful
   - Use doctests for simple examples

3. **Type Specifications**
   ```elixir
   @type filesystem :: {module(), Jido.VFS.Adapter.config()}

   @spec read(filesystem, Path.t(), keyword()) ::
           {:ok, binary()} | {:error, Jido.VFS.Errors.error()}
   def read({adapter, config}, path, opts \\ []) do
     # Implementation
   end
   ```

### Testing

1. **Test Organization**
   ```elixir
   defmodule Jido.VFS.Adapter.LocalTest do
     use ExUnit.Case, async: true

     describe "read/2" do
       test "reads file contents" do
         # Test implementation
       end

       test "returns error for missing file" do
         # Error case testing
       end
     end
   end
   ```

2. **Coverage Requirements**
   - Keep application source coverage at 90% or more. The coverage check excludes
     test support code. It includes all application source.
   - Test both success and error paths
   - Test async behavior where applicable

3. **Running Tests**
   ```bash
   # Run full test suite
   mix test

   # Run the 90% coverage check, including local integration tests
   mix coveralls --include integration

   # Write a JSON report (this command does not enforce the coverage threshold)
   mix coveralls.json --include integration

   # Run specific test file
   mix test test/jido_vfs/adapter/local_test.exs

   # Include S3 tests (requires Minio)
   mix test --include s3

   # Include integration tests
   mix test --include integration
   ```

   S3 tests use Minio. The test helper uses `minio` from `PATH` when available.
   Otherwise, it uses the binary from `mix minio_server.download`. Each run uses
   a separate temporary data directory and a free local port. The helper removes
   the data directory after the test suite.

### Error Handling

1. **Use With Patterns**
   ```elixir
   def copy(filesystem, source, destination, opts) do
     with {:ok, normalized_source} <- Jido.VFS.RelativePath.normalize(source),
          {:ok, normalized_dest} <- Jido.VFS.RelativePath.normalize(destination) do
       adapter.copy(config, normalized_source, normalized_dest, opts)
     end
   end
   ```

2. **Return Values**
   - Public API return shapes must be `:ok`, `{:ok, value}`, or `{:error, %Jido.VFS.Errors.*{}}`
   - Do not leak raw string errors from adapters
   - Unsupported operations must return `%Jido.VFS.Errors.UnsupportedOperation{}`
   - Prefer explicit capability checks via `Jido.VFS.supports?/2`

## Git Workflow

### Commit Message Format

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `style` | Changes that don't affect code meaning |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `test` | Adding or correcting tests |
| `chore` | Changes to build process or auxiliary tools |
| `ci` | CI configuration changes |

### Examples

```bash
# Feature
git commit -m "feat(git): add revision history support"

# Bug fix
git commit -m "fix(s3): resolve timeout in streaming operations"

# Breaking change
git commit -m "feat(api)!: change filesystem return type"
```

## Pull Request Process

1. **Before Submitting**
   - Run the full quality check suite: `mix quality`
   - Ensure all tests pass
   - Update documentation if needed
   - Add tests for new functionality

2. **PR Guidelines**
   - Create a feature branch from `main`
   - Use descriptive commit messages following conventional commits
   - Reference any related issues
   - Keep changes focused and atomic

3. **Review Process**
   - PRs require at least one review
   - Address all review comments
   - Maintain a clean commit history
   - Update your branch if needed

## Release Process

Releases are handled automatically by maintainers. Contributors should:

1. **Use Conventional Commits** - Your commit messages determine changelog entries
2. **Update `CHANGELOG.md` (`[Unreleased]`)** for behavior changes, especially hardening or breakage
3. **Documentation** - Keep README and CONTRIBUTING aligned with runtime behavior and test matrix

## Additional Resources

- [Hex Documentation](https://hexdocs.pm/jido_vfs)
- [GitHub Issues](https://github.com/agentjido/jido_vfs/issues)
- [AgentJido Discord](https://agentjido.xyz/discord)

## Questions or Problems?

If you have questions about contributing:
- Open a GitHub Issue
- Join our Discord community
- Check existing issues and documentation

Thank you for contributing to Jido.VFS!
