# Jido Agent Integration

`jido_vfs` is the storage abstraction layer in the Jido ecosystem. It is intentionally kept lower-level than `jido_action` and `jido` so it can be used by libraries, services, shells, and agents without pulling in the full agent stack.

## Recommendation

Do not make `jido_action` a required dependency of `jido_vfs`.

Reasons:

- `jido_vfs` is useful outside agent runtimes.
- Pulling `jido` and `jido_action` into the core package would widen the dependency surface significantly.
- Filesystem adapters and tool/action wrappers evolve at different rates.
- Agent-safe defaults are policy decisions, not storage-abstraction concerns.

## Better pattern

Keep `jido_vfs` as the adapter/core package and expose agent tools in one of these forms:

- Thin action modules in the consuming app.
- A separate `jido_vfs_actions` package.
- A tool pack in a separately maintained ecosystem integration package.

## Suggested default action set

Safe-by-default read actions:

- `ReadFile`
- `ReadStream`
- `ListContents`
- `Stat`
- `FileExists`
- `GetVisibility`
- `ListRevisions`
- `ReadRevision`

Opt-in mutation actions:

- `WriteFile`
- `WriteStream`
- `DeleteFile`
- `MoveFile`
- `CopyFile`
- `CopyBetweenFilesystems`
- `CreateDirectory`
- `DeleteDirectory`
- `SetVisibility`
- `Commit`
- `Rollback`
- `Clear`

## Recommended policy boundary

For autonomous agents, split actions into at least two tool groups:

- Read-only filesystem inspection tools.
- Mutating filesystem tools that are explicitly enabled by the host application.

This mirrors how other Jido packages separate safe browsing/status operations from state-changing operations.

## Action context shape

The cleanest integration point is to pass a configured filesystem through action context:

```elixir
%{
  filesystem: Jido.VFS.Adapter.Local.configure(prefix: "/workspace")
}
```

Alternative patterns that also work:

- Resolve a named filesystem module that uses `Jido.VFS.Filesystem`.
- Look up the adapter/config from application config for multi-tenant agents.

## Example action wrapper

```elixir
defmodule MyApp.Actions.ReadFile do
  use Jido.Action,
    name: "read_file",
    description: "Read a file from the configured virtual filesystem",
    schema: [
      path: [type: :string, required: true, doc: "Relative path to read"]
    ]

  @impl true
  def run(params, %{filesystem: filesystem}) do
    case Jido.VFS.read(filesystem, params.path) do
      {:ok, contents} -> {:ok, %{path: params.path, contents: contents}}
      {:error, error} -> {:error, error}
    end
  end
end
```

## Conclusion

`jido_vfs` should support Jido agents well, but through composition rather than a hard dependency on `jido_action`.

If the ecosystem wants a default tool pack, it should likely live next to this package, not inside its runtime core.
