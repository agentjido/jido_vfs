defmodule Jido.VFS.AdapterBoundaryTest do
  use ExUnit.Case, async: true
  alias Jido.VFS
  alias Jido.VFS.Errors

  defmodule Adapter do
    def configure(opts) do
      case opts[:result] do
        :raise -> raise "configuration failed"
        result -> result
      end
    end

    def versioning_module, do: __MODULE__
    def revisions(config, _, _), do: {:ok, config}
    def read_revision(_, _, _, _), do: {:ok, "bytes"}
  end

  defmodule InvalidVersioning do
    def versioning_module, do: :not_a_versioning_module
    def unsupported_operations, do: :invalid
  end

  defmodule FailedVersioning do
    def versioning_module, do: raise("unavailable")
    def unsupported_operations, do: raise("unavailable")
  end

  test "configuration failures become typed errors and the raising API preserves them" do
    for result <- [:raise, :invalid, {:error, :offline}, {:error, :unsupported}] do
      assert {:error, error} = VFS.safe_configure(Adapter, result: result)
      assert is_exception(error)
      assert_raise error.__struct__, fn -> VFS.configure!(Adapter, result: result) end
    end

    assert {:ok, {Adapter, %{}}} = VFS.safe_configure(Adapter, result: {Adapter, %{}})
    assert {:error, %Errors.AdapterError{}} = VFS.safe_configure("invalid", [])
    assert {:error, %Errors.UnsupportedOperation{}} = VFS.safe_configure(MissingAdapter)
  end

  test "capability queries cover each public operation without calling the adapter" do
    fs = VFS.Adapter.InMemory.configure(name: :capability_probe)

    for operation <- [
          :capture,
          :apply_edits,
          :replace,
          :write,
          :write_stream,
          :read,
          :read_stream,
          :delete,
          :move,
          :copy,
          :copy_between,
          :file_exists,
          :list_contents,
          :create_directory,
          :delete_directory,
          :clear,
          :set_visibility,
          :visibility,
          :stat,
          :access,
          :append,
          :truncate,
          :utime,
          :commit,
          :revisions,
          :read_revision,
          :rollback
        ] do
      assert is_boolean(VFS.supports?(fs, operation))
    end

    refute VFS.supports?({"invalid", %{}}, :read)

    for adapter <- [InvalidVersioning, FailedVersioning] do
      refute VFS.supports?({adapter, %{}}, :commit)
      assert {:error, %Errors.UnsupportedOperation{}} = VFS.commit({adapter, %{}})
      assert {:error, %Errors.UnsupportedOperation{}} = VFS.rollback({adapter, %{}}, "revision")
    end

    assert {:error, %Errors.UnsupportedOperation{}} = VFS.apply_edits({Adapter, %{}}, :token, [])
    assert {:error, %Errors.UnsupportedOperation{}} = VFS.replace({Adapter, %{}}, :token, %VFS.Capture{entries: %{}})
  end

  test "revision metadata has a common shape and preserves valid dates" do
    existing = %VFS.Revision{
      sha: "existing",
      author_name: "name",
      author_email: "email",
      message: "message",
      timestamp: ~U[2024-01-01 00:00:00Z]
    }

    revisions = [
      existing,
      %{revision: 123, timestamp: 0},
      %{sha: "sha", timestamp: ~U[2025-01-01 00:00:00Z]},
      %{sha: "no-date"},
      :legacy
    ]

    assert {:ok, [^existing, numeric, dated, missing_date, legacy]} = VFS.revisions({Adapter, revisions})
    assert numeric.sha == "123"
    assert numeric.timestamp == ~U[1970-01-01 00:00:00Z]
    assert dated.timestamp == ~U[2025-01-01 00:00:00Z]
    assert %DateTime{} = missing_date.timestamp
    assert legacy.sha == ":legacy"
    assert {:error, %Errors.PathTraversal{}} = VFS.revisions({Adapter, []}, "../escape")
    assert {:error, %Errors.PathTraversal{}} = VFS.read_revision({Adapter, []}, "../escape", "revision")
  end
end
