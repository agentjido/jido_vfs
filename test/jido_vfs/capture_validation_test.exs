defmodule Jido.VFS.CaptureValidationTest do
  use ExUnit.Case, async: true
  alias Jido.VFS
  alias Jido.VFS.Adapter.InMemory
  alias Jido.VFS.Capture

  test "invalid edits leave both the content and the revision unchanged", %{test: name} do
    filesystem = InMemory.configure(name: name)
    start_supervised!(filesystem)
    :ok = VFS.write(filesystem, "dir/file", "original")
    {:ok, capture, revision} = VFS.capture(filesystem)

    for operation <- [
          {:mkdir, "dir/file"},
          {:write, "dir", "bytes"},
          {:delete, "missing"},
          {:delete, "dir"},
          {:write, "dir/file/child", "bytes"},
          {:write, "new", "bytes", [mode: :invalid]},
          {:write, "../escape", "bytes"},
          {:unknown, "file"}
        ] do
      assert {:error, _} =
               VFS.apply_edits(filesystem, revision, [{:write, "new", "bytes"}, operation])

      assert {:ok, ^capture, ^revision} = VFS.capture(filesystem)
    end
  end

  test "captures refuse malformed entries and missing parent directories" do
    for entries <- [
          %{"file" => %{type: :unknown}},
          %{"dir/file" => %{type: :file, content: "bytes", metadata: %{}}},
          %{123 => %{type: :directory, metadata: %{}}}
        ] do
      assert_raise ArgumentError, fn -> Capture.to_tree(%Capture{entries: entries}) end
    end
  end
end
