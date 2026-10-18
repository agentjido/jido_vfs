defmodule Jido.VFS.Adapter.LocalFailuresTest do
  use ExUnit.Case, async: true
  alias Jido.VFS
  alias Jido.VFS.Adapter.Local
  alias Jido.VFS.Errors

  @tag :tmp_dir
  test "blocked parent paths refuse writes, streams, and append operations", %{tmp_dir: root} do
    fs = Local.configure(prefix: root)
    assert :ok = VFS.write(fs, "file", "original")
    assert {:error, %Errors.DirectoryNotEmpty{}} = VFS.write(fs, "file/child", "bytes")
    assert {:error, %Errors.DirectoryNotEmpty{}} = VFS.write_stream(fs, "file/child")
    assert {:error, %Errors.DirectoryNotEmpty{}} = VFS.append(fs, "file/child", "bytes")
    assert {:ok, "original"} = VFS.read(fs, "file")
    assert {:error, %Errors.FileNotFound{}} = VFS.access(fs, "missing", [:read])
  end

  @tag :tmp_dir
  test "native copies and moves report missing sources and preserve the destination", %{tmp_dir: root} do
    fs = Local.configure(prefix: root)
    other = Local.configure(prefix: Path.join(root, "other"))
    assert :ok = VFS.write(fs, "destination", "original")
    assert {:error, %Errors.FileNotFound{}} = VFS.copy(fs, "missing", "destination")
    assert {:error, %Errors.FileNotFound{}} = VFS.move(fs, "missing", "destination")

    assert {:error, %Errors.FileNotFound{}} =
             VFS.copy_between_filesystem({fs, "missing"}, {other, "destination"}, copy_between_strategy: :native)

    assert {:ok, "original"} = VFS.read(fs, "destination")
    assert {:ok, :missing} = VFS.file_exists(other, "destination")
  end

  @tag :tmp_dir
  test "stream and delete operations refuse directories", %{tmp_dir: root} do
    fs = Local.configure(prefix: root)
    assert :ok = VFS.create_directory(fs, "dir/")
    assert {:error, %Errors.AdapterError{}} = VFS.read_stream(fs, "dir")
    assert {:error, %Errors.PermissionDenied{}} = VFS.delete(fs, "dir")
    assert {:error, %Errors.FileNotFound{}} = VFS.truncate(fs, "missing", 1)
    assert {:error, _} = VFS.truncate(fs, "dir", 1)
    assert {:ok, %VFS.Stat.Dir{}} = VFS.stat(fs, "dir")
  end
end
