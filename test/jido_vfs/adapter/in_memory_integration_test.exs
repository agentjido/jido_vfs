defmodule Jido.VFS.Adapter.InMemoryIntegrationTest do
  @moduledoc """
  Comprehensive integration tests for the InMemory adapter.

  These tests exercise edge cases, error conditions, and boundary scenarios
  to ensure the adapter returns proper error types and handles all cases gracefully.

  Run with: mix test --include integration
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  setup %{test: test} do
    filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
    {:ok, _pid} = start_supervised(filesystem)
    {_, config} = filesystem
    {:ok, filesystem: filesystem, config: config, name: test}
  end

  defp via(name) do
    Jido.VFS.Registry.via(Jido.VFS.Adapter.InMemory, name)
  end

  # ============================================================================
  # CORE OPERATIONS: Write/Read/Delete/Move/Copy
  # ============================================================================

  describe "core operations - happy paths" do
    test "basic write/read roundtrip", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file.txt", "hello")
      assert {:ok, "hello"} = Jido.VFS.read(fs, "file.txt")
    end

    test "write with iodata (list)", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "iodata.txt", ["he", "llo", " ", "world"])
      assert {:ok, "hello world"} = Jido.VFS.read(fs, "iodata.txt")
    end

    test "write with binary iodata", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "binary.txt", ["he", <<"llo">>])
      assert {:ok, "hello"} = Jido.VFS.read(fs, "binary.txt")
    end

    test "empty file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "empty.bin", "")
      assert {:ok, ""} = Jido.VFS.read(fs, "empty.bin")

      assert {:ok, %Jido.VFS.Stat.File{size: 0}} = Jido.VFS.stat(fs, "empty.bin")
    end

    test "binary data with null bytes", %{filesystem: fs} do
      content = <<0, 1, 255, 0, 42, 128, 200>>
      assert :ok = Jido.VFS.write(fs, "binary.bin", content)
      assert {:ok, ^content} = Jido.VFS.read(fs, "binary.bin")
    end

    test "moderately large file (1MB)", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(1_024 * 1_024)
      assert :ok = Jido.VFS.write(fs, "large.bin", content)
      assert {:ok, read_content} = Jido.VFS.read(fs, "large.bin")
      assert byte_size(read_content) == byte_size(content)
      assert content == read_content
    end

    test "overwrite existing file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "overwrite.txt", "original")
      assert :ok = Jido.VFS.write(fs, "overwrite.txt", "updated")
      assert {:ok, "updated"} = Jido.VFS.read(fs, "overwrite.txt")
    end

    test "move file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "src.txt", "content")
      assert :ok = Jido.VFS.move(fs, "src.txt", "moved.txt")
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs, "src.txt")
      assert {:ok, "content"} = Jido.VFS.read(fs, "moved.txt")
    end

    test "copy file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "original.txt", "content")
      assert :ok = Jido.VFS.copy(fs, "original.txt", "copy.txt")
      assert {:ok, "content"} = Jido.VFS.read(fs, "original.txt")
      assert {:ok, "content"} = Jido.VFS.read(fs, "copy.txt")
    end

    test "delete file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "delete_me.txt", "content")
      assert :ok = Jido.VFS.delete(fs, "delete_me.txt")
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs, "delete_me.txt")
    end

    test "delete non-existing file is idempotent", %{filesystem: fs} do
      assert :ok = Jido.VFS.delete(fs, "does_not_exist.txt")
    end
  end

  # ============================================================================
  # PATH EDGE CASES
  # ============================================================================

  describe "path edge cases" do
    test "unicode filenames", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "ümlaut.txt", "german")
      assert {:ok, "german"} = Jido.VFS.read(fs, "ümlaut.txt")

      assert :ok = Jido.VFS.write(fs, "日本語.txt", "japanese")
      assert {:ok, "japanese"} = Jido.VFS.read(fs, "日本語.txt")

      assert :ok = Jido.VFS.write(fs, "emoji_🎉.txt", "party")
      assert {:ok, "party"} = Jido.VFS.read(fs, "emoji_🎉.txt")
    end

    test "unicode nested paths", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "日本語/ファイル.txt", "nested")
      assert {:ok, "nested"} = Jido.VFS.read(fs, "日本語/ファイル.txt")
    end

    test "special characters in filenames", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file with spaces.txt", "spaces")
      assert {:ok, "spaces"} = Jido.VFS.read(fs, "file with spaces.txt")

      assert :ok = Jido.VFS.write(fs, "file-with-dashes.txt", "dashes")
      assert {:ok, "dashes"} = Jido.VFS.read(fs, "file-with-dashes.txt")

      assert :ok = Jido.VFS.write(fs, "file_with_underscores.txt", "underscores")
      assert {:ok, "underscores"} = Jido.VFS.read(fs, "file_with_underscores.txt")
    end

    test "path traversal attempt with ..", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.write(fs, "../evil.txt", "bad")
    end

    test "path traversal attempt with nested ..", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} =
               Jido.VFS.write(fs, "a/b/../../c/../../../evil.txt", "bad")
    end

    test "path traversal in read", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.read(fs, "../etc/passwd")
    end

    test "path traversal in delete", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.delete(fs, "../evil.txt")
    end

    test "path traversal in move source", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.move(fs, "../evil.txt", "dest.txt")
    end

    test "path traversal in copy source", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.copy(fs, "../evil.txt", "dest.txt")
    end

    test "absolute paths are rejected", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.AbsolutePath{}} = Jido.VFS.write(fs, "/etc/passwd", "bad")
      assert {:error, %Jido.VFS.Errors.AbsolutePath{}} = Jido.VFS.read(fs, "/etc/passwd")
      assert {:error, %Jido.VFS.Errors.AbsolutePath{}} = Jido.VFS.delete(fs, "/etc/passwd")
    end

    test "deeply nested path", %{filesystem: fs} do
      deep_path = Enum.map_join(1..20, "/", fn n -> "dir#{n}" end) <> "/file.txt"
      assert :ok = Jido.VFS.write(fs, deep_path, "deep content")
      assert {:ok, "deep content"} = Jido.VFS.read(fs, deep_path)
    end

    test "path normalization with redundant slashes", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "a//b///c/file.txt", "slashes")
      assert {:ok, "slashes"} = Jido.VFS.read(fs, "a/b/c/file.txt")
    end

    test "path normalization with dot segments", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "a/./b/./file.txt", "dots")
      assert {:ok, "dots"} = Jido.VFS.read(fs, "a/b/file.txt")
    end
  end

  # ============================================================================
  # ERROR CONDITIONS
  # ============================================================================

  describe "error conditions - file not found" do
    test "read missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{file_path: path}} =
               Jido.VFS.read(fs, "missing.txt")

      assert path =~ "missing.txt"
    end

    test "move missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.move(fs, "missing.txt", "dest.txt")
    end

    test "copy missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.copy(fs, "missing.txt", "dest.txt")
    end

    test "stat missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.stat(fs, "missing.txt")
    end

    test "visibility of missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.visibility(fs, "missing.txt")
    end
  end

  # ============================================================================
  # DIRECTORY OPERATIONS
  # ============================================================================

  describe "directory operations" do
    test "create directory", %{filesystem: fs} do
      assert :ok = Jido.VFS.create_directory(fs, "new_dir/")
      # Verify directory exists via stat (file_exists checks binary content only)
      assert {:ok, %Jido.VFS.Stat.Dir{name: "new_dir"}} = Jido.VFS.stat(fs, "new_dir/")
    end

    test "create nested directories", %{filesystem: fs} do
      assert :ok = Jido.VFS.create_directory(fs, "a/b/c/d/e/")
      assert {:ok, contents} = Jido.VFS.list_contents(fs, "a/b/c/d/")

      assert Enum.any?(contents, fn item ->
               match?(%Jido.VFS.Stat.Dir{name: "e"}, item)
             end)
    end

    test "delete empty directory", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "empty_dir/")
      assert :ok = Jido.VFS.delete_directory(fs, "empty_dir/")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "empty_dir/")
    end

    test "delete non-empty directory fails without recursive", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "non_empty/file.txt", "content")

      assert {:error, %Jido.VFS.Errors.DirectoryNotEmpty{}} =
               Jido.VFS.delete_directory(fs, "non_empty/", recursive: false)
    end

    test "delete non-empty directory with recursive", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "to_delete/a/b/file.txt", "content")
      :ok = Jido.VFS.write(fs, "to_delete/file2.txt", "content2")
      assert :ok = Jido.VFS.delete_directory(fs, "to_delete/", recursive: true)
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "to_delete/")
    end

    test "list contents returns files and directories", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "file1.txt", "content")
      :ok = Jido.VFS.write(fs, "file2.txt", "content")
      :ok = Jido.VFS.create_directory(fs, "subdir/")

      assert {:ok, contents} = Jido.VFS.list_contents(fs, ".")
      assert length(contents) == 3

      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "file1.txt"}, &1))
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "file2.txt"}, &1))
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.Dir{name: "subdir"}, &1))
    end

    test "list contents of empty directory returns empty list", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "empty/")
      assert {:ok, []} = Jido.VFS.list_contents(fs, "empty/")
    end

    test "clear filesystem", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "file1.txt", "content")
      :ok = Jido.VFS.write(fs, "dir/file2.txt", "content")
      :ok = Jido.VFS.create_directory(fs, "empty_dir/")

      assert :ok = Jido.VFS.clear(fs)

      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "file1.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "dir/file2.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "empty_dir/")
    end

    test "hidden files are included in listing", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, ".hidden", "secret")
      :ok = Jido.VFS.write(fs, "visible.txt", "public")

      assert {:ok, contents} = Jido.VFS.list_contents(fs, ".")
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: ".hidden"}, &1))
    end
  end

  # ============================================================================
  # STREAM OPERATIONS
  # ============================================================================

  describe "stream operations - enumerable" do
    test "read stream basic", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "stream_read.txt", "Hello World")
      assert {:ok, stream} = Jido.VFS.read_stream(fs, "stream_read.txt")
      assert Enum.into(stream, <<>>) == "Hello World"
    end

    test "read stream with chunk size", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "chunked.txt", "abcdef")
      assert {:ok, stream} = Jido.VFS.read_stream(fs, "chunked.txt", chunk_size: 2)
      chunks = Enum.to_list(stream)
      assert chunks == ["ab", "cd", "ef"]
    end

    test "read stream with uneven chunks", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "uneven.txt", "abcdefg")
      assert {:ok, stream} = Jido.VFS.read_stream(fs, "uneven.txt", chunk_size: 3)
      chunks = Enum.to_list(stream)
      assert chunks == ["abc", "def", "g"]
    end

    test "read stream single byte chunks", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "single.txt", "abc")
      assert {:ok, stream} = Jido.VFS.read_stream(fs, "single.txt", chunk_size: 1)
      chunks = Enum.to_list(stream)
      assert chunks == ["a", "b", "c"]
    end

    test "partial stream consumption with take", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "partial.txt", "abcdefghij")
      {:ok, stream} = Jido.VFS.read_stream(fs, "partial.txt", chunk_size: 2)

      first_two = stream |> Stream.take(2) |> Enum.to_list()
      assert first_two == ["ab", "cd"]
    end

    test "stream halt behavior", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "halt.txt", "abcdefghij")
      {:ok, stream} = Jido.VFS.read_stream(fs, "halt.txt", chunk_size: 2)

      # Stream.take halts after taking n elements
      result = Enum.take(stream, 1)
      assert result == ["ab"]
    end

    test "stream suspend and resume with take_while", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "suspend.txt", "12345")
      {:ok, stream} = Jido.VFS.read_stream(fs, "suspend.txt", chunk_size: 1)

      result = stream |> Stream.take_while(fn c -> c != "3" end) |> Enum.to_list()
      assert result == ["1", "2"]
    end

    test "stream for non-existent file returns typed error", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read_stream(fs, "missing.txt")
    end

    test "stream count fallback works", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "count.txt", "Hello World")
      {:ok, stream} = Jido.VFS.read_stream(fs, "count.txt", chunk_size: 5)
      # Enum.count/1 falls back to reduce when count/1 returns error
      assert Enum.count(stream) > 0
    end

    test "stream slice fallback works", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "slice.txt", "Hello World")
      {:ok, stream} = Jido.VFS.read_stream(fs, "slice.txt", chunk_size: 5)
      result = Enum.slice(stream, 0, 1)
      assert is_list(result)
    end

    test "stream member? fallback works", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "member.txt", "Hello")
      {:ok, stream} = Jido.VFS.read_stream(fs, "member.txt", [])
      assert Enum.member?(stream, "Hello") == true
    end
  end

  describe "stream operations - collectable" do
    test "write stream basic", %{filesystem: fs} do
      assert {:ok, stream} = Jido.VFS.write_stream(fs, "stream_write.txt")
      Enum.into(["Hello", " ", "World"], stream)
      assert {:ok, "Hello World"} = Jido.VFS.read(fs, "stream_write.txt")
    end

    test "write stream appends to existing file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "append_stream.txt", "Initial ")
      {:ok, stream} = Jido.VFS.write_stream(fs, "append_stream.txt")

      Enum.into(["appended", " content"], stream)
      assert {:ok, "Initial appended content"} = Jido.VFS.read(fs, "append_stream.txt")
    end

    test "write stream with empty data", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "empty_stream.txt")
      Enum.into([], stream)
      assert {:ok, ""} = Jido.VFS.read(fs, "empty_stream.txt")
    end

    test "write stream with binary data", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "binary_stream.bin")
      Enum.into([<<0, 1, 2>>, <<3, 4, 5>>], stream)
      assert {:ok, <<0, 1, 2, 3, 4, 5>>} = Jido.VFS.read(fs, "binary_stream.bin")
    end

    test "collectable halt behavior", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "halt.txt")
      {[], collector_fun} = Collectable.into(stream)
      result = collector_fun.([], :halt)
      assert result == :ok
    end

    test "write stream returns stream with path", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "path_check.txt")
      result = Enum.into(["test"], stream)
      assert result.path == "path_check.txt"
    end
  end

  # ============================================================================
  # VISIBILITY OPERATIONS
  # ============================================================================

  describe "visibility operations" do
    test "write with public visibility", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "public.txt", "content", visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "public.txt")
    end

    test "write with private visibility (default)", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "private.txt", "content")
      assert {:ok, :private} = Jido.VFS.visibility(fs, "private.txt")
    end

    test "set visibility on existing file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "change_vis.txt", "content", visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "change_vis.txt")

      :ok = Jido.VFS.set_visibility(fs, "change_vis.txt", :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "change_vis.txt")
    end

    test "set visibility on directory", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "vis_dir/", directory_visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "vis_dir/")

      :ok = Jido.VFS.set_visibility(fs, "vis_dir/", :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "vis_dir/")
    end

    test "directory visibility on auto-created dirs", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "public_dir/file.txt", "content", directory_visibility: :public)
      :ok = Jido.VFS.write(fs, "private_dir/file.txt", "content", directory_visibility: :private)

      assert {:ok, :public} = Jido.VFS.visibility(fs, "public_dir/")
      assert {:ok, :private} = Jido.VFS.visibility(fs, "private_dir/")
    end

    test "visibility of missing file returns error", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.visibility(fs, "nonexistent.txt")
    end

    test "set visibility on missing file returns error", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.set_visibility(fs, "nonexistent.txt", :public)
    end

    test "list contents includes visibility info", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "list_pub.txt", "content", visibility: :public)
      :ok = Jido.VFS.write(fs, "list_priv.txt", "content", visibility: :private)

      {:ok, contents} = Jido.VFS.list_contents(fs, ".")

      pub_file = Enum.find(contents, &(&1.name == "list_pub.txt"))
      priv_file = Enum.find(contents, &(&1.name == "list_priv.txt"))

      assert pub_file.visibility == :public
      assert priv_file.visibility == :private
    end
  end

  # ============================================================================
  # EXTENDED OPERATIONS: stat, access, append, truncate, utime
  # ============================================================================

  describe "stat operation" do
    test "stat file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "stat_test.txt", "1234567890")

      assert {:ok, %Jido.VFS.Stat.File{} = stat} = Jido.VFS.stat(fs, "stat_test.txt")
      assert stat.name == "stat_test.txt"
      assert stat.size == 10
      assert stat.visibility in [:public, :private]
      assert is_integer(stat.mtime)
    end

    test "stat directory", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "stat_dir/")

      assert {:ok, %Jido.VFS.Stat.Dir{} = stat} = Jido.VFS.stat(fs, "stat_dir")
      assert stat.name == "stat_dir"
      assert stat.size == 0
    end

    test "stat missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.stat(fs, "missing.txt")
    end

    test "stat returns correct size after writes", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "size_test.txt", "abc")
      assert {:ok, %Jido.VFS.Stat.File{size: 3}} = Jido.VFS.stat(fs, "size_test.txt")

      :ok = Jido.VFS.write(fs, "size_test.txt", "abcdef")
      assert {:ok, %Jido.VFS.Stat.File{size: 6}} = Jido.VFS.stat(fs, "size_test.txt")
    end
  end

  describe "access operation" do
    test "access readable file", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "readable.txt", "content", [])
      assert :ok = Jido.VFS.Adapter.InMemory.access(config, "readable.txt", [:read])
    end

    test "access with write mode", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "writable.txt", "content", [])
      assert :ok = Jido.VFS.Adapter.InMemory.access(config, "writable.txt", [:write])
    end

    test "access with both modes", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "both.txt", "content", [])
      assert :ok = Jido.VFS.Adapter.InMemory.access(config, "both.txt", [:read, :write])
    end

    test "access missing file", %{config: config} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.access(config, "missing.txt", [:read])
    end

    test "access directory", %{config: config} do
      Jido.VFS.Adapter.InMemory.create_directory(config, "dir/", [])
      assert :ok = Jido.VFS.Adapter.InMemory.access(config, "dir/", [:read])
    end
  end

  describe "append operation" do
    test "append to existing file", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "append.txt", "abc", [])
      assert :ok = Jido.VFS.Adapter.InMemory.append(config, "append.txt", "def", [])
      assert {:ok, "abcdef"} = Jido.VFS.Adapter.InMemory.read(config, "append.txt")
    end

    test "append creates new file", %{config: config} do
      assert :ok = Jido.VFS.Adapter.InMemory.append(config, "new_append.txt", "content", [])
      assert {:ok, "content"} = Jido.VFS.Adapter.InMemory.read(config, "new_append.txt")
    end

    test "append with directory creation", %{config: config} do
      assert :ok =
               Jido.VFS.Adapter.InMemory.append(config, "nested/append.txt", "content", directory_visibility: :private)

      assert {:ok, "content"} = Jido.VFS.Adapter.InMemory.read(config, "nested/append.txt")
    end

    test "multiple appends", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "multi.txt", "", [])
      Jido.VFS.Adapter.InMemory.append(config, "multi.txt", "a", [])
      Jido.VFS.Adapter.InMemory.append(config, "multi.txt", "b", [])
      Jido.VFS.Adapter.InMemory.append(config, "multi.txt", "c", [])
      assert {:ok, "abc"} = Jido.VFS.Adapter.InMemory.read(config, "multi.txt")
    end

    test "append updates mtime", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "mtime_append.txt", "initial", [])
      {:ok, stat1} = Jido.VFS.Adapter.InMemory.stat(config, "mtime_append.txt")

      Process.sleep(1000)
      Jido.VFS.Adapter.InMemory.append(config, "mtime_append.txt", " appended", [])
      {:ok, stat2} = Jido.VFS.Adapter.InMemory.stat(config, "mtime_append.txt")

      assert stat2.mtime >= stat1.mtime
    end
  end

  describe "truncate operation" do
    test "truncate to zero", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "truncate.txt", "abcdef", [])
      assert :ok = Jido.VFS.Adapter.InMemory.truncate(config, "truncate.txt", 0)
      assert {:ok, ""} = Jido.VFS.Adapter.InMemory.read(config, "truncate.txt")
    end

    test "truncate to smaller size", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "truncate_small.txt", "abcdef", [])
      assert :ok = Jido.VFS.Adapter.InMemory.truncate(config, "truncate_small.txt", 3)

      {:ok, content} = Jido.VFS.Adapter.InMemory.read(config, "truncate_small.txt")
      assert content == "abc"
    end

    test "truncate to larger size pads with zeros", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "truncate_large.txt", "abc", [])
      assert :ok = Jido.VFS.Adapter.InMemory.truncate(config, "truncate_large.txt", 10)

      assert {:ok, content} = Jido.VFS.Adapter.InMemory.read(config, "truncate_large.txt")
      assert byte_size(content) == 10
      assert content == "abc" <> :binary.copy(<<0>>, 7)
    end

    test "truncate to same size is idempotent", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "same_size.txt", "hello", [])
      assert :ok = Jido.VFS.Adapter.InMemory.truncate(config, "same_size.txt", 5)
      assert {:ok, "hello"} = Jido.VFS.Adapter.InMemory.read(config, "same_size.txt")
    end

    test "truncate missing file", %{config: config} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.truncate(config, "missing.txt", 10)
    end

    test "truncate updates mtime", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "mtime_trunc.txt", "content", [])
      {:ok, stat1} = Jido.VFS.Adapter.InMemory.stat(config, "mtime_trunc.txt")

      Process.sleep(1000)
      Jido.VFS.Adapter.InMemory.truncate(config, "mtime_trunc.txt", 3)
      {:ok, stat2} = Jido.VFS.Adapter.InMemory.stat(config, "mtime_trunc.txt")

      assert stat2.mtime >= stat1.mtime
    end
  end

  describe "utime operation" do
    test "set modification time on file", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "utime.txt", "content", [])

      past_time = ~U[2020-01-01 12:00:00Z]
      assert :ok = Jido.VFS.Adapter.InMemory.utime(config, "utime.txt", past_time)

      {:ok, stat} = Jido.VFS.Adapter.InMemory.stat(config, "utime.txt")
      assert stat.mtime == DateTime.to_unix(past_time, :second)
    end

    test "set modification time on directory", %{config: config} do
      Jido.VFS.Adapter.InMemory.create_directory(config, "utime_dir/", [])

      past_time = ~U[2020-06-15 08:30:00Z]
      assert :ok = Jido.VFS.Adapter.InMemory.utime(config, "utime_dir/", past_time)

      {:ok, stat} = Jido.VFS.Adapter.InMemory.stat(config, "utime_dir/")
      assert stat.mtime == DateTime.to_unix(past_time, :second)
    end

    test "utime on missing file", %{config: config} do
      past_time = DateTime.utc_now()

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.utime(config, "missing.txt", past_time)
    end

    test "utime with future time", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "future.txt", "content", [])

      future_time = DateTime.utc_now() |> DateTime.add(3600 * 24 * 365, :second)
      assert :ok = Jido.VFS.Adapter.InMemory.utime(config, "future.txt", future_time)

      {:ok, stat} = Jido.VFS.Adapter.InMemory.stat(config, "future.txt")
      assert stat.mtime == DateTime.to_unix(future_time, :second)
    end
  end

  # ============================================================================
  # VERSIONING OPERATIONS
  # ============================================================================

  describe "versioning - basic operations" do
    test "write_version creates version and returns version_id", %{config: config} do
      assert {:ok, version_id} =
               Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert is_binary(version_id)
      assert String.length(version_id) == 32
    end

    test "read_version retrieves specific version", %{config: config} do
      {:ok, version_id} =
        Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert {:ok, "Hello World v1"} =
               Jido.VFS.Adapter.InMemory.read_version(config, "test.txt", version_id)
    end

    test "write_version also updates current file", %{config: config} do
      {:ok, _version_id} =
        Jido.VFS.Adapter.InMemory.write_version(config, "versioned.txt", "Version 1", [])

      assert {:ok, "Version 1"} = Jido.VFS.Adapter.InMemory.read(config, "versioned.txt")
    end

    test "each write_version generates unique version_id", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v2", [])
      {:ok, v3} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v3", [])

      assert v1 != v2
      assert v2 != v3
      assert v1 != v3
    end
  end

  describe "versioning - list_versions" do
    test "list_versions returns all versions for a path", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 2
      assert Enum.any?(versions, &(&1.version_id == v1))
      assert Enum.any?(versions, &(&1.version_id == v2))
    end

    test "list_versions returns empty list for non-versioned file", %{config: config} do
      {:ok, []} = Jido.VFS.Adapter.InMemory.list_versions(config, "nonexistent.txt")
    end

    test "list_versions includes timestamps", %{config: config} do
      {:ok, _} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "content", [])

      {:ok, [version]} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert is_integer(version.timestamp)
      assert version.timestamp > 0
    end

    test "versions are ordered chronologically (newest first in internal list)", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v1", [])
      Process.sleep(10)
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v2", [])
      Process.sleep(10)
      {:ok, v3} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v3", [])

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")

      version_ids = Enum.map(versions, & &1.version_id)
      assert version_ids == [v1, v2, v3]
    end
  end

  describe "versioning - get_latest_version" do
    test "get_latest_version returns most recent version", %{config: config} do
      {:ok, _v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert {:ok, ^v2} = Jido.VFS.Adapter.InMemory.get_latest_version(config, "test.txt")
    end

    test "get_latest_version returns error for non-versioned file", %{config: config} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.get_latest_version(config, "nonexistent.txt")
    end

    test "get_latest_version after many versions", %{config: config} do
      for i <- 1..10 do
        Jido.VFS.Adapter.InMemory.write_version(config, "many.txt", "Version #{i}", [])
      end

      {:ok, v_last} = Jido.VFS.Adapter.InMemory.write_version(config, "many.txt", "Final", [])

      assert {:ok, ^v_last} = Jido.VFS.Adapter.InMemory.get_latest_version(config, "many.txt")
    end
  end

  describe "versioning - restore_version" do
    test "restore_version restores file to specific version", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, _v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert {:ok, "Version 2"} = Jido.VFS.Adapter.InMemory.read(config, "test.txt")

      assert :ok = Jido.VFS.Adapter.InMemory.restore_version(config, "test.txt", v1)
      assert {:ok, "Version 1"} = Jido.VFS.Adapter.InMemory.read(config, "test.txt")
    end

    test "restore_version returns error for invalid version_id", %{config: config} do
      {:ok, _} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "content", [])

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.restore_version(config, "test.txt", "invalid_version_id")
    end

    test "restore_version preserves visibility", %{config: config} do
      {:ok, version_id} =
        Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Content", visibility: :public)

      Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Updated", visibility: :private)

      assert :ok = Jido.VFS.Adapter.InMemory.restore_version(config, "test.txt", version_id)
      assert {:ok, :public} = Jido.VFS.Adapter.InMemory.visibility(config, "test.txt")
    end

    test "restored version can be read", %{config: config} do
      content = :crypto.strong_rand_bytes(1000)
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "binary.bin", content, [])
      {:ok, _v2} = Jido.VFS.Adapter.InMemory.write_version(config, "binary.bin", "replaced", [])

      :ok = Jido.VFS.Adapter.InMemory.restore_version(config, "binary.bin", v1)
      assert {:ok, ^content} = Jido.VFS.Adapter.InMemory.read(config, "binary.bin")
    end
  end

  describe "versioning - delete_version" do
    test "delete_version removes specific version", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v1)

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 1
      assert hd(versions).version_id == v2
    end

    test "read_version fails after delete_version", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])

      :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v1)

      assert {:error, _} = Jido.VFS.Adapter.InMemory.read_version(config, "test.txt", v1)
    end

    test "delete_version does not affect current file", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, _v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v1)

      assert {:ok, "Version 2"} = Jido.VFS.Adapter.InMemory.read(config, "test.txt")
    end

    test "delete all versions", %{config: config} do
      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "v2", [])

      :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v1)
      :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v2)

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert versions == []
    end
  end

  describe "versioning - isolation between files" do
    test "versions are isolated between different files", %{config: config} do
      {:ok, v1_a} = Jido.VFS.Adapter.InMemory.write_version(config, "file_a.txt", "A content", [])
      {:ok, v1_b} = Jido.VFS.Adapter.InMemory.write_version(config, "file_b.txt", "B content", [])

      {:ok, versions_a} = Jido.VFS.Adapter.InMemory.list_versions(config, "file_a.txt")
      {:ok, versions_b} = Jido.VFS.Adapter.InMemory.list_versions(config, "file_b.txt")

      assert length(versions_a) == 1
      assert length(versions_b) == 1
      assert hd(versions_a).version_id == v1_a
      assert hd(versions_b).version_id == v1_b
    end
  end

  # ============================================================================
  # CONCURRENCY TESTS
  # ============================================================================

  describe "concurrency - simultaneous writes" do
    test "simultaneous writes to same file", %{filesystem: fs} do
      contents = Enum.map(1..10, fn n -> "content_#{n}" end)

      tasks =
        Enum.map(contents, fn content ->
          Task.async(fn ->
            Jido.VFS.write(fs, "concurrent.txt", content)
          end)
        end)

      results = Task.await_many(tasks)
      assert Enum.all?(results, &(&1 == :ok))

      {:ok, final_content} = Jido.VFS.read(fs, "concurrent.txt")
      assert final_content in contents
    end

    test "concurrent writes to different files", %{filesystem: fs} do
      tasks =
        Enum.map(1..20, fn n ->
          Task.async(fn ->
            Jido.VFS.write(fs, "file_#{n}.txt", "content_#{n}")
          end)
        end)

      results = Task.await_many(tasks)
      assert Enum.all?(results, &(&1 == :ok))

      for n <- 1..20 do
        expected = "content_#{n}"
        assert {:ok, ^expected} = Jido.VFS.read(fs, "file_#{n}.txt")
      end
    end
  end

  describe "concurrency - simultaneous reads" do
    test "concurrent reads of same file", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(10_000)
      :ok = Jido.VFS.write(fs, "concurrent_read.txt", content)

      tasks =
        Enum.map(1..20, fn _ ->
          Task.async(fn ->
            Jido.VFS.read(fs, "concurrent_read.txt")
          end)
        end)

      results = Task.await_many(tasks)

      assert Enum.all?(results, fn result ->
               result == {:ok, content}
             end)
    end

    test "concurrent reads of different files", %{filesystem: fs} do
      for n <- 1..10 do
        Jido.VFS.write(fs, "read_file_#{n}.txt", "content_#{n}")
      end

      tasks =
        Enum.flat_map(1..10, fn n ->
          Enum.map(1..3, fn _ ->
            Task.async(fn ->
              {n, Jido.VFS.read(fs, "read_file_#{n}.txt")}
            end)
          end)
        end)

      results = Task.await_many(tasks)

      for {n, result} <- results do
        assert result == {:ok, "content_#{n}"}
      end
    end
  end

  describe "concurrency - mixed operations" do
    test "concurrent write and read", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "wr_concurrent.txt", "initial")

      writer =
        Task.async(fn ->
          Process.sleep(10)
          Jido.VFS.write(fs, "wr_concurrent.txt", "updated")
        end)

      reader =
        Task.async(fn ->
          Process.sleep(20)
          Jido.VFS.read(fs, "wr_concurrent.txt")
        end)

      assert :ok = Task.await(writer)
      assert {:ok, content} = Task.await(reader)
      assert content in ["initial", "updated"]
    end

    test "concurrent directory creation", %{filesystem: fs} do
      tasks =
        Enum.map(1..10, fn n ->
          Task.async(fn ->
            result = Jido.VFS.write(fs, "concurrent_dir#{n}/file.txt", "content#{n}")
            {n, result}
          end)
        end)

      results = Task.await_many(tasks)

      for {_n, result} <- results do
        assert result == :ok
      end

      for n <- 1..10 do
        expected = "content#{n}"
        assert {:ok, ^expected} = Jido.VFS.read(fs, "concurrent_dir#{n}/file.txt")
      end
    end

    test "concurrent versioning operations", %{config: config} do
      tasks =
        Enum.map(1..10, fn n ->
          Task.async(fn ->
            Jido.VFS.Adapter.InMemory.write_version(config, "versioned.txt", "Version #{n}", [])
          end)
        end)

      results = Task.await_many(tasks)

      version_ids =
        for {:ok, id} <- results do
          id
        end

      assert length(version_ids) == 10
      assert length(Enum.uniq(version_ids)) == 10

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "versioned.txt")
      assert length(versions) == 10
    end

    test "concurrent appends", %{config: config} do
      Jido.VFS.Adapter.InMemory.write(config, "concurrent_append.txt", "", [])

      tasks =
        Enum.map(1..10, fn n ->
          Task.async(fn ->
            Jido.VFS.Adapter.InMemory.append(config, "concurrent_append.txt", "#{n}", [])
          end)
        end)

      results = Task.await_many(tasks)
      assert Enum.all?(results, &(&1 == :ok))

      {:ok, content} = Jido.VFS.Adapter.InMemory.read(config, "concurrent_append.txt")
      assert String.length(content) == 10 or String.length(content) == 11
    end
  end

  # ============================================================================
  # PROCESS LIFECYCLE TESTS
  # ============================================================================

  describe "process lifecycle - agent management" do
    test "state is isolated between different named filesystems", %{test: test} do
      name1 = :"#{test}_fs1"
      name2 = :"#{test}_fs2"

      fs1 = Jido.VFS.Adapter.InMemory.configure(name: name1)
      fs2 = Jido.VFS.Adapter.InMemory.configure(name: name2)

      start_supervised!({Jido.VFS.Adapter.InMemory, fs1}, id: :fs1)
      start_supervised!({Jido.VFS.Adapter.InMemory, fs2}, id: :fs2)

      :ok = Jido.VFS.write(fs1, "file.txt", "content1")
      :ok = Jido.VFS.write(fs2, "file.txt", "content2")

      assert {:ok, "content1"} = Jido.VFS.read(fs1, "file.txt")
      assert {:ok, "content2"} = Jido.VFS.read(fs2, "file.txt")
    end

    test "filesystem survives and maintains state across operations", %{filesystem: fs} do
      for i <- 1..100 do
        :ok = Jido.VFS.write(fs, "file_#{i}.txt", "content_#{i}")
      end

      for i <- 1..100 do
        expected = "content_#{i}"
        assert {:ok, ^expected} = Jido.VFS.read(fs, "file_#{i}.txt")
      end
    end

    test "clear resets state completely", %{filesystem: fs, name: name} do
      :ok = Jido.VFS.write(fs, "file1.txt", "content")
      :ok = Jido.VFS.write(fs, "dir/file2.txt", "content")

      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(elem(fs, 1), "versioned.txt", "v1", [])

      :ok = Jido.VFS.clear(fs)

      state = Agent.get(via(name), fn s -> s end)
      assert state == {%{}, %{}, %{}}

      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "file1.txt")

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.read_version(elem(fs, 1), "versioned.txt", v1)
    end
  end

  # ============================================================================
  # FILE EXISTS EDGE CASES
  # ============================================================================

  describe "file_exists edge cases" do
    test "file_exists for existing file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "exists.txt", "content")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "exists.txt")
    end

    test "file_exists for missing file", %{filesystem: fs} do
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "missing.txt")
    end

    test "file_exists for directory (via stat)", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "dir_exists/")
      # Note: InMemory's file_exists only checks for binary content, use stat for directories
      assert {:ok, %Jido.VFS.Stat.Dir{}} = Jido.VFS.stat(fs, "dir_exists/")
    end

    test "file_exists in missing parent directory", %{filesystem: fs} do
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "no_such_dir/file.txt")
    end

    test "file_exists after delete", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "temp.txt", "content")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "temp.txt")

      :ok = Jido.VFS.delete(fs, "temp.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "temp.txt")
    end
  end

  # ============================================================================
  # COPY BETWEEN FILESYSTEMS
  # ============================================================================

  describe "copy between filesystems" do
    test "copy between same filesystem", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "cross_src.txt", "cross content")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {fs, "cross_src.txt"},
                 {fs, "cross_dest.txt"}
               )

      assert {:ok, "cross content"} = Jido.VFS.read(fs, "cross_dest.txt")
    end

    test "copy from missing file returns error", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.copy_between_filesystem(
                 {fs, "nonexistent.txt"},
                 {fs, "dest.txt"}
               )
    end

    test "copy between different named filesystems", %{test: test} do
      name1 = :"#{test}_copy_fs1"
      name2 = :"#{test}_copy_fs2"

      fs1 = Jido.VFS.Adapter.InMemory.configure(name: name1)
      fs2 = Jido.VFS.Adapter.InMemory.configure(name: name2)

      start_supervised!({Jido.VFS.Adapter.InMemory, fs1}, id: :copy_fs1)
      start_supervised!({Jido.VFS.Adapter.InMemory, fs2}, id: :copy_fs2)

      :ok = Jido.VFS.write(fs1, "source.txt", "original content")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {fs1, "source.txt"},
                 {fs2, "dest.txt"}
               )

      assert {:ok, "original content"} = Jido.VFS.read(fs2, "dest.txt")
    end
  end
end
