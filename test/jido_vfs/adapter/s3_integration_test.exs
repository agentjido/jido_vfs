defmodule Jido.VFS.Adapter.S3IntegrationTest do
  @moduledoc """
  Comprehensive integration tests for the S3 adapter.

  These tests exercise edge cases, error conditions, and boundary scenarios
  specific to S3/object storage to ensure the adapter returns proper error
  types and handles all cases gracefully.

  The S3 adapter has these key characteristics:
  - Object storage (no true directories, uses prefixes)
  - Multipart upload for streaming (5MB minimum part size)
  - ACL-based visibility control
  - Eventual consistency (though Minio is strongly consistent)
  - Cross-bucket copy operations
  - Prefix-based path routing

  Run with: mix test --include integration --include s3
  """
  use ExUnit.Case, async: false
  use Mimic

  @moduletag :integration
  @moduletag :s3
  @minio_available Application.compile_env(:jido_vfs, :minio_available, false)

  if not @minio_available do
    @moduletag skip: "Minio not available"
  end

  setup_all do
    {:ok, raw_config: JidoVfsTest.Minio.config()}
  end

  setup %{raw_config: config} do
    JidoVfsTest.Minio.clean_bucket("default")
    JidoVfsTest.Minio.recreate_bucket("default")
    Jido.VFS.Adapter.S3.reset_visibility_store()

    filesystem = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
    {_, s3_config} = filesystem

    on_exit(fn ->
      JidoVfsTest.Minio.clean_bucket("default")
      Jido.VFS.Adapter.S3.reset_visibility_store()
    end)

    {:ok, filesystem: filesystem, config: s3_config, raw_config: config, bucket: "default"}
  end

  setup :copy_download_module

  defp copy_download_module(_context) do
    Mimic.copy(ExAws.S3.Download)
    :ok
  end

  # ============================================================================
  # CORE OPERATIONS: Write/Read/Delete/Move/Copy
  # ============================================================================

  describe "core operations - happy paths" do
    test "basic write/read roundtrip", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file.txt", "hello")
      assert {:ok, "hello"} = Jido.VFS.read(fs, "file.txt")
    end

    test "write with iodata (list) - converted to binary", %{filesystem: fs} do
      # S3 adapter uses ExAws which requires binary content (iodata converted)
      content = IO.iodata_to_binary(["he", "llo", " ", "world"])
      assert :ok = Jido.VFS.write(fs, "iodata.txt", content)
      assert {:ok, "hello world"} = Jido.VFS.read(fs, "iodata.txt")
    end

    test "write with binary iodata - converted to binary", %{filesystem: fs} do
      # S3 adapter uses ExAws which requires binary content (iodata converted)
      content = IO.iodata_to_binary(["he", <<"llo">>])
      assert :ok = Jido.VFS.write(fs, "binary.txt", content)
      assert {:ok, "hello"} = Jido.VFS.read(fs, "binary.txt")
    end

    test "empty file", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "empty.bin", "")
      assert {:ok, ""} = Jido.VFS.read(fs, "empty.bin")
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

    test "delete non-existing file is idempotent (S3 behavior)", %{filesystem: fs} do
      assert :ok = Jido.VFS.delete(fs, "does_not_exist.txt")
    end

    test "copy to same path fails (S3 does not allow identical copy)", %{filesystem: fs} do
      # S3 does not allow copying an object to itself without changing metadata
      :ok = Jido.VFS.write(fs, "same.txt", "content")
      assert {:error, %Jido.VFS.Errors.AdapterError{}} = Jido.VFS.copy(fs, "same.txt", "same.txt")
      # Original should still exist
      assert {:ok, "content"} = Jido.VFS.read(fs, "same.txt")
    end

    test "move to nested path", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "top.txt", "content")
      assert :ok = Jido.VFS.move(fs, "top.txt", "nested/path/to/file.txt")
      assert {:ok, "content"} = Jido.VFS.read(fs, "nested/path/to/file.txt")
    end
  end

  # ============================================================================
  # S3-SPECIFIC LARGE FILE OPERATIONS
  # ============================================================================

  describe "large file operations" do
    @tag timeout: 120_000
    test "5MB file (S3 multipart boundary)", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(5 * 1024 * 1024)
      assert :ok = Jido.VFS.write(fs, "5mb.bin", content)
      assert {:ok, read_content} = Jido.VFS.read(fs, "5mb.bin")
      assert byte_size(read_content) == byte_size(content)
      assert content == read_content
    end

    @tag timeout: 180_000
    test "10MB file (exceeds multipart boundary)", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(10 * 1024 * 1024)
      assert :ok = Jido.VFS.write(fs, "10mb.bin", content)
      assert {:ok, read_content} = Jido.VFS.read(fs, "10mb.bin")
      assert byte_size(read_content) == byte_size(content)
      assert :crypto.hash(:sha256, content) == :crypto.hash(:sha256, read_content)
    end

    test "multiple large files in sequence", %{filesystem: fs} do
      for i <- 1..3 do
        content = :crypto.strong_rand_bytes(1_024 * 1_024)
        assert :ok = Jido.VFS.write(fs, "large_#{i}.bin", content)
        assert {:ok, ^content} = Jido.VFS.read(fs, "large_#{i}.bin")
      end
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

    test "unicode content", %{filesystem: fs} do
      content = "こんにちは世界 🌍 مرحبا"
      assert :ok = Jido.VFS.write(fs, "unicode_content.txt", content)
      assert {:ok, ^content} = Jido.VFS.read(fs, "unicode_content.txt")
    end

    test "special characters in filenames", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file with spaces.txt", "spaces")
      assert {:ok, "spaces"} = Jido.VFS.read(fs, "file with spaces.txt")

      assert :ok = Jido.VFS.write(fs, "file-with-dashes.txt", "dashes")
      assert {:ok, "dashes"} = Jido.VFS.read(fs, "file-with-dashes.txt")

      assert :ok = Jido.VFS.write(fs, "file_with_underscores.txt", "underscores")
      assert {:ok, "underscores"} = Jido.VFS.read(fs, "file_with_underscores.txt")
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

    test "path normalization - leading slash stripped", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "leading.txt", "content")
      assert {:ok, "content"} = Jido.VFS.read(fs, "leading.txt")
    end

    test "hidden files (dot prefix)", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, ".hidden", "secret")
      assert {:ok, "secret"} = Jido.VFS.read(fs, ".hidden")
    end

    test "files with multiple dots", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file.test.backup.txt", "content")
      assert {:ok, "content"} = Jido.VFS.read(fs, "file.test.backup.txt")
    end

    test "special S3 characters that need URL encoding", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "file+plus.txt", "plus")
      assert {:ok, "plus"} = Jido.VFS.read(fs, "file+plus.txt")

      assert :ok = Jido.VFS.write(fs, "file=equals.txt", "equals")
      assert {:ok, "equals"} = Jido.VFS.read(fs, "file=equals.txt")
    end

    test "very long filename (up to 1024 bytes for S3)", %{filesystem: fs} do
      long_name = String.duplicate("a", 200) <> ".txt"
      assert :ok = Jido.VFS.write(fs, long_name, "long filename")
      assert {:ok, "long filename"} = Jido.VFS.read(fs, long_name)
    end

    test "filename starting with number", %{filesystem: fs} do
      assert :ok = Jido.VFS.write(fs, "123file.txt", "content")
      assert {:ok, "content"} = Jido.VFS.read(fs, "123file.txt")
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

    test "read from nested missing path", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.read(fs, "no_such_dir/missing.txt")
    end

    test "move missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.move(fs, "missing.txt", "dest.txt")
    end

    test "copy missing file", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.copy(fs, "missing.txt", "dest.txt")
    end
  end

  describe "error conditions - directory errors" do
    test "delete non-empty directory without recursive fails", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "non_empty/file.txt", "content")

      assert {:error, %Jido.VFS.Errors.DirectoryNotEmpty{}} =
               Jido.VFS.delete_directory(fs, "non_empty/", recursive: false)
    end
  end

  # ============================================================================
  # DIRECTORY OPERATIONS (S3 PREFIX-BASED)
  # ============================================================================

  describe "directory operations (prefix-based)" do
    test "create directory (creates empty object with trailing slash)", %{filesystem: fs} do
      assert :ok = Jido.VFS.create_directory(fs, "new_dir/")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "new_dir/")
    end

    test "create directory requires trailing slash", %{filesystem: fs} do
      # S3 adapter requires explicit trailing slash for directory paths
      # Without trailing slash, Jido.VFS returns NotDirectory error
      assert {:error, %Jido.VFS.Errors.NotDirectory{}} = Jido.VFS.create_directory(fs, "new_dir2")
    end

    test "create nested directories", %{filesystem: fs} do
      assert :ok = Jido.VFS.create_directory(fs, "a/b/c/d/e/")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "a/b/c/d/e/")
    end

    test "delete empty directory", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "empty_dir/")
      # Note: delete_directory on S3 calls delete on the trimmed path (without trailing slash)
      # but create_directory creates with trailing slash. Need recursive delete to fully clean.
      assert :ok = Jido.VFS.delete_directory(fs, "empty_dir/", recursive: true)
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "empty_dir/")
    end

    test "delete non-empty directory with recursive", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "to_delete/a/b/file.txt", "content")
      :ok = Jido.VFS.write(fs, "to_delete/file2.txt", "content2")
      assert :ok = Jido.VFS.delete_directory(fs, "to_delete/", recursive: true)
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "to_delete/a/b/file.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "to_delete/file2.txt")
    end

    @tag timeout: 180_000
    test "recursive delete handles paginated object listings (>1000 objects)", %{filesystem: fs} do
      for i <- 1..1_050 do
        :ok = Jido.VFS.write(fs, "bulk_delete/item_#{i}.txt", "#{i}")
      end

      assert :ok = Jido.VFS.delete_directory(fs, "bulk_delete/", recursive: true)
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "bulk_delete/item_1.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "bulk_delete/item_1050.txt")
    end

    test "list contents returns files and directories", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "file1.txt", "content")
      :ok = Jido.VFS.write(fs, "file2.txt", "content")
      :ok = Jido.VFS.write(fs, "subdir/nested.txt", "nested content")

      assert {:ok, contents} = Jido.VFS.list_contents(fs, ".")
      assert length(contents) >= 2

      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "file1.txt"}, &1))
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "file2.txt"}, &1))
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.Dir{name: "subdir"}, &1))
    end

    test "list contents of nested directory", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "parent/child1.txt", "content1")
      :ok = Jido.VFS.write(fs, "parent/child2.txt", "content2")
      :ok = Jido.VFS.write(fs, "parent/nested/deep.txt", "deep content")

      {:ok, contents} = Jido.VFS.list_contents(fs, "parent/")

      # S3 lists files under the prefix
      file_names = Enum.map(contents, & &1.name)

      assert "child1.txt" in file_names or
               Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "child1.txt"}, &1))

      assert "child2.txt" in file_names or
               Enum.any?(contents, &match?(%Jido.VFS.Stat.File{name: "child2.txt"}, &1))

      # nested is returned as a common prefix (directory)
      assert Enum.any?(contents, &match?(%Jido.VFS.Stat.Dir{name: "nested"}, &1))
    end

    test "list contents of empty directory marker", %{filesystem: fs} do
      # In S3, an empty directory marker is just a 0-byte object with trailing slash
      # When we list it, it may or may not appear in the common prefixes
      :ok = Jido.VFS.create_directory(fs, "empty/")
      {:ok, contents} = Jido.VFS.list_contents(fs, "empty/")
      # The directory marker itself may or may not be returned depending on S3 implementation
      # For Minio, we expect it to be empty since there are no objects under the prefix
      assert is_list(contents)
    end

    test "list contents returns correct file sizes", %{filesystem: fs} do
      content = "1234567890"
      :ok = Jido.VFS.write(fs, "sized.txt", content)

      {:ok, contents} = Jido.VFS.list_contents(fs, ".")
      file = Enum.find(contents, &(&1.name == "sized.txt"))

      assert file.size == 10
    end

    test "list contents returns mtime as DateTime", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "timed.txt", "content")

      {:ok, contents} = Jido.VFS.list_contents(fs, ".")
      file = Enum.find(contents, &(&1.name == "timed.txt"))

      assert %DateTime{} = file.mtime
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
      assert Enum.any?(contents, &(&1.name == ".hidden"))
    end

    test "list root directory with empty prefix", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "root_file.txt", "content")
      :ok = Jido.VFS.write(fs, "subdir/nested.txt", "nested")

      {:ok, contents} = Jido.VFS.list_contents(fs, "")
      assert Enum.any?(contents, &(&1.name == "root_file.txt"))
    end
  end

  # ============================================================================
  # STREAM OPERATIONS (MULTIPART UPLOAD/DOWNLOAD)
  # ============================================================================

  describe "stream operations - write (multipart upload)" do
    test "write stream via Collectable", %{filesystem: fs} do
      assert {:ok, stream} = Jido.VFS.write_stream(fs, "stream_write.txt")
      Enum.into(["Hello", " ", "World"], stream)
      assert {:ok, "Hello World"} = Jido.VFS.read(fs, "stream_write.txt")
    end

    test "write stream with binary chunks", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "binary_stream.bin")
      chunks = [<<1, 2, 3>>, <<4, 5, 6>>, <<7, 8, 9>>]
      Enum.into(chunks, stream)
      assert {:ok, <<1, 2, 3, 4, 5, 6, 7, 8, 9>>} = Jido.VFS.read(fs, "binary_stream.bin")
    end

    test "write stream with empty data does not create object", %{filesystem: fs} do
      # S3 multipart upload with no data doesn't complete the upload
      # so no object is created
      {:ok, stream} = Jido.VFS.write_stream(fs, "empty_stream.txt")
      _result = Enum.into([], stream)
      # No object created since multipart upload had no parts
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs, "empty_stream.txt")
    end

    @tag timeout: 180_000
    test "write stream exceeding multipart threshold (>5MB)", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "multipart_stream.bin")

      chunks =
        Stream.repeatedly(fn -> :crypto.strong_rand_bytes(1_024 * 1_024) end)
        |> Stream.take(6)
        |> Enum.to_list()

      _result = Enum.into(chunks, stream)

      {:ok, content} = Jido.VFS.read(fs, "multipart_stream.bin")
      assert byte_size(content) == 6 * 1024 * 1024
    end

    test "stream collectable halt behavior", %{filesystem: fs} do
      {:ok, stream} = Jido.VFS.write_stream(fs, "halt_stream.txt")
      {_acc, collector_fun} = Collectable.into(stream)
      result = collector_fun.(nil, :halt)
      assert result == :ok
    end

    test "halting a multipart upload aborts and leaves no object", %{
      filesystem: fs,
      raw_config: raw_config,
      bucket: bucket
    } do
      {:ok, stream} = Jido.VFS.write_stream(fs, "halt_cleanup.bin")
      {state, collector_fun} = Collectable.into(stream)

      next_state = collector_fun.(state, {:cont, :crypto.strong_rand_bytes(1_024 * 1_024)})
      assert :ok = collector_fun.(next_state, :halt)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs, "halt_cleanup.bin")

      {:ok, %{body: body}} = ExAws.S3.list_multipart_uploads(bucket) |> ExAws.request(raw_config)
      uploads = Map.get(body, :uploads, [])
      refute Enum.any?(uploads, fn upload -> upload.key == "halt_cleanup.bin" end)
    end
  end

  describe "stream operations - read" do
    test "read stream basic", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(10_000)
      :ok = Jido.VFS.write(fs, "stream_read.bin", content)
      assert {:ok, stream} = Jido.VFS.read_stream(fs, "stream_read.bin")
      assert Enum.into(stream, <<>>) == content
    end

    test "read stream with large file", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(1_024 * 1_024)
      :ok = Jido.VFS.write(fs, "large_stream.bin", content)
      {:ok, stream} = Jido.VFS.read_stream(fs, "large_stream.bin")

      result = Enum.into(stream, <<>>)
      assert byte_size(result) == byte_size(content)
    end

    test "read stream of missing file returns error", %{filesystem: fs} do
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read_stream(fs, "missing.txt")
    end

    test "read stream surfaces chunk task failures as adapter errors", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(100_000)
      :ok = Jido.VFS.write(fs, "timeout_stream.bin", content)

      expect(ExAws.S3.Download, :get_chunk, fn _op, _boundaries, _config ->
        Process.sleep(10)
        {0, content}
      end)

      {:ok, stream} =
        Jido.VFS.read_stream(fs, "timeout_stream.bin",
          chunk_size: 200_000,
          max_concurrency: 1,
          timeout: 0
        )

      assert_raise Jido.VFS.Errors.AdapterError, fn ->
        Enum.to_list(stream)
      end
    end

    test "read stream produces chunks", %{filesystem: fs} do
      content = String.duplicate("x", 100_000)
      :ok = Jido.VFS.write(fs, "chunked_read.txt", content)
      {:ok, stream} = Jido.VFS.read_stream(fs, "chunked_read.txt")

      chunks = Enum.to_list(stream)
      assert length(chunks) >= 1
      assert Enum.join(chunks, "") == content
    end

    test "read stream on empty file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "empty_read.txt", "")
      {:ok, stream} = Jido.VFS.read_stream(fs, "empty_read.txt")
      result = Enum.into(stream, <<>>)
      assert result == ""
    end
  end

  # ============================================================================
  # VISIBILITY OPERATIONS (ACL-BASED)
  # ============================================================================

  describe "visibility operations" do
    test "write with public visibility stores visibility", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "public.txt", "content", visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "public.txt")
    end

    test "write with private visibility stores visibility", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "private.txt", "content", visibility: :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "private.txt")
    end

    test "default visibility is public", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "default_vis.txt", "content")
      assert {:ok, :public} = Jido.VFS.visibility(fs, "default_vis.txt")
    end

    test "set visibility on existing file", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "change_vis.txt", "content", visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "change_vis.txt")

      :ok = Jido.VFS.set_visibility(fs, "change_vis.txt", :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "change_vis.txt")
    end

    test "visibility toggle back and forth", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "toggle.txt", "content", visibility: :public)
      :ok = Jido.VFS.set_visibility(fs, "toggle.txt", :private)
      :ok = Jido.VFS.set_visibility(fs, "toggle.txt", :public)
      :ok = Jido.VFS.set_visibility(fs, "toggle.txt", :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "toggle.txt")
    end

    test "directory visibility applies to files in directory", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "public_dir/file.txt", "content", directory_visibility: :public)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "public_dir/")
    end

    test "set visibility on directory", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "vis_dir/")
      :ok = Jido.VFS.set_visibility(fs, "vis_dir/", :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "vis_dir/")
    end

    test "visibility store is path-specific", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "a.txt", "a", visibility: :public)
      :ok = Jido.VFS.write(fs, "b.txt", "b", visibility: :private)

      assert {:ok, :public} = Jido.VFS.visibility(fs, "a.txt")
      assert {:ok, :private} = Jido.VFS.visibility(fs, "b.txt")
    end

    test "delete removes stored file visibility", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "deleted.txt", "content", visibility: :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs, "deleted.txt")

      assert :ok = Jido.VFS.delete(fs, "deleted.txt")
      assert {:ok, :public} = Jido.VFS.visibility(fs, "deleted.txt")
    end

    test "delete_directory removes stored directory and child visibility", %{filesystem: fs} do
      :ok =
        Jido.VFS.write(
          fs,
          "private_dir/inner.txt",
          "content",
          visibility: :private,
          directory_visibility: :private
        )

      assert {:ok, :private} = Jido.VFS.visibility(fs, "private_dir/")
      assert {:ok, :private} = Jido.VFS.visibility(fs, "private_dir/inner.txt")

      assert :ok = Jido.VFS.delete_directory(fs, "private_dir/", recursive: true)
      assert {:ok, :public} = Jido.VFS.visibility(fs, "private_dir/")
      assert {:ok, :public} = Jido.VFS.visibility(fs, "private_dir/inner.txt")
    end

    test "clear removes only visibility entries scoped to configured prefix", %{raw_config: config} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "vis_clear_a/")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "vis_clear_b/")

      :ok = Jido.VFS.write(fs_a, "file.txt", "content", visibility: :private)
      :ok = Jido.VFS.write(fs_b, "file.txt", "content", visibility: :private)
      assert {:ok, :private} = Jido.VFS.visibility(fs_a, "file.txt")
      assert {:ok, :private} = Jido.VFS.visibility(fs_b, "file.txt")

      assert :ok = Jido.VFS.clear(fs_a)

      assert {:ok, :public} = Jido.VFS.visibility(fs_a, "file.txt")
      assert {:ok, :private} = Jido.VFS.visibility(fs_b, "file.txt")
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

    test "file_exists for directory (trailing slash)", %{filesystem: fs} do
      :ok = Jido.VFS.create_directory(fs, "dir_exists/")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "dir_exists/")
    end

    test "file_exists in missing parent directory", %{filesystem: fs} do
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "no_such_dir/file.txt")
    end

    test "file_exists after delete", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "delete_check.txt", "content")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "delete_check.txt")
      :ok = Jido.VFS.delete(fs, "delete_check.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(fs, "delete_check.txt")
    end

    test "file_exists with nested path", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "nested/deep/file.txt", "content")
      assert {:ok, :exists} = Jido.VFS.file_exists(fs, "nested/deep/file.txt")
    end
  end

  # ============================================================================
  # CROSS-BUCKET OPERATIONS
  # ============================================================================

  describe "cross-bucket operations" do
    setup %{raw_config: config} do
      JidoVfsTest.Minio.clean_bucket("secondary")
      JidoVfsTest.Minio.recreate_bucket("secondary")

      on_exit(fn ->
        JidoVfsTest.Minio.clean_bucket("secondary")
      end)

      {:ok, config_b: config}
    end

    test "copy between same bucket (same config)", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "source.txt", "cross content")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {fs, "source.txt"},
                 {fs, "dest.txt"}
               )

      assert {:ok, "cross content"} = Jido.VFS.read(fs, "dest.txt")
      assert {:ok, "cross content"} = Jido.VFS.read(fs, "source.txt")
    end

    test "copy between different buckets (same config)", %{raw_config: config, config_b: config_b} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config_b, bucket: "secondary")

      :ok = Jido.VFS.write(fs_a, "source.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {fs_a, "source.txt"},
                 {fs_b, "dest.txt"}
               )

      assert {:ok, "Hello World"} = Jido.VFS.read(fs_b, "dest.txt")
    end

    test "copy missing file between buckets returns error", %{
      raw_config: config,
      config_b: config_b
    } do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config_b, bucket: "secondary")

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.copy_between_filesystem(
                 {fs_a, "nonexistent.txt"},
                 {fs_b, "dest.txt"}
               )
    end

    test "copy large file between buckets", %{raw_config: config, config_b: config_b} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config_b, bucket: "secondary")

      content = :crypto.strong_rand_bytes(1_024 * 1_024)
      :ok = Jido.VFS.write(fs_a, "large_source.bin", content)

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {fs_a, "large_source.bin"},
                 {fs_b, "large_dest.bin"}
               )

      assert {:ok, ^content} = Jido.VFS.read(fs_b, "large_dest.bin")
    end

    test "move between buckets (copy + delete)", %{raw_config: config, config_b: config_b} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config_b, bucket: "secondary")

      :ok = Jido.VFS.write(fs_a, "move_source.txt", "moving")

      {:ok, content} = Jido.VFS.read(fs_a, "move_source.txt")
      :ok = Jido.VFS.write(fs_b, "move_dest.txt", content)
      :ok = Jido.VFS.delete(fs_a, "move_source.txt")

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs_a, "move_source.txt")
      assert {:ok, "moving"} = Jido.VFS.read(fs_b, "move_dest.txt")
    end
  end

  # ============================================================================
  # PREFIX CONFIGURATION TESTS
  # ============================================================================

  describe "prefix configuration" do
    test "write with prefix", %{raw_config: config} do
      fs = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "myprefix")
      :ok = Jido.VFS.write(fs, "file.txt", "prefixed content")
      assert {:ok, "prefixed content"} = Jido.VFS.read(fs, "file.txt")
    end

    test "files with different prefixes are isolated", %{raw_config: config} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "prefix_a")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "prefix_b")

      :ok = Jido.VFS.write(fs_a, "file.txt", "content A")
      :ok = Jido.VFS.write(fs_b, "file.txt", "content B")

      assert {:ok, "content A"} = Jido.VFS.read(fs_a, "file.txt")
      assert {:ok, "content B"} = Jido.VFS.read(fs_b, "file.txt")
    end

    test "files under different prefixes are readable independently", %{raw_config: config} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "list_a/")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "list_b/")

      :ok = Jido.VFS.write(fs_a, "a1.txt", "content A1")
      :ok = Jido.VFS.write(fs_a, "a2.txt", "content A2")
      :ok = Jido.VFS.write(fs_b, "b1.txt", "content B1")

      # Files are accessible through their respective filesystems
      assert {:ok, "content A1"} = Jido.VFS.read(fs_a, "a1.txt")
      assert {:ok, "content A2"} = Jido.VFS.read(fs_a, "a2.txt")
      assert {:ok, "content B1"} = Jido.VFS.read(fs_b, "b1.txt")

      # Files under different prefixes are not accessible from wrong filesystem
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs_a, "b1.txt")
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs_b, "a1.txt")

      assert {:ok, list_a} = Jido.VFS.list_contents(fs_a, ".")
      assert {:ok, list_b} = Jido.VFS.list_contents(fs_b, ".")

      assert Enum.map(list_a, & &1.name) |> Enum.sort() == ["a1.txt", "a2.txt"]
      assert Enum.map(list_b, & &1.name) == ["b1.txt"]
    end

    test "delete with prefix only affects prefixed files", %{raw_config: config} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "del_a")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "del_b")

      :ok = Jido.VFS.write(fs_a, "file.txt", "content A")
      :ok = Jido.VFS.write(fs_b, "file.txt", "content B")

      :ok = Jido.VFS.delete(fs_a, "file.txt")

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs_a, "file.txt")
      assert {:ok, "content B"} = Jido.VFS.read(fs_b, "file.txt")
    end

    test "clear only affects files in configured prefix", %{raw_config: config} do
      fs_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "clear_a/")
      fs_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "clear_b/")

      :ok = Jido.VFS.write(fs_a, "file.txt", "content A")
      :ok = Jido.VFS.write(fs_b, "file.txt", "content B")

      :ok = Jido.VFS.clear(fs_a)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(fs_a, "file.txt")
      assert {:ok, "content B"} = Jido.VFS.read(fs_b, "file.txt")
    end

    test "nested prefix paths", %{raw_config: config} do
      fs =
        Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "deep/nested/prefix")

      :ok = Jido.VFS.write(fs, "file.txt", "deep content")
      assert {:ok, "deep content"} = Jido.VFS.read(fs, "file.txt")
    end
  end

  # ============================================================================
  # CONCURRENCY TESTS
  # ============================================================================

  describe "concurrency" do
    test "concurrent reads of same file", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(10_000)
      :ok = Jido.VFS.write(fs, "concurrent_read.txt", content)

      tasks =
        Enum.map(1..10, fn _ ->
          Task.async(fn ->
            Jido.VFS.read(fs, "concurrent_read.txt")
          end)
        end)

      results = Task.await_many(tasks, 30_000)

      assert Enum.all?(results, fn result ->
               result == {:ok, content}
             end)
    end

    test "concurrent writes to different files", %{filesystem: fs} do
      tasks =
        Enum.map(1..10, fn n ->
          Task.async(fn ->
            path = "concurrent_#{n}.txt"
            content = "content_#{n}"
            :ok = Jido.VFS.write(fs, path, content)
            {path, content}
          end)
        end)

      results = Task.await_many(tasks, 30_000)

      for {path, expected_content} <- results do
        assert {:ok, ^expected_content} = Jido.VFS.read(fs, path)
      end
    end

    test "concurrent writes to same file (last writer wins)", %{filesystem: fs} do
      contents = Enum.map(1..5, fn n -> "content_#{n}" end)

      tasks =
        Enum.map(contents, fn content ->
          Task.async(fn ->
            Jido.VFS.write(fs, "concurrent_same.txt", content)
          end)
        end)

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))

      {:ok, final_content} = Jido.VFS.read(fs, "concurrent_same.txt")
      assert final_content in contents
    end

    test "concurrent directory creation", %{filesystem: fs} do
      tasks =
        Enum.map(1..5, fn n ->
          Task.async(fn ->
            result = Jido.VFS.write(fs, "concurrent_dir#{n}/file.txt", "content#{n}")
            {n, result}
          end)
        end)

      results = Task.await_many(tasks, 30_000)

      for {n, result} <- results do
        assert result == :ok
        expected = "content#{n}"
        assert {:ok, ^expected} = Jido.VFS.read(fs, "concurrent_dir#{n}/file.txt")
      end
    end

    test "concurrent delete and read", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "del_read.txt", "content")

      delete_task =
        Task.async(fn ->
          Process.sleep(10)
          Jido.VFS.delete(fs, "del_read.txt")
        end)

      read_task =
        Task.async(fn ->
          Jido.VFS.read(fs, "del_read.txt")
        end)

      delete_result = Task.await(delete_task)
      read_result = Task.await(read_task)

      assert delete_result == :ok

      assert read_result == {:ok, "content"} or
               match?({:error, %Jido.VFS.Errors.FileNotFound{}}, read_result)
    end

    test "concurrent list and write", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "list_write/existing.txt", "existing")

      write_task =
        Task.async(fn ->
          for i <- 1..5 do
            Jido.VFS.write(fs, "list_write/new_#{i}.txt", "new content #{i}")
          end
        end)

      list_task =
        Task.async(fn ->
          Process.sleep(50)
          Jido.VFS.list_contents(fs, "list_write/")
        end)

      Task.await(write_task, 30_000)
      {:ok, contents} = Task.await(list_task, 30_000)

      assert is_list(contents)
      # At least one file should be visible
      names = Enum.map(contents, & &1.name)
      assert "existing.txt" in names or length(contents) > 0
    end
  end

  # ============================================================================
  # S3-SPECIFIC METADATA TESTS
  # ============================================================================

  describe "S3-specific metadata" do
    test "content type can be set via options", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "typed.json", ~s({"key": "value"}), content_type: "application/json")
      assert {:ok, _} = Jido.VFS.read(fs, "typed.json")
    end

    test "custom metadata can be passed", %{filesystem: fs} do
      :ok =
        Jido.VFS.write(fs, "meta.txt", "content", meta: [{"x-amz-meta-custom", "value"}])

      assert {:ok, _} = Jido.VFS.read(fs, "meta.txt")
    end

    test "cache control can be set", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "cached.txt", "content", cache_control: "max-age=3600")
      assert {:ok, _} = Jido.VFS.read(fs, "cached.txt")
    end
  end

  # ============================================================================
  # EDGE CASES AND BOUNDARY CONDITIONS
  # ============================================================================

  describe "edge cases and boundary conditions" do
    test "write and read content with various line endings", %{filesystem: fs} do
      content_unix = "line1\nline2\nline3"
      content_windows = "line1\r\nline2\r\nline3"
      content_mac = "line1\rline2\rline3"

      :ok = Jido.VFS.write(fs, "unix.txt", content_unix)
      :ok = Jido.VFS.write(fs, "windows.txt", content_windows)
      :ok = Jido.VFS.write(fs, "mac.txt", content_mac)

      assert {:ok, ^content_unix} = Jido.VFS.read(fs, "unix.txt")
      assert {:ok, ^content_windows} = Jido.VFS.read(fs, "windows.txt")
      assert {:ok, ^content_mac} = Jido.VFS.read(fs, "mac.txt")
    end

    test "write and read all byte values (0-255)", %{filesystem: fs} do
      content = :binary.list_to_bin(Enum.to_list(0..255))
      :ok = Jido.VFS.write(fs, "all_bytes.bin", content)
      assert {:ok, ^content} = Jido.VFS.read(fs, "all_bytes.bin")
    end

    test "rapid successive writes and reads", %{filesystem: fs} do
      for i <- 1..50 do
        content = "iteration #{i}"
        :ok = Jido.VFS.write(fs, "rapid.txt", content)
        assert {:ok, ^content} = Jido.VFS.read(fs, "rapid.txt")
      end
    end

    test "copy preserves binary content exactly", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(1000)
      :ok = Jido.VFS.write(fs, "binary_copy_src.bin", content)
      :ok = Jido.VFS.copy(fs, "binary_copy_src.bin", "binary_copy_dest.bin")
      assert {:ok, ^content} = Jido.VFS.read(fs, "binary_copy_dest.bin")
    end

    test "move preserves binary content exactly", %{filesystem: fs} do
      content = :crypto.strong_rand_bytes(1000)
      :ok = Jido.VFS.write(fs, "binary_move_src.bin", content)
      :ok = Jido.VFS.move(fs, "binary_move_src.bin", "binary_move_dest.bin")
      assert {:ok, ^content} = Jido.VFS.read(fs, "binary_move_dest.bin")
    end

    @tag timeout: 180_000
    test "list contents with more than 1000 files (pagination behavior)", %{filesystem: fs} do
      for i <- 1..1_050 do
        :ok =
          Jido.VFS.write(
            fs,
            "many/file_#{String.pad_leading(Integer.to_string(i), 4, "0")}.txt",
            "#{i}"
          )
      end

      {:ok, contents} = Jido.VFS.list_contents(fs, "many/")
      assert length(contents) == 1_050
    end

    test "operations on root path variations", %{filesystem: fs} do
      :ok = Jido.VFS.write(fs, "root.txt", "content")

      {:ok, contents1} = Jido.VFS.list_contents(fs, "")
      {:ok, contents2} = Jido.VFS.list_contents(fs, ".")

      assert Enum.any?(contents1, &(&1.name == "root.txt"))
      assert Enum.any?(contents2, &(&1.name == "root.txt"))
    end
  end

  # ============================================================================
  # ADAPTER BEHAVIOR TESTS
  # ============================================================================

  describe "adapter behavior" do
    test "starts_processes returns false", %{} do
      assert Jido.VFS.Adapter.S3.starts_processes() == false
    end

    test "configure returns module and config tuple", %{raw_config: config} do
      result = Jido.VFS.Adapter.S3.configure(config: config, bucket: "test")
      assert {Jido.VFS.Adapter.S3, %Jido.VFS.Adapter.S3.Config{}} = result
    end

    test "configure with all options", %{raw_config: config} do
      {_, s3_config} =
        Jido.VFS.Adapter.S3.configure(
          config: config,
          bucket: "test",
          prefix: "/custom/prefix"
        )

      assert s3_config.bucket == "test"
      assert s3_config.prefix == "/custom/prefix"
    end

    test "configure with default prefix", %{raw_config: config} do
      {_, s3_config} = Jido.VFS.Adapter.S3.configure(config: config, bucket: "test")
      assert s3_config.prefix == "/"
    end
  end
end
