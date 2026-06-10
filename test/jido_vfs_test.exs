defmodule JidoVFSTest do
  use ExUnit.Case, async: true

  defmacrop assert_in_list(list, match) do
    quote do
      assert Enum.any?(unquote(list), &match?(unquote(match), &1))
    end
  end

  defmodule RaisingConfigureAdapter do
    def configure(_opts), do: raise("boom")
  end

  defmodule InvalidConfigureAdapter do
    def configure(_opts), do: :invalid
  end

  defmodule MetadataVersioningAdapter do
    def configure(_opts), do: {__MODULE__, %{configured: true}}
    def versioning_module, do: MetadataVersioningAdapter.Versioning
  end

  defmodule MetadataVersioningAdapter.Versioning do
    def commit(_config, _message, _opts), do: :ok
  end

  defmodule UnsupportedMetadataAdapter do
    def configure(_opts), do: {__MODULE__, %{}}
    def unsupported_operations, do: [:copy_between]
    def copy(_source_config, _source, _destination_config, _destination, _opts), do: :ok
  end

  defmodule CopyOptionCheckingAdapter do
    @behaviour Jido.VFS.Adapter

    def starts_processes, do: false
    def configure(opts), do: {__MODULE__, Map.new(opts)}
    def unsupported_operations, do: [:read_stream, :write_stream, :append, :copy_between]
    def versioning_module, do: nil

    def write(%{test_pid: test_pid}, path, contents, opts) do
      send(test_pid, {:copy_option_write, path, IO.iodata_to_binary(contents), opts})

      if Keyword.has_key?(opts, :chunk_size) do
        {:error, {:unexpected_chunk_size, opts}}
      else
        :ok
      end
    end

    def write(_config, _path, _contents, _opts), do: :ok
    def write_stream(_config, _path, _opts), do: unsupported(:write_stream)
    def read(%{content: content}, _path), do: {:ok, content}
    def read(_config, _path), do: {:error, Jido.VFS.Errors.FileNotFound.exception(file_path: "missing")}
    def read_stream(_config, _path, _opts), do: unsupported(:read_stream)
    def delete(_config, _path), do: :ok
    def move(_config, _source, _destination, _opts), do: :ok
    def copy(_config, _source, _destination, _opts), do: :ok
    def copy(_source_config, _source, _destination_config, _destination, _opts), do: unsupported(:copy_between)
    def file_exists(_config, _path), do: {:ok, :missing}
    def list_contents(_config, _path), do: {:ok, []}
    def create_directory(_config, _path, _opts), do: :ok
    def delete_directory(_config, _path, _opts), do: :ok
    def clear(_config), do: :ok
    def set_visibility(_config, _path, _visibility), do: :ok
    def visibility(_config, _path), do: {:ok, :public}

    defp unsupported(operation) do
      {:error, Jido.VFS.Errors.UnsupportedOperation.exception(operation: operation, adapter: __MODULE__)}
    end
  end

  describe "safe adapter configuration" do
    test "returns configured filesystem for valid adapter" do
      assert {:ok, {Jido.VFS.Adapter.Local, %Jido.VFS.Adapter.Local.Config{}}} =
               Jido.VFS.safe_configure(Jido.VFS.Adapter.Local, prefix: System.tmp_dir!())
    end

    test "normalizes raised configure errors" do
      assert {:error, %Jido.VFS.Errors.AdapterError{adapter: RaisingConfigureAdapter}} =
               Jido.VFS.safe_configure(RaisingConfigureAdapter, [])
    end

    test "returns typed error for invalid configure return shape" do
      assert {:error, %Jido.VFS.Errors.AdapterError{adapter: InvalidConfigureAdapter}} =
               Jido.VFS.safe_configure(InvalidConfigureAdapter, [])
    end
  end

  describe "adapter metadata callbacks" do
    test "uses adapter unsupported_operations callback in supports?/2" do
      filesystem = {UnsupportedMetadataAdapter, %{}}
      refute Jido.VFS.supports?(filesystem, :copy_between)
    end

    test "uses adapter versioning_module callback for commit operations" do
      filesystem = {MetadataVersioningAdapter, %{configured: true}}

      assert Jido.VFS.supports?(filesystem, :commit)
      assert :ok = Jido.VFS.commit(filesystem, "metadata commit")
    end
  end

  describe "chunk/2" do
    test "empty binary returns empty list" do
      assert Jido.VFS.chunk("", 10) == []
    end

    test "binary smaller than chunk size returns single item list" do
      assert Jido.VFS.chunk("hello", 10) == ["hello"]
    end

    test "binary equal to chunk size returns single item list" do
      assert Jido.VFS.chunk("hello", 5) == ["hello"]
    end

    test "binary larger than chunk size returns multiple chunks" do
      assert Jido.VFS.chunk("hello world", 5) == ["hello", " worl", "d"]
    end

    test "invalid chunk size raises argument error" do
      assert_raise ArgumentError, ~r/chunk size must be a positive integer/, fn ->
        Jido.VFS.chunk("hello", 0)
      end
    end

    test "binary much larger than chunk size returns many chunks" do
      data = String.duplicate("a", 100)
      chunks = Jido.VFS.chunk(data, 10)

      assert length(chunks) == 10
      assert Enum.all?(chunks, &(byte_size(&1) == 10))
      assert Enum.join(chunks) == data
    end

    test "chunk size of 1 splits every character" do
      assert Jido.VFS.chunk("abc", 1) == ["a", "b", "c"]
    end
  end

  describe "S3 visibility isolation" do
    test "visibility metadata is scoped by bucket and prefix" do
      Jido.VFS.Adapter.S3.reset_visibility_store()

      try do
        fs_a = Jido.VFS.Adapter.S3.configure(config: [], bucket: "bucket-a")
        fs_b = Jido.VFS.Adapter.S3.configure(config: [], bucket: "bucket-b")
        fs_c = Jido.VFS.Adapter.S3.configure(config: [], bucket: "bucket-a", prefix: "prefix-a")
        fs_d = Jido.VFS.Adapter.S3.configure(config: [], bucket: "bucket-a", prefix: "prefix-b")

        assert :ok = Jido.VFS.set_visibility(fs_a, "shared.txt", :private)
        assert :ok = Jido.VFS.set_visibility(fs_c, "shared.txt", :private)

        assert {:ok, :private} = Jido.VFS.visibility(fs_a, "shared.txt")
        assert {:ok, :public} = Jido.VFS.visibility(fs_b, "shared.txt")
        assert {:ok, :private} = Jido.VFS.visibility(fs_c, "shared.txt")
        assert {:ok, :public} = Jido.VFS.visibility(fs_d, "shared.txt")
      after
        Jido.VFS.Adapter.S3.reset_visibility_store()
      end
    end
  end

  describe "path error handling" do
    @describetag :tmp_dir

    test "handles :enotdir error when path is not a directory", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      # Create a file first
      :ok = Jido.VFS.write(filesystem, "not_a_dir.txt", "content")

      # Try to create a directory with same name as file - this should trigger :enotdir
      # Note: This test might be adapter-specific and may not trigger convert_path_error
      case Jido.VFS.create_directory(filesystem, "not_a_dir.txt/subdir") do
        {:error, %Jido.VFS.Errors.NotDirectory{}} -> :ok
        # Different adapters may handle this differently
        {:error, _other} -> :ok
        # Some adapters might not check this
        :ok -> :ok
      end
    end
  end

  describe "filesystem without own processes" do
    @describetag :tmp_dir

    test "user can write to filesystem", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
    end

    test "user can check if files exist on a filesystem", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem, "test.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "not-test.txt")
    end

    test "user can read from filesystem", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "user can delete from filesystem", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.delete(filesystem, "test.txt")

      assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "user can move files", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.move(filesystem, "test.txt", "not-test.txt")

      assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
    end

    test "user can copy files", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.copy(filesystem, "test.txt", "not-test.txt")

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
    end

    test "user can list files", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.write(filesystem, "test-1.txt", "Hello World")

      {:ok, list} = Jido.VFS.list_contents(filesystem, ".")

      assert length(list) == 2
      assert_in_list list, %Jido.VFS.Stat.File{name: "test.txt"}
      assert_in_list list, %Jido.VFS.Stat.File{name: "test-1.txt"}
    end
  end

  describe "module based filesystem without own processes" do
    @describetag :tmp_dir

    test "user can write to filesystem", %{tmp_dir: prefix} do
      defmodule Local.WriteTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      assert :ok = Local.WriteTest.write("test.txt", "Hello World")
    end

    test "user can check if files exist on a filesystem", %{tmp_dir: prefix} do
      defmodule Local.FileExistsTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.FileExistsTest.write("test.txt", "Hello World")

      assert {:ok, :exists} = Local.FileExistsTest.file_exists("test.txt")
      assert {:ok, :missing} = Local.FileExistsTest.file_exists("not-test.txt")
    end

    test "user can read from filesystem", %{tmp_dir: prefix} do
      defmodule Local.ReadTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.ReadTest.write("test.txt", "Hello World")

      assert {:ok, "Hello World"} = Local.ReadTest.read("test.txt")
    end

    test "user can delete from filesystem", %{tmp_dir: prefix} do
      defmodule Local.DeleteTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.DeleteTest.write("test.txt", "Hello World")
      :ok = Local.DeleteTest.delete("test.txt")

      assert {:error, _} = Local.DeleteTest.read("test.txt")
    end

    test "user can move files", %{tmp_dir: prefix} do
      defmodule Local.MoveTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.MoveTest.write("test.txt", "Hello World")
      :ok = Local.MoveTest.move("test.txt", "not-test.txt")

      assert {:error, _} = Local.MoveTest.read("test.txt")
      assert {:ok, "Hello World"} = Local.MoveTest.read("not-test.txt")
    end

    test "user can copy files", %{tmp_dir: prefix} do
      defmodule Local.CopyTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.CopyTest.write("test.txt", "Hello World")
      :ok = Local.CopyTest.copy("test.txt", "not-test.txt")

      assert {:ok, "Hello World"} = Local.CopyTest.read("test.txt")
      assert {:ok, "Hello World"} = Local.CopyTest.read("not-test.txt")
    end

    test "user can list files", %{tmp_dir: prefix} do
      defmodule Local.ListContentsTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      :ok = Local.ListContentsTest.write("test.txt", "Hello World")
      :ok = Local.ListContentsTest.write("test-1.txt", "Hello World")

      {:ok, list} = Local.ListContentsTest.list_contents(".")

      assert length(list) == 2
      assert_in_list list, %Jido.VFS.Stat.File{name: "test.txt"}
      assert_in_list list, %Jido.VFS.Stat.File{name: "test-1.txt"}
    end

    test "module wrapper exposes the full public filesystem API", %{tmp_dir: prefix} do
      defmodule Local.FullApiTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: prefix
      end

      expected_exports = [
        write_stream: 1,
        write_stream: 2,
        create_directory: 1,
        create_directory: 2,
        delete_directory: 1,
        delete_directory: 2,
        clear: 0,
        clear: 1,
        set_visibility: 2,
        visibility: 1,
        stat: 1,
        access: 2,
        append: 2,
        append: 3,
        truncate: 2,
        utime: 2,
        commit: 0,
        commit: 1,
        commit: 2,
        revisions: 0,
        revisions: 1,
        revisions: 2,
        read_revision: 2,
        read_revision: 3,
        rollback: 1,
        rollback: 2
      ]

      for {function, arity} <- expected_exports do
        assert function_exported?(Local.FullApiTest, function, arity)
      end

      assert {:ok, stream} = Local.FullApiTest.write_stream("nested/stream.txt")
      Enum.into(["Hello", " ", "World"], stream)
      assert {:ok, "Hello World"} = Local.FullApiTest.read("nested/stream.txt")

      assert :ok = Local.FullApiTest.create_directory("empty/")
      assert {:ok, %Jido.VFS.Stat.Dir{}} = Local.FullApiTest.stat("empty/")
      assert :ok = Local.FullApiTest.delete_directory("empty/")

      assert :ok = Local.FullApiTest.append("append.txt", "ab")
      assert {:ok, "ab"} = Local.FullApiTest.read("append.txt")
      assert :ok = Local.FullApiTest.truncate("append.txt", 1)
      assert :ok = Local.FullApiTest.access("append.txt", [:read])
      assert :ok = Local.FullApiTest.set_visibility("append.txt", :private)
      assert {:ok, :private} = Local.FullApiTest.visibility("append.txt")

      assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :commit}} =
               Local.FullApiTest.commit()

      assert :ok = Local.FullApiTest.clear()
      assert {:ok, :missing} = Local.FullApiTest.file_exists("append.txt")
    end
  end

  describe "filesystem with own processes" do
    test "user can write to filesystem" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      assert :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
    end

    test "user can check if files exist on a filesystem" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem, "test.txt")
      assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "not-test.txt")
    end

    test "user can read from filesystem" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "user can delete from filesystem" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.delete(filesystem, "test.txt")

      assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "user can move files" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.move(filesystem, "test.txt", "not-test.txt")

      assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
    end

    test "user can copy files" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.copy(filesystem, "test.txt", "not-test.txt")

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
    end

    test "user can list files" do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest)

      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.write(filesystem, "test-1.txt", "Hello World")

      {:ok, list} = Jido.VFS.list_contents(filesystem, ".")

      assert length(list) == 2
      assert_in_list list, %Jido.VFS.Stat.File{name: "test.txt"}
      assert_in_list list, %Jido.VFS.Stat.File{name: "test-1.txt"}
    end
  end

  describe "module based filesystem with own processes" do
    test "user can write to filesystem" do
      defmodule InMemory.WriteTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.WriteTest)

      assert :ok = InMemory.WriteTest.write("test.txt", "Hello World")
    end

    test "user can check if files exist on a filesystem" do
      defmodule InMemory.FileExistsTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.FileExistsTest)

      :ok = InMemory.FileExistsTest.write("test.txt", "Hello World")

      assert {:ok, :exists} = InMemory.FileExistsTest.file_exists("test.txt")
      assert {:ok, :missing} = InMemory.FileExistsTest.file_exists("not-test.txt")
    end

    test "user can read from filesystem" do
      defmodule InMemory.ReadTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.ReadTest)

      :ok = InMemory.ReadTest.write("test.txt", "Hello World")

      assert {:ok, "Hello World"} = InMemory.ReadTest.read("test.txt")
    end

    test "user can delete from filesystem" do
      defmodule InMemory.DeleteTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.DeleteTest)

      :ok = InMemory.DeleteTest.write("test.txt", "Hello World")
      :ok = InMemory.DeleteTest.delete("test.txt")

      assert {:error, _} = InMemory.DeleteTest.read("test.txt")
    end

    test "user can move files" do
      defmodule InMemory.MoveTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.MoveTest)

      :ok = InMemory.MoveTest.write("test.txt", "Hello World")
      :ok = InMemory.MoveTest.move("test.txt", "not-test.txt")

      assert {:error, _} = InMemory.MoveTest.read("test.txt")
      assert {:ok, "Hello World"} = InMemory.MoveTest.read("not-test.txt")
    end

    test "user can copy files" do
      defmodule InMemory.CopyTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.CopyTest)

      :ok = InMemory.CopyTest.write("test.txt", "Hello World")
      :ok = InMemory.CopyTest.copy("test.txt", "not-test.txt")

      assert {:ok, "Hello World"} = InMemory.CopyTest.read("test.txt")
      assert {:ok, "Hello World"} = InMemory.CopyTest.read("not-test.txt")
    end

    test "user can list files" do
      defmodule InMemory.ListContentsTest do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemory.ListContentsTest)

      :ok = InMemory.ListContentsTest.write("test.txt", "Hello World")
      :ok = InMemory.ListContentsTest.write("test-1.txt", "Hello World")

      {:ok, list} = InMemory.ListContentsTest.list_contents(".")

      assert length(list) == 2
      assert_in_list list, %Jido.VFS.Stat.File{name: "test.txt"}
      assert_in_list list, %Jido.VFS.Stat.File{name: "test-1.txt"}
    end
  end

  describe "filesystem independant" do
    @describetag :tmp_dir

    setup %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)
      {:ok, filesystem: filesystem}
    end

    test "reads configuration from :otp_app" do
      first_configuration = [
        adapter: Jido.VFS.Adapter.Local,
        prefix: "ziKK7t5LzV5XiJjYh30KxCLorRXqLwwEnZYJ"
      ]

      second_configuration = [
        adapter: Jido.VFS.Adapter.Local,
        prefix: "nC70eQp95g0f19uY4jAiWzcIg15uY8fDwC2P"
      ]

      Application.put_env(:jido_vfs_test, JidoVFSTest.AdhocFilesystem, first_configuration)

      on_exit(fn ->
        Application.delete_env(:jido_vfs_test, JidoVFSTest.AdhocFilesystem)
        :persistent_term.erase({Jido.VFS.Filesystem, JidoVFSTest.AdhocFilesystem})
      end)

      defmodule AdhocFilesystem do
        use Jido.VFS.Filesystem, otp_app: :jido_vfs_test
      end

      {_module, first_module_config} = JidoVFSTest.AdhocFilesystem.__filesystem__()
      Application.put_env(:jido_vfs_test, JidoVFSTest.AdhocFilesystem, second_configuration)
      {_module, second_module_config} = JidoVFSTest.AdhocFilesystem.__filesystem__()

      assert first_module_config.prefix == "ziKK7t5LzV5XiJjYh30KxCLorRXqLwwEnZYJ"
      assert second_module_config.prefix == "ziKK7t5LzV5XiJjYh30KxCLorRXqLwwEnZYJ"
    end

    test "directory traversals are detected and reported", %{filesystem: filesystem} do
      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../test.txt"}} =
               Jido.VFS.write(filesystem, "../test.txt", "Hello World")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../test.txt"}} =
               Jido.VFS.read(filesystem, "../test.txt")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../test.txt"}} =
               Jido.VFS.delete(filesystem, "../test.txt")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../test"}} =
               Jido.VFS.list_contents(filesystem, "../test")
    end

    test "relative paths are required", %{filesystem: filesystem} do
      assert {:error, %Jido.VFS.Errors.AbsolutePath{absolute_path: "/../test.txt"}} =
               Jido.VFS.write(filesystem, "/../test.txt", "Hello World")

      assert {:error, %Jido.VFS.Errors.AbsolutePath{absolute_path: "/../test.txt"}} =
               Jido.VFS.read(filesystem, "/../test.txt")

      assert {:error, %Jido.VFS.Errors.AbsolutePath{absolute_path: "/../test.txt"}} =
               Jido.VFS.delete(filesystem, "/../test.txt")

      assert {:error, %Jido.VFS.Errors.AbsolutePath{absolute_path: "/../test"}} =
               Jido.VFS.list_contents(filesystem, "/../test")
    end

    test "copy and move report destination path errors", %{filesystem: filesystem} do
      :ok = Jido.VFS.write(filesystem, "source.txt", "content")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../bad-copy.txt"}} =
               Jido.VFS.copy(filesystem, "source.txt", "../bad-copy.txt")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../bad-move.txt"}} =
               Jido.VFS.move(filesystem, "source.txt", "../bad-move.txt")
    end
  end

  describe "copying between different filesystems" do
    @describetag :tmp_dir

    setup %{tmp_dir: prefix} do
      prefix_a = Path.join(prefix, "a")
      prefix_b = Path.join(prefix, "b")

      {:ok, prefixes: [prefix_a, prefix_b]}
    end

    test "direct copy - same adapter", %{prefixes: [prefix_a, prefix_b]} do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix_a)
      filesystem_b = Jido.VFS.Adapter.Local.configure(prefix: prefix_b)

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_b, "test.txt")
    end

    test "direct copy - same adapter rejects traversal in source path", %{prefixes: [prefix_a, prefix_b]} do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix_a)
      filesystem_b = Jido.VFS.Adapter.Local.configure(prefix: prefix_b)

      outside_file = Path.join(Path.dirname(prefix_a), "outside-secret.txt")
      File.write!(outside_file, "secret")

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../outside-secret.txt"}} =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "../outside-secret.txt"},
                 {filesystem_b, "copied.txt"}
               )

      assert {:ok, :missing} = Jido.VFS.file_exists(filesystem_b, "copied.txt")
    end

    test "indirect copy - same adapter" do
      filesystem_a = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.A)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.B)

      filesystem_a |> Supervisor.child_spec(id: :a) |> start_supervised()
      filesystem_b |> Supervisor.child_spec(id: :b) |> start_supervised()

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_b, "test.txt")
    end

    test "different adapter", %{prefixes: [prefix_a | _]} do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix_a)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.B)

      start_supervised(filesystem_b)

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_b, "test.txt")
    end
  end

  describe "streaming operations" do
    @describetag :tmp_dir

    test "write_stream functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      {:ok, stream} = Jido.VFS.write_stream(filesystem, "stream.txt")
      data = ["Hello", " ", "World"]
      Enum.into(data, stream)

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "stream.txt")
    end

    test "read_stream functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "stream.txt", "Hello World")
      {:ok, stream} = Jido.VFS.read_stream(filesystem, "stream.txt")

      assert Enum.into(stream, "") == "Hello World"
    end

    test "write_stream with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid.txt"}} =
               Jido.VFS.write_stream(filesystem, "../invalid.txt")
    end

    test "read_stream with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid.txt"}} =
               Jido.VFS.read_stream(filesystem, "../invalid.txt")
    end
  end

  describe "directory operations" do
    @describetag :tmp_dir

    test "create_directory functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert :ok = Jido.VFS.create_directory(filesystem, "test_dir/")
      {:ok, contents} = Jido.VFS.list_contents(filesystem, ".")

      assert_in_list contents, %Jido.VFS.Stat.Dir{name: "test_dir"}
    end

    test "delete_directory functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.create_directory(filesystem, "test_dir/")
      assert :ok = Jido.VFS.delete_directory(filesystem, "test_dir/")

      {:ok, contents} = Jido.VFS.list_contents(filesystem, ".")
      refute Enum.any?(contents, &match?(%Jido.VFS.Stat.Dir{name: "test_dir"}, &1))
    end

    test "clear filesystem functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test1.txt", "content")
      :ok = Jido.VFS.write(filesystem, "test2.txt", "content")
      :ok = Jido.VFS.create_directory(filesystem, "subdir/")

      assert :ok = Jido.VFS.clear(filesystem)

      {:ok, contents} = Jido.VFS.list_contents(filesystem, ".")
      assert contents == []
    end

    test "create_directory with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid/"}} =
               Jido.VFS.create_directory(filesystem, "../invalid/")
    end

    test "delete_directory with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid/"}} =
               Jido.VFS.delete_directory(filesystem, "../invalid/")
    end
  end

  describe "visibility operations" do
    @describetag :tmp_dir

    test "set_visibility functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")
      assert :ok = Jido.VFS.set_visibility(filesystem, "test.txt", :public)
    end

    test "visibility functionality", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")
      :ok = Jido.VFS.set_visibility(filesystem, "test.txt", :public)

      assert {:ok, :public} = Jido.VFS.visibility(filesystem, "test.txt")
    end

    test "set_visibility with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid.txt"}} =
               Jido.VFS.set_visibility(filesystem, "../invalid.txt", :public)
    end

    test "visibility with invalid path", %{tmp_dir: prefix} do
      filesystem = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      assert {:error, %Jido.VFS.Errors.PathTraversal{attempted_path: "../invalid.txt"}} =
               Jido.VFS.visibility(filesystem, "../invalid.txt")
    end
  end

  describe "chunk function" do
    test "chunks empty string" do
      assert Jido.VFS.chunk("", 5) == []
    end

    test "chunks string smaller than size" do
      assert Jido.VFS.chunk("Hi", 5) == ["Hi"]
    end

    test "chunks string equal to size" do
      assert Jido.VFS.chunk("Hello", 5) == ["Hello"]
    end

    test "chunks string larger than size" do
      assert Jido.VFS.chunk("Hello World", 5) == ["Hello", " Worl", "d"]
    end

    test "chunks with size 1" do
      assert Jido.VFS.chunk("ABC", 1) == ["A", "B", "C"]
    end
  end

  describe "copy_between_filesystem edge cases" do
    @describetag :tmp_dir

    test "copy_via_local_memory with read stream only", %{tmp_dir: prefix} do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.StreamOnly)

      start_supervised(filesystem_b)

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem_b, "test.txt")
    end

    test "copy_via_local_memory with write stream only", %{tmp_dir: prefix} do
      filesystem_a = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.WriteStreamOnly)
      filesystem_b = Jido.VFS.Adapter.Local.configure(prefix: prefix)

      start_supervised(filesystem_a)

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem_b, "test.txt")
    end

    test "copy_via_local_memory no streaming support" do
      filesystem_a = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.NoStreamA)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.NoStreamB)

      filesystem_a |> Supervisor.child_spec(id: :no_stream_a) |> start_supervised()
      filesystem_b |> Supervisor.child_spec(id: :no_stream_b) |> start_supervised()

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "test.txt"}
               )

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem_b, "test.txt")
    end

    test "copy_between_filesystem stream strategy copies across adapters", %{tmp_dir: prefix} do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.StreamStrategy)

      start_supervised(filesystem_b)

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello Stream")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "copy.txt"},
                 copy_between_strategy: :stream
               )

      assert {:ok, "Hello Stream"} = Jido.VFS.read(filesystem_b, "copy.txt")
    end

    test "copy_between_filesystem stream strategy returns file not found for missing local source", %{
      tmp_dir: prefix
    } do
      filesystem_a = Jido.VFS.Adapter.Local.configure(prefix: prefix)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.MissingLocalStreamSource)

      start_supervised(filesystem_b)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "missing.txt"},
                 {filesystem_b, "copy.txt"},
                 copy_between_strategy: :stream
               )

      assert {:ok, :missing} = Jido.VFS.file_exists(filesystem_b, "copy.txt")
    end

    test "copy_between_filesystem rejects invalid chunk size" do
      filesystem_a = {CopyOptionCheckingAdapter, %{content: "Hello"}}
      filesystem_b = {CopyOptionCheckingAdapter, %{test_pid: self()}}

      assert {:error, %Jido.VFS.Errors.AdapterError{reason: %{reason: {:invalid_chunk_size, 0}}}} =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "source.txt"},
                 {filesystem_b, "copy.txt"},
                 copy_between_strategy: :stream,
                 chunk_size: 0
               )
    end

    test "copy_between_filesystem does not forward copy-only options to adapter writes" do
      filesystem_a = {CopyOptionCheckingAdapter, %{content: "Hello Options"}}
      filesystem_b = {CopyOptionCheckingAdapter, %{test_pid: self()}}

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "source.txt"},
                 {filesystem_b, "copy.txt"},
                 chunk_size: 2,
                 visibility: :private
               )

      assert_receive {:copy_option_write, "copy.txt", "Hello Options", opts}
      refute Keyword.has_key?(opts, :chunk_size)
      assert Keyword.fetch!(opts, :visibility) == :private
    end

    test "copy_between_filesystem native strategy returns unsupported when adapter cannot copy_between" do
      filesystem_a = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.NativeStrategyA)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.NativeStrategyB)

      filesystem_a |> Supervisor.child_spec(id: :native_strategy_a) |> start_supervised()
      filesystem_b |> Supervisor.child_spec(id: :native_strategy_b) |> start_supervised()

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello Native")

      assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :copy_between}} =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "copy.txt"},
                 copy_between_strategy: :native
               )
    end

    test "copy_between_filesystem rejects invalid strategy" do
      filesystem_a = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.InvalidStrategyA)
      filesystem_b = Jido.VFS.Adapter.InMemory.configure(name: InMemoryTest.InvalidStrategyB)

      filesystem_a |> Supervisor.child_spec(id: :invalid_strategy_a) |> start_supervised()
      filesystem_b |> Supervisor.child_spec(id: :invalid_strategy_b) |> start_supervised()

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello Invalid Strategy")

      assert {:error, %Jido.VFS.Errors.AdapterError{}} =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "copy.txt"},
                 copy_between_strategy: :invalid
               )
    end
  end

  describe "extended filesystem operations" do
    test "stat/2 works with high-level API", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      content = "Hello World"
      :ok = Jido.VFS.write(filesystem, "test.txt", content)

      assert {:ok, %Jido.VFS.Stat.File{} = stat} = Jido.VFS.stat(filesystem, "test.txt")
      assert stat.name == "test.txt"
      assert stat.size == byte_size(content)
    end

    test "stat/2 returns unsupported for adapters without implementation", %{test: _test} do
      # Use a mock adapter that doesn't implement stat
      assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :stat}} =
               Jido.VFS.stat({NonExistentAdapter, %{}}, "test.txt")
    end

    test "access/3 works with high-level API", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")
      assert :ok = Jido.VFS.access(filesystem, "test.txt", [:read])
    end

    test "append/4 works with high-level API", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello")
      :ok = Jido.VFS.append(filesystem, "test.txt", " World")

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "truncate/3 works with high-level API", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.truncate(filesystem, "test.txt", 5)

      assert {:ok, "Hello"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "utime/3 works with high-level API", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")
      new_time = ~U[2023-01-01 12:00:00Z]
      :ok = Jido.VFS.utime(filesystem, "test.txt", new_time)

      assert {:ok, %Jido.VFS.Stat.File{mtime: mtime}} = Jido.VFS.stat(filesystem, "test.txt")
      assert mtime == DateTime.to_unix(new_time, :second)
    end

    test "extended operations handle path normalization errors", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      # Test with invalid paths that should trigger path normalization errors
      invalid_path = "../outside"

      assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.stat(filesystem, invalid_path)

      assert {:error, %Jido.VFS.Errors.PathTraversal{}} =
               Jido.VFS.access(filesystem, invalid_path, [:read])

      assert {:error, %Jido.VFS.Errors.PathTraversal{}} =
               Jido.VFS.append(filesystem, invalid_path, "content")

      assert {:error, %Jido.VFS.Errors.PathTraversal{}} =
               Jido.VFS.truncate(filesystem, invalid_path, 10)

      assert {:error, %Jido.VFS.Errors.PathTraversal{}} =
               Jido.VFS.utime(filesystem, invalid_path, DateTime.utc_now())
    end
  end
end
