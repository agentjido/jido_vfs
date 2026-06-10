defmodule Jido.VFS.Adapter.InMemoryTest do
  use ExUnit.Case, async: true
  import Jido.VFS.AdapterTest
  doctest Jido.VFS.Adapter.InMemory

  adapter_test %{test: test} do
    filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
    start_supervised(filesystem)
    {:ok, filesystem: filesystem}
  end

  describe "write" do
    test "success", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok = Jido.VFS.Adapter.InMemory.write(config, "test.txt", "Hello World", [])

      assert {:ok, {"Hello World", _meta}} =
               Agent.get(via(test), fn state ->
                 state
                 |> elem(0)
                 |> Map.fetch!("/")
                 |> elem(0)
                 |> Map.fetch("test.txt")
               end)
    end

    test "folders are automatically created is missing", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok = Jido.VFS.Adapter.InMemory.write(config, "folder/test.txt", "Hello World", [])

      assert {:ok, "Hello World"} = Jido.VFS.Adapter.InMemory.read(config, "folder/test.txt")
    end
  end

  describe "read" do
    test "success", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, "Hello World"} = Jido.VFS.Adapter.InMemory.read(config, "test.txt")
    end

    test "stream success", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, %Jido.VFS.Adapter.InMemory.AgentStream{} = stream} =
               Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", [])

      assert Enum.into(stream, <<>>) == "Hello World"
    end

    test "stream with custom chunk size", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, stream} =
               Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 3)

      chunks = Enum.to_list(stream)
      assert length(chunks) > 1
      assert Enum.join(chunks) == "Hello World"
    end

    test "stream enumerable protocol count/1 fallback works", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 5)
      # Enum.count/1 falls back to reduce when count/1 returns error
      assert Enum.count(stream) > 0
    end

    test "stream enumerable protocol slice/1 fallback works", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 5)
      # Enum.slice/3 falls back to reduce when slice/1 returns error
      result = Enum.slice(stream, 0, 1)
      assert is_list(result)
    end

    test "stream enumerable protocol member?/2 fallback works", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", [])
      # Enum.member?/2 falls back to reduce when member?/2 returns error
      assert Enum.member?(stream, "Hello") == true
    end

    test "stream for non-existent file returns typed error", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.read_stream(config, "missing.txt", [])
    end

    test "stream suspend and resume functionality", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 2)

      # Test suspend/resume by taking only first 2 chunks
      result = Enum.take(stream, 2)
      assert length(result) == 2
      assert is_binary(hd(result))
    end
  end

  describe "write_stream" do
    test "collectable protocol writes data correctly", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.write_stream(config, "output.txt", [])

      data = ["Hello", " ", "World"]
      result_stream = Enum.into(data, stream)

      assert result_stream.path == "output.txt"
      assert {:ok, "Hello World"} = Jido.VFS.Adapter.InMemory.read(config, "output.txt")
    end

    test "collectable protocol appends to existing file", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      # Write initial content
      :ok = Jido.VFS.Adapter.InMemory.write(config, "append.txt", "Initial ", [])

      {:ok, stream} = Jido.VFS.Adapter.InMemory.write_stream(config, "append.txt", [])

      data = ["appended", " content"]
      Enum.into(data, stream)

      assert {:ok, "Initial appended content"} = Jido.VFS.Adapter.InMemory.read(config, "append.txt")
    end

    test "collectable protocol handles halt", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      {:ok, stream} = Jido.VFS.Adapter.InMemory.write_stream(config, "halt.txt", [])

      # Simulate halt by accessing the collector function directly
      {[], collector_fun} = Collectable.into(stream)
      result = collector_fun.([], :halt)

      assert result == :ok
    end
  end

  describe "delete" do
    test "success", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert :ok = Jido.VFS.Adapter.InMemory.delete(config, "test.txt")

      assert :error =
               Agent.get(via(test), fn state ->
                 state
                 |> elem(0)
                 |> Map.fetch!("/")
                 |> elem(0)
                 |> Map.fetch("test.txt")
               end)
    end

    test "successful even if no file to delete", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      assert :ok = Jido.VFS.Adapter.InMemory.delete(config, "test.txt")
    end
  end

  describe "versioning" do
    test "write_version creates version and returns version_id", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:ok, version_id} =
               Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert is_binary(version_id)
      assert String.length(version_id) == 32
    end

    test "read_version retrieves specific version", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, version_id} =
        Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert {:ok, "Hello World v1"} =
               Jido.VFS.Adapter.InMemory.read_version(config, "test.txt", version_id)
    end

    test "list_versions returns all versions for a path", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 2
      assert Enum.any?(versions, &(&1.version_id == v1))
      assert Enum.any?(versions, &(&1.version_id == v2))
    end

    test "get_latest_version returns most recent version", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, _v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert {:ok, ^v2} = Jido.VFS.Adapter.InMemory.get_latest_version(config, "test.txt")
    end

    test "restore_version restores file to specific version", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, _v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert :ok = Jido.VFS.Adapter.InMemory.restore_version(config, "test.txt", v1)
      assert {:ok, "Version 1"} = Jido.VFS.Adapter.InMemory.read(config, "test.txt")
    end

    test "delete_version removes specific version", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert :ok = Jido.VFS.Adapter.InMemory.delete_version(config, "test.txt", v1)

      {:ok, versions} = Jido.VFS.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 1
      assert hd(versions).version_id == v2

      assert {:error, _} = Jido.VFS.Adapter.InMemory.read_version(config, "test.txt", v1)
    end

    test "versioning preserves visibility", %{test: test} do
      {_, config} = filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, version_id} =
        Jido.VFS.Adapter.InMemory.write_version(config, "test.txt", "Content", visibility: :public)

      assert :ok = Jido.VFS.Adapter.InMemory.restore_version(config, "test.txt", version_id)
      assert {:ok, :public} = Jido.VFS.Adapter.InMemory.visibility(config, "test.txt")
    end
  end

  describe "extended filesystem operations" do
    test "stat/2 returns file metadata", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      content = "Hello World"
      :ok = Jido.VFS.write(filesystem, "test.txt", content)

      assert {:ok, %Jido.VFS.Stat.File{} = stat} =
               Jido.VFS.Adapter.InMemory.stat(elem(filesystem, 1), "test.txt")

      assert stat.name == "test.txt"
      assert stat.size == byte_size(content)
      assert is_integer(stat.mtime)
      assert stat.visibility == :private
    end

    test "stat/2 returns directory metadata", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.create_directory(filesystem, "test/")

      assert {:ok, %Jido.VFS.Stat.Dir{} = stat} =
               Jido.VFS.Adapter.InMemory.stat(elem(filesystem, 1), "test/")

      assert stat.name == "test"
      assert stat.size == 0
      assert is_integer(stat.mtime)
      assert stat.visibility == :private
    end

    test "stat/2 returns error for non-existent path", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.stat(elem(filesystem, 1), "missing.txt")
    end

    test "access/3 checks file permissions", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")

      assert :ok = Jido.VFS.Adapter.InMemory.access(elem(filesystem, 1), "test.txt", [:read])
      assert :ok = Jido.VFS.Adapter.InMemory.access(elem(filesystem, 1), "test.txt", [:write])
      assert :ok = Jido.VFS.Adapter.InMemory.access(elem(filesystem, 1), "test.txt", [:read, :write])
    end

    test "access/3 returns error for non-existent file", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.access(elem(filesystem, 1), "missing.txt", [:read])
    end

    test "append/4 appends to existing file", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello")
      :ok = Jido.VFS.Adapter.InMemory.append(elem(filesystem, 1), "test.txt", " World", [])

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "append/4 creates file if it doesn't exist", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.Adapter.InMemory.append(elem(filesystem, 1), "new.txt", "New content", [])

      assert {:ok, "New content"} = Jido.VFS.read(filesystem, "new.txt")
    end

    test "truncate/3 shrinks file", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.Adapter.InMemory.truncate(elem(filesystem, 1), "test.txt", 5)

      assert {:ok, "Hello"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "truncate/3 grows file with null bytes", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hi")
      :ok = Jido.VFS.Adapter.InMemory.truncate(elem(filesystem, 1), "test.txt", 5)

      assert {:ok, "Hi\0\0\0"} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "truncate/3 empties file when size is 0", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      :ok = Jido.VFS.Adapter.InMemory.truncate(elem(filesystem, 1), "test.txt", 0)

      assert {:ok, ""} = Jido.VFS.read(filesystem, "test.txt")
    end

    test "truncate/3 returns error for non-existent file", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.truncate(elem(filesystem, 1), "missing.txt", 10)
    end

    test "utime/3 updates modification time", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      :ok = Jido.VFS.write(filesystem, "test.txt", "content")
      new_time = ~U[2023-01-01 12:00:00Z]
      :ok = Jido.VFS.Adapter.InMemory.utime(elem(filesystem, 1), "test.txt", new_time)

      assert {:ok, %Jido.VFS.Stat.File{mtime: mtime}} =
               Jido.VFS.Adapter.InMemory.stat(elem(filesystem, 1), "test.txt")

      assert mtime == DateTime.to_unix(new_time, :second)
    end

    test "utime/3 returns error for non-existent file", %{test: test} do
      filesystem = Jido.VFS.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.Adapter.InMemory.utime(
                 elem(filesystem, 1),
                 "missing.txt",
                 DateTime.utc_now()
               )
    end
  end

  describe "copy_between_filesystem" do
    test "stream strategy returns file not found for missing source", %{test: test} do
      source = Jido.VFS.Adapter.InMemory.configure(name: :"#{test}_source")
      destination = Jido.VFS.Adapter.InMemory.configure(name: :"#{test}_destination")

      start_supervised!({Jido.VFS.Adapter.InMemory, source}, id: {test, :source})
      start_supervised!({Jido.VFS.Adapter.InMemory, destination}, id: {test, :destination})

      assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
               Jido.VFS.copy_between_filesystem(
                 {source, "missing.txt"},
                 {destination, "copied.txt"},
                 copy_between_strategy: :stream
               )

      assert {:ok, :missing} = Jido.VFS.file_exists(destination, "copied.txt")
    end
  end

  defp via(name) do
    Jido.VFS.Registry.via(Jido.VFS.Adapter.InMemory, name)
  end
end
