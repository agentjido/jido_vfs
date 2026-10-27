defmodule Jido.VFS.CopyFailureTest do
  use ExUnit.Case, async: true
  alias Jido.VFS
  alias Jido.VFS.Errors
  alias JidoVfsTest.CopyCollector, as: Collector

  defmodule Source do
    def read(config, _), do: config
  end

  defmodule StreamingSource do
    def read_stream(config, _, _), do: config
  end

  defmodule Destination do
    def write(config, path, bytes, opts) do
      send(config.owner, {:write, path, bytes, opts})
      Map.get(config, :write, :ok)
    end
  end

  defmodule AppendingDestination do
    defdelegate write(config, path, bytes, opts), to: Destination

    def append(config, path, bytes, opts) do
      send(config.owner, {:append, path, bytes, opts})
      Map.get(config, :append, :ok)
    end
  end

  defmodule StreamingDestination do
    defdelegate write(config, path, bytes, opts), to: Destination
    def write_stream(config, _, _), do: config.stream
  end

  @tag :tmp_dir
  test "fallback copies preserve bytes and remove their temporary files", %{tmp_dir: directory} do
    for strategy <- [:stream, :tempfile],
        source <- [{Source, {:ok, "abcdef"}}, {StreamingSource, {:ok, ["abc", "def"]}}] do
      destination = {Destination, %{owner: self()}}

      assert :ok =
               VFS.copy_between_filesystem({source, "from"}, {destination, "to"},
                 copy_between_strategy: strategy,
                 copy_between_temp_dir: directory,
                 chunk_size: 3
               )

      assert_receive {:write, "to", "abcdef", []}
      assert File.ls!(directory) == []
    end
  end

  @tag :tmp_dir
  test "append fallback writes bounded chunks and reports a failed chunk", %{tmp_dir: directory} do
    for strategy <- [:stream, :tempfile] do
      source = {StreamingSource, {:ok, ["abc", "def"]}}
      destination = {AppendingDestination, %{owner: self()}}
      opts = [copy_between_strategy: strategy, copy_between_temp_dir: directory, chunk_size: 3]
      assert :ok = VFS.copy_between_filesystem({source, "from"}, {destination, "to"}, opts)
      assert_receive {:write, "to", "", []}
      assert_receive {:append, "to", "abc", []}
      assert_receive {:append, "to", "def", []}

      assert {:error, %Errors.Unknown.Unknown{}} =
               VFS.copy_between_filesystem(
                 {source, "from"},
                 {{AppendingDestination, %{owner: self(), append: {:error, :full}}}, "to"},
                 opts
               )

      assert File.ls!(directory) == []
    end
  end

  @tag :tmp_dir
  test "source failures do not open the destination and always remove temporary files", %{tmp_dir: directory} do
    for strategy <- [:stream, :tempfile],
        source <- [{Source, {:error, :unavailable}}, {StreamingSource, {:error, :unavailable}}] do
      assert {:error, %Errors.Unknown.Unknown{}} =
               VFS.copy_between_filesystem({source, "from"}, {{Destination, %{owner: self()}}, "to"},
                 copy_between_strategy: strategy,
                 copy_between_temp_dir: directory
               )

      refute_receive {:write, _, _, _}
      assert File.ls!(directory) == []
    end
  end

  @tag :tmp_dir
  test "destination failures and collector exceptions identify the failed side", %{tmp_dir: directory} do
    for strategy <- [:stream, :tempfile],
        destination <- [
          {Destination, %{owner: self(), write: {:error, :full}}},
          {AppendingDestination, %{owner: self(), write: {:error, :full}}},
          {StreamingDestination, %{owner: self(), stream: {:error, :offline}}},
          {StreamingDestination, %{owner: self(), stream: {:ok, %Collector{failure: :raise}}}},
          {StreamingDestination, %{owner: self(), stream: {:ok, %Collector{failure: :throw}}}}
        ] do
      assert {:error, error} =
               VFS.copy_between_filesystem({{Source, {:ok, "data"}}, "from"}, {destination, "to"},
                 copy_between_strategy: strategy,
                 copy_between_temp_dir: directory
               )

      case destination do
        {StreamingDestination, %{stream: {:ok, _}}} ->
          assert %Errors.AdapterError{reason: %{side: :destination}} = error

        _ ->
          assert %Errors.Unknown.Unknown{} = error
      end

      assert File.ls!(directory) == []
    end
  end

  @tag :tmp_dir
  test "an unavailable temporary directory is refused before a copy starts", %{tmp_dir: directory} do
    file = Path.join(directory, "file")
    File.write!(file, "occupied")

    assert {:error,
            %Errors.AdapterError{
              reason: %{reason: {:temp_dir_unavailable, ^file, filesystem_reason}}
            }} =
             VFS.copy_between_filesystem({{Source, {:ok, "data"}}, "from"}, {{Destination, %{owner: self()}}, "to"},
               copy_between_temp_dir: file
             )

    assert filesystem_reason in [:eexist, :enotdir]
    assert File.read!(file) == "occupied"
    refute_receive {:write, _, _, _}
  end

  @tag :tmp_dir
  test "lazy source exceptions close the temporary file and retain error evidence", %{tmp_dir: directory} do
    for failure <- [:raise, :throw] do
      chunks =
        Stream.map(["data"], fn _ ->
          case failure do
            :raise -> raise "read failed"
            :throw -> throw(:read_failed)
          end
        end)

      assert {:error, %Errors.AdapterError{reason: %{side: :source}}} =
               VFS.copy_between_filesystem(
                 {{StreamingSource, {:ok, chunks}}, "from"},
                 {{Destination, %{owner: self()}}, "to"},
                 copy_between_temp_dir: directory
               )

      assert File.ls!(directory) == []
    end
  end
end
