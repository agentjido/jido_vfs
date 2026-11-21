defmodule Jido.VFS.Adapter.InMemory do
  @moduledoc """
  Jido.VFS Adapter using an `Agent` for in memory storage.

  ## Direct usage

      iex> filesystem = Jido.VFS.Adapter.InMemory.configure(name: InMemoryFileSystem)
      iex> start_supervised(filesystem)
      iex> :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      iex> {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")

  ## Usage with a module

      defmodule InMemoryFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.InMemory
      end

      start_supervised(InMemoryFileSystem)

      InMemoryFileSystem.write("test.txt", "Hello World")
      {:ok, "Hello World"} = InMemoryFileSystem.read("test.txt")
  """

  alias Jido.VFS.Errors

  defmodule AgentStream do
    @moduledoc """
    Enumerable and collectable stream for chunked in-memory file access.
    """

    @type t :: %__MODULE__{
            config: map() | nil,
            path: String.t() | nil,
            chunk_size: pos_integer()
          }

    @enforce_keys [:config, :path]
    defstruct config: nil, path: nil, chunk_size: 1024

    defimpl Enumerable do
      def reduce(%{config: config, path: path, chunk_size: chunk_size} = stream, a, b) do
        case Jido.VFS.Adapter.InMemory.read(config, path) do
          {:ok, contents} ->
            contents
            |> Jido.VFS.chunk(chunk_size)
            |> reduce_list(stream, a, b)

          _ ->
            {:halted, []}
        end
      end

      defp reduce_list(_list, _stream, {:halt, acc}, _fun), do: {:halted, acc}
      defp reduce_list(list, stream, {:suspend, acc}, fun), do: {:suspended, acc, &reduce_list(list, stream, &1, fun)}
      defp reduce_list([], _stream, {:cont, acc}, _fun), do: {:done, acc}
      defp reduce_list([head | tail], stream, {:cont, acc}, fun), do: reduce_list(tail, stream, fun.(head, acc), fun)

      def count(_), do: {:error, __MODULE__}
      def slice(_), do: {:error, __MODULE__}
      def member?(_, _), do: {:error, __MODULE__}
    end

    defimpl Collectable do
      def into(%{config: config, path: path} = stream) do
        fun = fn
          list, {:cont, x} ->
            [x | list]

          list, :done ->
            contents = IO.iodata_to_binary(:lists.reverse(list))
            :ok = Jido.VFS.Adapter.InMemory.append(config, path, contents, [])
            stream

          _, :halt ->
            :ok
        end

        {[], fun}
      end
    end
  end

  use Agent

  defmodule Config do
    @moduledoc """
    Runtime configuration for the in-memory adapter.
    """

    @type t :: %__MODULE__{
            name: atom() | module() | pid() | term() | nil
          }

    defstruct name: nil
  end

  @behaviour Jido.VFS.Adapter

  @impl Jido.VFS.Adapter
  def unsupported_operations, do: [:copy_between]

  @impl Jido.VFS.Adapter
  def versioning_module, do: Jido.VFS.Adapter.InMemory.Versioning

  defp unsupported(operation) do
    {:error, Errors.UnsupportedOperation.exception(operation: operation, adapter: __MODULE__)}
  end

  @impl Jido.VFS.Adapter
  def starts_processes, do: true

  def start_link({__MODULE__, %Config{} = config}) do
    start_link(config)
  end

  def start_link(%Config{} = config) do
    Agent.start_link(fn -> {{%{}, %{}, %{}}, make_ref()} end, name: Jido.VFS.Registry.via(__MODULE__, config.name))
  end

  @impl Jido.VFS.Adapter
  def configure(opts) do
    config = %Config{
      name: Keyword.fetch!(opts, :name)
    }

    {__MODULE__, config}
  end

  @impl Jido.VFS.Adapter
  def write(config, path, contents, opts) do
    state_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      put_file(state, path, contents, opts)
    end)
  end

  defp put_file(state, path, contents, opts) do
    visibility = Keyword.get(opts, :visibility, :private)
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)
    timestamp = System.system_time(:second)

    if match?({children, _} when is_map(children), get_in(state, accessor(path))),
      do: raise(ArgumentError, "path is a directory")

    old_meta =
      case get_in(state, accessor(path)) do
        {bytes, meta} when is_binary(bytes) -> meta
        _ -> %{}
      end

    mode = Keyword.get(opts, :mode, Map.get(old_meta, :mode, :regular))
    unless mode in [:regular, :executable], do: raise(ArgumentError, "invalid mode")

    file =
      {IO.iodata_to_binary(contents), %{visibility: visibility, mtime: timestamp, mode: mode}}

    directory = {%{}, %{visibility: directory_visibility, mtime: timestamp}}
    put_in(state, accessor(path, directory), file)
  end

  @impl Jido.VFS.Adapter
  def write_stream(config, path, opts) do
    {:ok,
     %AgentStream{
       config: config,
       path: path,
       chunk_size: Keyword.get(opts, :chunk_size, 1024)
     }}
  end

  @impl Jido.VFS.Adapter
  def read(config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {binary, _meta} when is_binary(binary) -> {:ok, binary}
        _ -> {:error, Errors.FileNotFound.exception(file_path: path)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def read_stream(config, path, opts) do
    case file_exists(config, path) do
      {:ok, :exists} ->
        {:ok,
         %AgentStream{
           config: config,
           path: path,
           chunk_size: Keyword.get(opts, :chunk_size, 1024)
         }}

      {:ok, :missing} ->
        {:error, Errors.FileNotFound.exception(file_path: path)}

      {:error, _} = error ->
        error
    end
  end

  @impl Jido.VFS.Adapter
  def delete(%Config{} = config, path) do
    state_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      {_, state} = pop_in(state, accessor(path))
      state
    end)
  end

  @impl Jido.VFS.Adapter
  def move(%Config{} = config, source, destination, opts) do
    visibility = Keyword.get(opts, :visibility, :private)
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)
    timestamp = System.system_time(:second)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(source)) do
        {binary, meta} when is_binary(binary) ->
          file = {binary, Map.merge(meta, %{visibility: visibility, mtime: timestamp})}
          directory = {%{}, %{visibility: directory_visibility, mtime: timestamp}}

          {_, state} =
            state |> put_in(accessor(destination, directory), file) |> pop_in(accessor(source))

          {:ok, state}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: source)}, state}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def copy(%Config{} = config, source, destination, opts) do
    visibility = Keyword.get(opts, :visibility, :private)
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)
    timestamp = System.system_time(:second)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(source)) do
        {binary, meta} when is_binary(binary) ->
          file = {binary, Map.merge(meta, %{visibility: visibility, mtime: timestamp})}
          directory = {%{}, %{visibility: directory_visibility, mtime: timestamp}}
          {:ok, put_in(state, accessor(destination, directory), file)}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: source)}, state}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def copy(
        %Config{} = _source_config,
        _source,
        %Config{} = _destination_config,
        _destination,
        _opts
      ) do
    unsupported(:copy_between)
  end

  @impl Jido.VFS.Adapter
  def file_exists(%Config{} = config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {binary, _meta} when is_binary(binary) -> {:ok, :exists}
        _ -> {:ok, :missing}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def list_contents(%Config{} = config, path) do
    contents =
      state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
        paths =
          case get_in(state, accessor(path)) do
            {%{} = map, _meta} -> map
            _ -> %{}
          end

        list_entries(paths)
      end)

    {:ok, contents}
  end

  @impl Jido.VFS.Adapter
  def create_directory(%Config{} = config, path, opts) do
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)
    directory = {%{}, %{visibility: directory_visibility, mtime: System.system_time(:second)}}

    state_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      put_in(state, accessor(path, directory), directory)
    end)
  end

  @impl Jido.VFS.Adapter
  def delete_directory(%Config{} = config, path, opts) do
    recursive? = Keyword.get(opts, :recursive, false)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case {recursive?, get_in(state, accessor(path))} do
        {_, nil} ->
          {:ok, state}

        {recursive?, {map, _meta}} when is_map(map) and (map_size(map) == 0 or recursive?) ->
          {_, state} = pop_in(state, accessor(path))
          {:ok, state}

        _ ->
          {{:error, Errors.DirectoryNotEmpty.exception(dir_path: path)}, state}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def clear(%Config{} = config) do
    state_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn _ -> {%{}, %{}, %{}} end)
  end

  @impl Jido.VFS.Adapter
  def set_visibility(%Config{} = config, path, visibility) do
    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {_, _} ->
          state =
            update_in(state, accessor(path), fn {contents, meta} ->
              {contents, Map.put(meta, :visibility, visibility)}
            end)

          {:ok, state}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: path)}, state}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def visibility(%Config{} = config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {_, %{visibility: visibility}} -> {:ok, visibility}
        _ -> {:error, Errors.FileNotFound.exception(file_path: path)}
      end
    end)
  end

  defp accessor(path, default \\ nil) when is_binary(path) do
    path
    |> Path.absname("/")
    |> Path.split()
    |> do_accessor([], default)
    |> Enum.reverse()
  end

  defp do_accessor([segment], acc, default) do
    [Access.key(segment, default), Access.elem(0) | acc]
  end

  defp do_accessor([segment | rest], acc, default) do
    intermediate_default = default || {%{}, %{}}
    do_accessor(rest, [Access.key(segment, intermediate_default), Access.elem(0) | acc], default)
  end

  defp list_entries(paths) do
    for {name, {content, meta}} <- paths do
      entry =
        case content do
          %{} -> %Jido.VFS.Stat.Dir{size: 0}
          bin when is_binary(bin) -> %Jido.VFS.Stat.File{size: byte_size(bin)}
        end

      %{entry | name: name, mtime: Map.get(meta, :mtime, 0), visibility: meta.visibility}
    end
  end

  @doc """
  Reads or edits the working tree with a single adapter operation.

  Use relative paths and `"."` for the root when reading a directory.
  The adapter applies a write batch in full or leaves the tree unchanged.
  A conditional write checks the current file in the same transaction.
  The check function must return `:ok` or `{:error, reason}`.
  """
  def working_file(config, :read, [path]) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      read_working_file(state, path)
    end)
  end

  def working_file(config, :read, [path, limit]) do
    case working_file(config, :read, [path]) do
      {:ok, bytes} when byte_size(bytes) > limit -> {:error, :discovery_byte_limit}
      result -> result
    end
  end

  def working_file(config, :read, [path, offset, count]) do
    case working_file(config, :read, [path]) do
      {:ok, bytes} when offset >= byte_size(bytes) ->
        {:ok, ""}

      {:ok, bytes} ->
        chunk = binary_part(bytes, offset, min(count, byte_size(bytes) - offset))
        {:ok, :binary.copy(chunk)}

      error ->
        error
    end
  end

  def working_file(config, :upload_target, [path]) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      with {:ok, _} <- fetch_directory(state, Path.dirname(path)) do
        case fetch_entry(state, path) do
          {:ok, {children, _}} when is_map(children) -> {:error, :eisdir}
          _ -> :ok
        end
      end
    end)
  end

  def working_file(config, :list, [path]), do: working_file(config, :list, [path, :infinity])

  def working_file(config, :list, [path, limit]) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case fetch_directory(state, path) do
        {:ok, {children, _}} when limit == :infinity or map_size(children) <= limit ->
          {:ok, list_entries(children)}

        {:ok, _} ->
          {:error, :discovery_entry_limit}

        error ->
          error
      end
    end)
  end

  def working_file(config, operation, args)
      when operation in [:write, :write_many, :mkdir, :delete, :move] do
    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case edit_working_tree(state, operation, args) do
        {:ok, next} -> {:ok, next}
        {:error, _} = error -> {error, state}
      end
    end)
  end

  defp read_working_file(state, path) do
    case fetch_entry(state, path) do
      {:ok, {bytes, _}} when is_binary(bytes) -> {:ok, bytes}
      {:ok, _} -> {:error, :eisdir}
      error -> error
    end
  end

  defp edit_working_tree(state, :write, [path, bytes, check]) do
    with :ok <- check.(read_working_file(state, path)),
         do: {:ok, put_file(state, path, bytes, [])}
  end

  defp edit_working_tree(state, :write, [path, bytes]) do
    case fetch_entry(state, path) do
      {:ok, {children, _}} when is_map(children) -> {:error, :eisdir}
      {:error, :not_directory} = error -> error
      _ -> {:ok, put_file(state, path, bytes, [])}
    end
  end

  defp edit_working_tree(state, :write_many, [files]) do
    Enum.reduce_while(files, {:ok, state}, fn {path, bytes}, {:ok, tree} ->
      case edit_working_tree(tree, :write, [path, bytes]) do
        {:ok, _} = result -> {:cont, result}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp edit_working_tree(state, :mkdir, [path]) do
    case fetch_directory(state, path) do
      {:ok, _} ->
        {:ok, state}

      {:error, :not_found} ->
        directory = {%{}, %{visibility: :private, mtime: System.system_time(:second)}}
        {:ok, put_in(state, accessor(path, directory), directory)}

      error ->
        error
    end
  end

  defp edit_working_tree(state, :delete, [path]) do
    case fetch_entry(state, path) do
      {:ok, {children, _}} when is_map(children) and map_size(children) > 0 ->
        {:error, :directory_not_empty}

      {:ok, _} ->
        {:ok, remove_working_entry(state, path)}

      error ->
        error
    end
  end

  defp edit_working_tree(state, :move, [source, destination]) do
    with {:ok, entry} <- fetch_entry(state, source),
         {:ok, _} <- fetch_directory(state, Path.dirname(destination)),
         {:error, :not_found} <- fetch_entry(state, destination) do
      next = remove_working_entry(state, source)
      {:ok, put_in(next, accessor(destination), entry)}
    else
      {:ok, _} -> {:error, :already_exists}
      error -> error
    end
  end

  defp remove_working_entry({files, directories, versions}, path) do
    [_state_accessor | entry_accessors] = accessor(path)
    {_, files} = pop_in(files, entry_accessors)
    {files, directories, versions}
  end

  defp fetch_directory(state, path) do
    case fetch_entry(state, path) do
      {:ok, {children, _}} = result when is_map(children) -> result
      {:ok, _} -> {:error, :not_directory}
      error -> error
    end
  end

  defp fetch_entry({files, _, _}, path) do
    root = Map.get(files, "/", {%{}, %{}})
    lookup(root, if(path == ".", do: [], else: String.split(path, "/")))
  end

  defp lookup(entry, []), do: {:ok, entry}

  defp lookup({children, _}, [part | rest]) when is_map(children) do
    case Map.fetch(children, part) do
      {:ok, entry} -> lookup(entry, rest)
      :error -> {:error, :not_found}
    end
  end

  defp lookup(_, _), do: {:error, :not_directory}

  # Versioning implementation

  @impl Jido.VFS.Adapter
  def write_version(%Config{} = config, path, contents, opts) do
    visibility = Keyword.get(opts, :visibility, :private)
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn {files, dirs, versions} ->
      version_id = generate_version_id()
      timestamp = System.system_time(:second)

      # Store the version
      version_key = {path, version_id}

      version_data =
        {IO.iodata_to_binary(contents), %{visibility: visibility, timestamp: timestamp, mtime: timestamp}}

      new_versions = Map.put(versions, version_key, version_data)

      # Store version metadata for the path
      path_versions = Map.get(versions, path, [])
      version_info = %{version_id: version_id, timestamp: timestamp}
      final_versions = Map.put(new_versions, path, [version_info | path_versions])

      # Update the current file
      file = {IO.iodata_to_binary(contents), %{visibility: visibility, mtime: timestamp}}
      directory = {%{}, %{visibility: directory_visibility, mtime: timestamp}}

      # Use the same logic as the regular write function 
      updated_files =
        try do
          put_in(files, accessor(path, directory), file)
        rescue
          # If the path doesn't exist, start with an empty file structure
          _ ->
            put_in({%{}, %{}}, accessor(path, directory), file) |> elem(0)
        end

      {{:ok, version_id}, {updated_files, dirs, final_versions}}
    end)
  end

  @impl Jido.VFS.Adapter
  def read_version(%Config{} = config, path, version_id) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn {_files, _dirs, versions} ->
      case Map.get(versions, {path, version_id}) do
        {binary, _meta} when is_binary(binary) -> {:ok, binary}
        _ -> {:error, Errors.FileNotFound.exception(file_path: "#{path}@#{version_id}")}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def list_versions(%Config{} = config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn {_files, _dirs, versions} ->
      case Map.get(versions, path) do
        nil -> {:ok, []}
        version_list -> {:ok, Enum.reverse(version_list)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def delete_version(%Config{} = config, path, version_id) do
    state_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn {files, dirs, versions} ->
      # Remove the version data
      versions = Map.delete(versions, {path, version_id})

      # Remove from version list for the path
      path_versions = Map.get(versions, path, [])
      updated_versions = Enum.reject(path_versions, &(&1.version_id == version_id))
      versions = Map.put(versions, path, updated_versions)

      {files, dirs, versions}
    end)

    :ok
  end

  @impl Jido.VFS.Adapter
  def get_latest_version(%Config{} = config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn {_files, _dirs, versions} ->
      case Map.get(versions, path) do
        [latest | _] -> {:ok, latest.version_id}
        [] -> {:error, Errors.FileNotFound.exception(file_path: path)}
        nil -> {:error, Errors.FileNotFound.exception(file_path: path)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def restore_version(%Config{} = config, path, version_id) do
    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn {files, dirs, versions} ->
      case Map.get(versions, {path, version_id}) do
        {binary, meta} when is_binary(binary) ->
          # Restore the file to current
          file =
            {binary, %{visibility: meta.visibility, mtime: Map.get(meta, :timestamp, System.system_time(:second))}}

          directory = {%{}, %{visibility: :private}}

          updated_files =
            try do
              put_in(files, accessor(path, directory), file)
            rescue
              _ ->
                put_in({%{}, %{}}, accessor(path, directory), file) |> elem(0)
            end

          {:ok, {updated_files, dirs, versions}}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: "#{path}@#{version_id}")}, {files, dirs, versions}}
      end
    end)
  end

  # Extended filesystem operations

  @impl Jido.VFS.Adapter
  def stat(%Config{} = config, path) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {binary, meta} when is_binary(binary) ->
          {:ok,
           %Jido.VFS.Stat.File{
             name: Path.basename(path),
             size: byte_size(binary),
             mtime: Map.get(meta, :mtime, 0),
             visibility: meta.visibility
           }}

        {%{}, meta} ->
          {:ok,
           %Jido.VFS.Stat.Dir{
             name: Path.basename(path),
             size: 0,
             mtime: Map.get(meta, :mtime, 0),
             visibility: meta.visibility
           }}

        _ ->
          {:error, Errors.FileNotFound.exception(file_path: path)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def access(%Config{} = config, path, modes) do
    state_get(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {_, meta} ->
          # InMemory adapter supports all access modes
          # Could check visibility for :read/:write permissions if needed
          case {Enum.member?(modes, :read), Enum.member?(modes, :write), meta.visibility} do
            # read access always allowed
            {true, false, _} -> :ok
            # write access always allowed  
            {false, true, _} -> :ok
            # read-write access always allowed
            {true, true, _} -> :ok
            _ -> :ok
          end

        _ ->
          {:error, Errors.FileNotFound.exception(file_path: path)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def append(%Config{} = config, path, contents, opts) do
    visibility = Keyword.get(opts, :visibility, :private)
    directory_visibility = Keyword.get(opts, :directory_visibility, :private)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {existing_binary, meta} when is_binary(existing_binary) ->
          # File exists, append to it
          new_contents = existing_binary <> IO.iodata_to_binary(contents)
          file = {new_contents, Map.merge(meta, %{visibility: visibility, mtime: System.system_time(:second)})}
          {:ok, put_in(state, accessor(path), file)}

        _ ->
          # File doesn't exist, create it with the contents
          file =
            {IO.iodata_to_binary(contents), %{visibility: visibility, mtime: System.system_time(:second)}}

          directory = {%{}, %{visibility: directory_visibility}}
          {:ok, put_in(state, accessor(path, directory), file)}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def truncate(%Config{} = config, path, new_size) do
    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {binary, meta} when is_binary(binary) ->
          current_size = byte_size(binary)

          new_binary =
            cond do
              new_size >= current_size ->
                # Pad with zeros if growing
                binary <> :binary.copy(<<0>>, new_size - current_size)

              new_size == 0 ->
                # Truncate to empty
                ""

              true ->
                # Truncate to smaller size
                binary_part(binary, 0, new_size)
            end

          updated_meta = Map.put(meta, :mtime, System.system_time(:second))
          file = {new_binary, updated_meta}
          {:ok, put_in(state, accessor(path), file)}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: path)}, state}
      end
    end)
  end

  @impl Jido.VFS.Adapter
  def utime(%Config{} = config, path, mtime) do
    mtime_seconds = DateTime.to_unix(mtime, :second)

    state_get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn state ->
      case get_in(state, accessor(path)) do
        {contents, meta} ->
          updated_meta = Map.put(meta, :mtime, mtime_seconds)
          updated_entry = {contents, updated_meta}
          {:ok, put_in(state, accessor(path), updated_entry)}

        _ ->
          {{:error, Errors.FileNotFound.exception(file_path: path)}, state}
      end
    end)
  end

  defp generate_version_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  @doc "Captures one atomic memory state."
  @impl Jido.VFS.Adapter
  @spec capture(Config.t()) :: {:ok, Jido.VFS.Capture.t(), reference()}
  def capture(config) do
    Agent.get(Jido.VFS.Registry.via(__MODULE__, config.name), fn {data, token} ->
      {tree, _meta} = Map.get(elem(data, 0), "/", {%{}, %{visibility: :private}})
      {:ok, Jido.VFS.Capture.from_tree(tree), token}
    end)
  end

  @doc "Applies an edit batch only at the expected state."
  @impl Jido.VFS.Adapter
  @spec apply_edits(Config.t(), reference(), list()) :: {:ok, reference()} | {:error, term()}
  def apply_edits(config, expected, edits),
    do: change_tree(config, expected, fn capture -> Jido.VFS.Capture.edit(capture, edits) end)

  @doc "Replaces working content without changing stored revision history."
  @impl Jido.VFS.Adapter
  @spec replace(Config.t(), reference(), Jido.VFS.Capture.t()) :: {:ok, reference()} | {:error, term()}
  def replace(config, expected, capture), do: change_tree(config, expected, fn _ -> capture end)

  defp change_tree(config, expected, fun) do
    Agent.get_and_update(Jido.VFS.Registry.via(__MODULE__, config.name), fn {data, token} = state ->
      if expected == token do
        try do
          {tree, meta} = Map.get(elem(data, 0), "/", {%{}, %{visibility: :private}})
          next_tree = tree |> Jido.VFS.Capture.from_tree() |> fun.() |> Jido.VFS.Capture.to_tree()
          next_token = make_ref()
          {{:ok, next_token}, {put_elem(data, 0, %{"/" => {next_tree, meta}}), next_token}}
        rescue
          error ->
            {{:error, Errors.AdapterError.exception(adapter: __MODULE__, reason: Exception.message(error))}, state}
        end
      else
        {{:error, Errors.StaleState.exception([])}, state}
      end
    end)
  end

  defp state_get(server, fun), do: Agent.get(server, fn {data, _token} -> fun.(data) end)

  defp state_update(server, fun) do
    case state_get_and_update(server, fn data -> {:ok, fun.(data)} end) do
      :ok -> :ok
      error -> error
    end
  end

  defp state_get_and_update(server, fun) do
    Agent.get_and_update(server, fn {data, token} = state ->
      try do
        case fun.(data) do
          {{:error, _} = error, _next} -> {error, state}
          {reply, next} -> {reply, {next, make_ref()}}
        end
      rescue
        error ->
          {{:error, Errors.AdapterError.exception(adapter: __MODULE__, reason: Exception.message(error))},
           {data, token}}
      end
    end)
  end
end
