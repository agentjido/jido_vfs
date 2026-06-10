defmodule Jido.VFS do
  @external_resource "README.md"
  @moduledoc @external_resource
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  alias Jido.VFS.Errors

  @type adapter :: module()
  @type filesystem :: {module(), Jido.VFS.Adapter.config()}
  @type operation ::
          :write
          | :write_stream
          | :read
          | :read_stream
          | :delete
          | :move
          | :copy
          | :copy_between
          | :file_exists
          | :list_contents
          | :create_directory
          | :delete_directory
          | :clear
          | :set_visibility
          | :visibility
          | :stat
          | :access
          | :append
          | :truncate
          | :utime
          | :commit
          | :revisions
          | :read_revision
          | :rollback

  @type copy_between_strategy :: :native | :stream | :tempfile

  defp convert_path_error({:path, :traversal}, path),
    do: Errors.PathTraversal.exception(attempted_path: path)

  defp convert_path_error({:path, :absolute}, path),
    do: Errors.AbsolutePath.exception(absolute_path: path)

  defp convert_path_error(:enotdir, path), do: Errors.NotDirectory.exception(not_dir_path: path)

  @doc """
  Safely configure an adapter and normalize configure-time failures into typed errors.
  """
  @spec safe_configure(adapter(), keyword()) :: {:ok, filesystem()} | {:error, term()}
  def safe_configure(adapter, opts \\ [])

  def safe_configure(adapter, opts) when is_atom(adapter) and is_list(opts) do
    if function_exported?(adapter, :configure, 1) do
      case normalize_adapter_call(fn -> adapter.configure(opts) end) do
        {configured_adapter, _config} = filesystem
        when is_atom(configured_adapter) and configured_adapter not in [:ok, :error] ->
          {:ok, filesystem}

        {:error, %Errors.Unknown.Unknown{error: reason}} ->
          {:error, Errors.AdapterError.exception(adapter: adapter, reason: reason)}

        {:error, reason} ->
          if jido_vfs_error?(reason) do
            {:error, reason}
          else
            {:error, Errors.AdapterError.exception(adapter: adapter, reason: reason)}
          end

        other ->
          {:error,
           Errors.AdapterError.exception(
             adapter: adapter,
             reason: %{operation: :configure, reason: {:invalid_config_result, other}}
           )}
      end
    else
      {:error, Errors.UnsupportedOperation.exception(operation: :configure, adapter: adapter)}
    end
  end

  def safe_configure(adapter, _opts) do
    {:error,
     Errors.AdapterError.exception(
       adapter: adapter,
       reason: %{operation: :configure, reason: :invalid_adapter_or_options}
     )}
  end

  @doc """
  Configure an adapter, raising when configuration fails.
  """
  @spec configure!(adapter(), keyword()) :: filesystem()
  def configure!(adapter, opts \\ []) do
    case safe_configure(adapter, opts) do
      {:ok, filesystem} -> filesystem
      {:error, error} -> raise error
    end
  end

  @doc """
  Returns whether a filesystem supports a specific operation.
  """
  @spec supports?(filesystem, operation()) :: boolean()
  def supports?({adapter, _config}, operation) do
    supports_adapter?(adapter, operation)
  end

  defp supports_adapter?(adapter, operation) when is_atom(adapter) and is_atom(operation) do
    unsupported = adapter_unsupported_operations(adapter)

    if operation in unsupported do
      false
    else
      case operation do
        :copy_between -> function_exported?(adapter, :copy, 5)
        :write -> function_exported?(adapter, :write, 4)
        :write_stream -> function_exported?(adapter, :write_stream, 3)
        :read -> function_exported?(adapter, :read, 2)
        :read_stream -> function_exported?(adapter, :read_stream, 3)
        :delete -> function_exported?(adapter, :delete, 2)
        :move -> function_exported?(adapter, :move, 4)
        :copy -> function_exported?(adapter, :copy, 4)
        :file_exists -> function_exported?(adapter, :file_exists, 2)
        :list_contents -> function_exported?(adapter, :list_contents, 2)
        :create_directory -> function_exported?(adapter, :create_directory, 3)
        :delete_directory -> function_exported?(adapter, :delete_directory, 3)
        :clear -> function_exported?(adapter, :clear, 1)
        :set_visibility -> function_exported?(adapter, :set_visibility, 3)
        :visibility -> function_exported?(adapter, :visibility, 2)
        :stat -> function_exported?(adapter, :stat, 2)
        :access -> function_exported?(adapter, :access, 3)
        :append -> function_exported?(adapter, :append, 4)
        :truncate -> function_exported?(adapter, :truncate, 3)
        :utime -> function_exported?(adapter, :utime, 3)
        :commit -> supports_versioning_operation?(adapter, :commit, 3)
        :revisions -> supports_versioning_operation?(adapter, :revisions, 3)
        :read_revision -> supports_versioning_operation?(adapter, :read_revision, 4)
        :rollback -> supports_versioning_operation?(adapter, :rollback, 3)
      end
    end
  end

  defp supports_adapter?(_adapter, _operation), do: false

  defp adapter_unsupported_operations(adapter) do
    if function_exported?(adapter, :unsupported_operations, 0) do
      case adapter.unsupported_operations() do
        operations when is_list(operations) -> operations
        _ -> []
      end
    else
      []
    end
  rescue
    _ -> []
  end

  defp supports_versioning_operation?(adapter, operation, arity) do
    with versioning_module when not is_nil(versioning_module) <- get_versioning_module(adapter),
         {:module, _module} <- Code.ensure_loaded(versioning_module) do
      function_exported?(versioning_module, operation, arity)
    else
      _ -> false
    end
  end

  defp unsupported(adapter, operation) do
    {:error, Errors.UnsupportedOperation.exception(operation: operation, adapter: adapter)}
  end

  defp normalize_adapter_result(result) do
    case result do
      {:error, %Errors.UnsupportedOperation{}} = error ->
        error

      {:error, :unsupported} ->
        {:error, Errors.UnsupportedOperation.exception(operation: :unknown, adapter: :unknown)}

      {:error, reason} ->
        {:error, Errors.to_error(reason)}

      other ->
        other
    end
  end

  defp normalize_adapter_call(fun) when is_function(fun, 0) do
    fun.() |> normalize_adapter_result()
  rescue
    e -> {:error, Errors.to_error(e)}
  end

  @doc """
  Write to a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      LocalFileSystem.write("test.txt", "Hello World")

  """
  @spec write(filesystem, Path.t(), iodata(), keyword()) :: :ok | {:error, term}
  def write({adapter, config}, path, contents, opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.write(config, normalized_path, contents, opts) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Returns a `Stream` for writing to the given `path`.

  ## Options

  The following stream options apply to all adapters:

    * `:chunk_size` - When reading, the amount to read,
      usually expressed as a number of bytes.

  ## Examples

  > Note: The shape of the returned stream will
  > necessarily depend on the adapter in use. In the
  > following examples the [`Local`](`Jido.VFS.Adapter.Local`)
  > adapter is invoked, which returns a `File.Stream`.

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, %File.Stream{}} = Jido.VFS.write_stream(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      {:ok, %File.Stream{}} = LocalFileSystem.write_stream("test.txt")

  """
  @spec write_stream(filesystem, Path.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, term}
  def write_stream({adapter, config}, path, opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.write_stream(config, normalized_path, opts) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Read from a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      {:ok, "Hello World"} = LocalFileSystem.read("test.txt")

  """
  @spec read(filesystem, Path.t(), keyword()) :: {:ok, binary} | {:error, term}
  def read({adapter, config}, path, _opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.read(config, normalized_path) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Returns a `Stream` for reading the given `path`.

  ## Options

  The following stream options apply to all adapters:

    * `:chunk_size` - When reading, the amount to read,
      usually expressed as a number of bytes.

  ## Examples

  > Note: The shape of the returned stream will
  > necessarily depend on the adapter in use. In the
  > following examples the [`Local`](`Jido.VFS.Adapter.Local`)
  > adapter is invoked, which returns a `File.Stream`.

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, %File.Stream{}} = Jido.VFS.read_stream(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      {:ok, %File.Stream{}} = LocalFileSystem.read_stream("test.txt")

  """
  @spec read_stream(filesystem, Path.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, term}
  def read_stream({adapter, config}, path, opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.read_stream(config, normalized_path, opts) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Delete a file from a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.delete(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.delete("test.txt")

  """
  @spec delete(filesystem, Path.t(), keyword()) :: :ok | {:error, term}
  def delete({adapter, config}, path, _opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.delete(config, normalized_path) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Move a file from source to destination on a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.move(filesystem, "test.txt", "other-test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.move("test.txt", "other-test.txt")

  """
  @spec move(filesystem, Path.t(), Path.t(), keyword()) :: :ok | {:error, term}
  def move({adapter, config}, source, destination, opts \\ []) do
    with {:ok, normalized_source} <- Jido.VFS.RelativePath.normalize(source) do
      with {:ok, normalized_destination} <- Jido.VFS.RelativePath.normalize(destination) do
        normalize_adapter_call(fn ->
          adapter.move(config, normalized_source, normalized_destination, opts)
        end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, destination)}
      end
    else
      {:error, reason} -> {:error, convert_path_error(reason, source)}
    end
  end

  @doc """
  Copy a file from source to destination on a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.copy(filesystem, "test.txt", "other-test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.copy("test.txt", "other-test.txt")

  """
  @spec copy(filesystem, Path.t(), Path.t(), keyword()) :: :ok | {:error, term}
  def copy({adapter, config}, source, destination, opts \\ []) do
    with {:ok, normalized_source} <- Jido.VFS.RelativePath.normalize(source) do
      with {:ok, normalized_destination} <- Jido.VFS.RelativePath.normalize(destination) do
        normalize_adapter_call(fn ->
          adapter.copy(config, normalized_source, normalized_destination, opts)
        end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, destination)}
      end
    else
      {:error, reason} -> {:error, convert_path_error(reason, source)}
    end
  end

  @doc """
  Copy a file from source to destination on a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.copy(filesystem, "test.txt", "other-test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.copy("test.txt", "other-test.txt")

  """
  @spec file_exists(filesystem, Path.t(), keyword()) :: {:ok, :exists | :missing} | {:error, term}
  def file_exists({adapter, config}, path, _opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.file_exists(config, normalized_path) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  List the contents of a folder on a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, contents} = Jido.VFS.list_contents(filesystem, ".")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      {:ok, contents} = LocalFileSystem.list_contents(".")

  """
  @spec list_contents(filesystem, Path.t(), keyword()) ::
          {:ok, [%Jido.VFS.Stat.Dir{} | %Jido.VFS.Stat.File{}]} | {:error, term}
  def list_contents({adapter, config}, path, _opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.list_contents(config, normalized_path) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Create a directory

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.create_directory(filesystem, "test/")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      LocalFileSystem.create_directory("test/")

  """
  @spec create_directory(filesystem, Path.t(), keyword()) :: :ok | {:error, term}
  def create_directory({adapter, config}, path, opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path),
         {:ok, normalized_path} <- Jido.VFS.RelativePath.assert_directory(normalized_path) do
      normalize_adapter_call(fn -> adapter.create_directory(config, normalized_path, opts) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Delete a directory.

  ## Options

    * `:recursive` - Recursively delete contents. Defaults to `false`.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.delete_directory(filesystem, "test/")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      LocalFileSystem.delete_directory("test/")

  """
  @spec delete_directory(filesystem, Path.t(), keyword()) :: :ok | {:error, term}
  def delete_directory({adapter, config}, path, opts \\ []) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path),
         {:ok, normalized_path} <- Jido.VFS.RelativePath.assert_directory(normalized_path) do
      normalize_adapter_call(fn -> adapter.delete_directory(config, normalized_path, opts) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Clear the filesystem.

  This is always recursive.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.clear(filesystem)

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      LocalFileSystem.clear()

  """
  @spec clear(filesystem, keyword()) :: :ok | {:error, term}
  def clear({adapter, config}, _opts \\ []) do
    normalize_adapter_call(fn -> adapter.clear(config) end)
  end

  @spec set_visibility(filesystem, Path.t(), Jido.VFS.Visibility.t()) :: :ok | {:error, term}
  def set_visibility({adapter, config}, path, visibility) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.set_visibility(config, normalized_path, visibility) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @spec visibility(filesystem, Path.t()) :: {:ok, Jido.VFS.Visibility.t()} | {:error, term}
  def visibility({adapter, config}, path) do
    with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> adapter.visibility(config, normalized_path) end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
    end
  end

  @doc """
  Get file or directory metadata (stat information)

  Returns detailed metadata about a file or directory including size, modification time, and visibility.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, %Jido.VFS.Stat.File{}} = Jido.VFS.stat(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      {:ok, %Jido.VFS.Stat.File{}} = LocalFileSystem.stat("test.txt")

  """
  @spec stat(filesystem, Path.t()) ::
          {:ok, %Jido.VFS.Stat.File{} | %Jido.VFS.Stat.Dir{}} | {:error, term}
  def stat({adapter, config}, path) do
    if supports?({adapter, config}, :stat) do
      with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
        normalize_adapter_call(fn -> adapter.stat(config, normalized_path) end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, path)}
      end
    else
      unsupported(adapter, :stat)
    end
  end

  @doc """
  Check file access permissions

  Checks whether the given file or directory can be accessed with the specified modes.

  ## Modes

    * `:read` - Check read access
    * `:write` - Check write access

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.access(filesystem, "test.txt", [:read, :write])

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.access("test.txt", [:read])

  """
  @spec access(filesystem, Path.t(), [:read | :write]) :: :ok | {:error, term}
  def access({adapter, config}, path, modes) do
    if supports?({adapter, config}, :access) do
      with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
        normalize_adapter_call(fn -> adapter.access(config, normalized_path, modes) end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, path)}
      end
    else
      unsupported(adapter, :access)
    end
  end

  @doc """
  Append content to a file

  If the file exists, the content is appended to the end. If it doesn't exist,
  a new file is created with the given content.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.append(filesystem, "test.txt", "Additional content")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.append("test.txt", "More data")

  """
  @spec append(filesystem, Path.t(), iodata(), keyword()) :: :ok | {:error, term}
  def append({adapter, config}, path, contents, opts \\ []) do
    if supports?({adapter, config}, :append) do
      with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
        normalize_adapter_call(fn -> adapter.append(config, normalized_path, contents, opts) end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, path)}
      end
    else
      unsupported(adapter, :append)
    end
  end

  @doc """
  Truncate a file to a specific size

  Resizes the file to the specified number of bytes. If the new size is larger than
  the current size, the file is padded with null bytes. If smaller, the file is truncated.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.truncate(filesystem, "test.txt", 100)

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.truncate("test.txt", 0)  # Empty the file

  """
  @spec truncate(filesystem, Path.t(), non_neg_integer()) :: :ok | {:error, term}
  def truncate({adapter, config}, path, new_size) do
    if supports?({adapter, config}, :truncate) do
      with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
        normalize_adapter_call(fn -> adapter.truncate(config, normalized_path, new_size) end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, path)}
      end
    else
      unsupported(adapter, :truncate)
    end
  end

  @doc """
  Update file modification time

  Changes the modification time of a file or directory.

  ## Examples

  ### Direct filesystem

      filesystem = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Jido.VFS.utime(filesystem, "test.txt", DateTime.utc_now())

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      :ok = LocalFileSystem.utime("test.txt", ~U[2023-01-01 00:00:00Z])

  """
  @spec utime(filesystem, Path.t(), DateTime.t()) :: :ok | {:error, term}
  def utime({adapter, config}, path, mtime) do
    if supports?({adapter, config}, :utime) do
      with {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
        normalize_adapter_call(fn -> adapter.utime(config, normalized_path, mtime) end)
      else
        {:error, reason} -> {:error, convert_path_error(reason, path)}
      end
    else
      unsupported(adapter, :utime)
    end
  end

  @doc """
  Copy a file from one filesystem to the other

  Copy behavior is controlled by `:copy_between_strategy`:

  - `:native` - use adapter-native cross-filesystem copy only (`copy/5`).
  - `:stream` - stream chunks from source to destination without local temp files.
  - `:tempfile` - spool through a local temp file.

  The default strategy is `:tempfile`.

  ## Options

  - `:copy_between_strategy` - `:native | :stream | :tempfile` (default: `:tempfile`)
  - `:copy_between_temp_dir` - temp directory for `:tempfile` strategy (default: `System.tmp_dir!/0`)
  - `:chunk_size` - chunk size for streaming strategies (default: `64 * 1024`)
  - all other options are forwarded to adapter `read`/`read_stream`/`write`/`write_stream`/`append` calls.

  ## Examples

  ### Direct filesystem

      filesystem_source = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage")
      filesystem_destination = Jido.VFS.Adapter.Local.configure(prefix: "/home/user/storage2")
      :ok = Jido.VFS.copy_between_filesystem({filesystem_source, "test.txt"}, {filesystem_destination, "copy.txt"})

  ### Module-based filesystem

      defmodule LocalSourceFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage"
      end

      defmodule LocalDestinationFileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.Local,
          prefix: "/home/user/storage2"
      end

      :ok = Jido.VFS.copy_between_filesystem(
        {LocalSourceFileSystem.__filesystem__(), "test.txt"},
        {LocalDestinationFileSystem.__filesystem__(), "copy.txt"}
      )

  """
  @spec copy_between_filesystem(
          source :: {filesystem, Path.t()},
          destination :: {filesystem, Path.t()},
          keyword()
        ) :: :ok | {:error, term}
  def copy_between_filesystem(source, destination, opts \\ [])

  # Same adapter, same config -> just do a plain copy
  def copy_between_filesystem({filesystem, source}, {filesystem, destination}, opts) do
    with {:ok, _strategy} <- copy_between_strategy_from_opts(opts),
         {:ok, _chunk_size} <- copy_between_chunk_size_from_opts(opts) do
      copy(filesystem, source, destination, copy_between_runtime_opts(opts))
    end
  end

  # Same adapter
  def copy_between_filesystem(
        {{adapter, config_source}, path_source},
        {{adapter, config_destination}, path_destination},
        opts
      ) do
    with {:ok, normalized_source, normalized_destination} <-
           normalize_copy_paths(path_source, path_destination),
         {:ok, strategy} <- copy_between_strategy_from_opts(opts) do
      copy_between_with_strategy(
        {{adapter, config_source}, normalized_source},
        {{adapter, config_destination}, normalized_destination},
        strategy,
        opts
      )
    else
      {:error, {:source, reason}} -> {:error, convert_path_error(reason, path_source)}
      {:error, {:destination, reason}} -> {:error, convert_path_error(reason, path_destination)}
      {:error, reason} -> {:error, reason}
    end
  end

  # Different adapters
  def copy_between_filesystem({source_filesystem, source_path}, {destination_filesystem, destination_path}, opts) do
    with {:ok, normalized_source, normalized_destination} <-
           normalize_copy_paths(source_path, destination_path),
         {:ok, strategy} <- copy_between_strategy_from_opts(opts) do
      copy_between_with_strategy(
        {source_filesystem, normalized_source},
        {destination_filesystem, normalized_destination},
        strategy,
        opts
      )
    else
      {:error, {:source, reason}} -> {:error, convert_path_error(reason, source_path)}
      {:error, {:destination, reason}} -> {:error, convert_path_error(reason, destination_path)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp copy_between_with_strategy(
         {{adapter, config_source}, source_path},
         {{adapter, config_destination}, destination_path},
         :native,
         opts
       ) do
    if supports?({adapter, config_source}, :copy_between) do
      normalize_adapter_call(fn ->
        adapter.copy(
          config_source,
          source_path,
          config_destination,
          destination_path,
          copy_between_runtime_opts(opts)
        )
      end)
    else
      unsupported(adapter, :copy_between)
    end
  end

  defp copy_between_with_strategy(_source, _destination, :native, _opts) do
    unsupported(__MODULE__, :copy_between)
  end

  defp copy_between_with_strategy(source, destination, :stream, opts) do
    copy_via_stream(source, destination, opts)
  end

  defp copy_between_with_strategy(source, destination, :tempfile, opts) do
    copy_via_tempfile(source, destination, opts)
  end

  defp copy_between_runtime_opts(opts) do
    Keyword.drop(opts, [:copy_between_strategy, :copy_between_temp_dir, :chunk_size])
  end

  defp copy_between_stream_opts(opts, chunk_size) do
    opts
    |> copy_between_runtime_opts()
    |> Keyword.put(:chunk_size, chunk_size)
  end

  defp copy_between_strategy_from_opts(opts) do
    case Keyword.get(opts, :copy_between_strategy, :tempfile) do
      strategy when strategy in [:native, :stream, :tempfile] ->
        {:ok, strategy}

      strategy ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: %{
             operation: :copy_between_filesystem,
             reason: {:invalid_copy_between_strategy, strategy},
             allowed_strategies: [:native, :stream, :tempfile]
           }
         )}
    end
  end

  defp copy_via_stream(
         {source_filesystem, source_path},
         {destination_filesystem, destination_path},
         opts
       ) do
    adapter_opts = copy_between_runtime_opts(opts)

    with {:ok, chunk_size} <- copy_between_chunk_size_from_opts(opts),
         stream_opts = copy_between_stream_opts(opts, chunk_size),
         {:ok, chunks} <- source_copy_chunks(source_filesystem, source_path, stream_opts, chunk_size),
         :ok <-
           write_copy_chunks_to_destination(
             destination_filesystem,
             destination_path,
             chunks,
             stream_opts,
             adapter_opts,
             chunk_size
           ) do
      :ok
    end
  end

  defp source_copy_chunks(source_filesystem, source_path, opts, chunk_size) do
    if supports?(source_filesystem, :read_stream) do
      case Jido.VFS.read_stream(source_filesystem, source_path, opts) do
        {:ok, read_stream} -> {:ok, read_stream}
        {:error, reason} -> copy_side_error(:source, source_path, reason)
      end
    else
      case Jido.VFS.read(source_filesystem, source_path) do
        {:ok, contents} -> {:ok, chunk(contents, chunk_size)}
        {:error, reason} -> copy_side_error(:source, source_path, reason)
      end
    end
  end

  defp write_copy_chunks_to_destination(
         destination_filesystem,
         destination_path,
         chunks,
         stream_opts,
         adapter_opts,
         _chunk_size
       ) do
    cond do
      supports?(destination_filesystem, :write_stream) ->
        with {:ok, write_stream} <-
               open_destination_write_stream(destination_filesystem, destination_path, stream_opts, adapter_opts) do
          try do
            Enum.into(chunks, write_stream)
            :ok
          rescue
            error ->
              copy_side_error(:destination, destination_path, error)
          catch
            kind, reason ->
              copy_side_error(:destination, destination_path, %{kind: kind, reason: reason})
          end
        else
          {:error, reason} ->
            copy_side_error(:destination, destination_path, reason)
        end

      supports?(destination_filesystem, :append) ->
        with :ok <- Jido.VFS.write(destination_filesystem, destination_path, "", adapter_opts) do
          try do
            Enum.reduce_while(chunks, :ok, fn chunk, :ok ->
              case Jido.VFS.append(destination_filesystem, destination_path, chunk, adapter_opts) do
                :ok -> {:cont, :ok}
                {:error, reason} -> {:halt, copy_side_error(:destination, destination_path, reason)}
              end
            end)
          rescue
            error ->
              copy_side_error(:destination, destination_path, error)
          catch
            kind, reason ->
              copy_side_error(:destination, destination_path, %{kind: kind, reason: reason})
          end
        else
          {:error, reason} ->
            copy_side_error(:destination, destination_path, reason)
        end

      true ->
        try do
          contents =
            chunks
            |> Enum.reduce([], fn chunk, acc -> [acc, chunk] end)
            |> IO.iodata_to_binary()

          case Jido.VFS.write(destination_filesystem, destination_path, contents, adapter_opts) do
            :ok -> :ok
            {:error, reason} -> copy_side_error(:destination, destination_path, reason)
          end
        rescue
          error ->
            copy_side_error(:destination, destination_path, error)
        catch
          kind, reason ->
            copy_side_error(:destination, destination_path, %{kind: kind, reason: reason})
        end
    end
  end

  defp copy_via_tempfile(
         {source_filesystem, source_path},
         {destination_filesystem, destination_path},
         opts
       ) do
    temp_dir = Keyword.get(opts, :copy_between_temp_dir, System.tmp_dir!())
    adapter_opts = copy_between_runtime_opts(opts)

    with {:ok, chunk_size} <- copy_between_chunk_size_from_opts(opts),
         :ok <- ensure_copy_temp_dir(temp_dir) do
      stream_opts = copy_between_stream_opts(opts, chunk_size)

      temp_path =
        Path.join(
          temp_dir,
          "jido_vfs_copy_#{System.unique_integer([:positive, :monotonic])}"
        )

      try do
        with :ok <-
               copy_source_into_tempfile(source_filesystem, source_path, temp_path, stream_opts, chunk_size),
             :ok <-
               copy_tempfile_into_destination(
                 destination_filesystem,
                 destination_path,
                 temp_path,
                 adapter_opts,
                 stream_opts,
                 chunk_size
               ) do
          :ok
        end
      rescue
        e -> {:error, Errors.to_error(e)}
      catch
        kind, reason ->
          {:error,
           Errors.AdapterError.exception(
             adapter: __MODULE__,
             reason: %{operation: :copy_between_filesystem, kind: kind, reason: reason}
           )}
      after
        File.rm(temp_path)
      end
    end
  end

  defp ensure_copy_temp_dir(temp_dir) do
    case File.mkdir_p(temp_dir) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: %{operation: :copy_between_filesystem, reason: {:temp_dir_unavailable, temp_dir, reason}}
         )}
    end
  end

  defp copy_between_chunk_size_from_opts(opts) do
    case Keyword.get(opts, :chunk_size, 64 * 1024) do
      size when is_integer(size) and size > 0 ->
        {:ok, size}

      size ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: %{
             operation: :copy_between_filesystem,
             reason: {:invalid_chunk_size, size}
           }
         )}
    end
  end

  defp normalize_copy_paths(source_path, destination_path) do
    case Jido.VFS.RelativePath.normalize(source_path) do
      {:ok, normalized_source} ->
        case Jido.VFS.RelativePath.normalize(destination_path) do
          {:ok, normalized_destination} ->
            {:ok, normalized_source, normalized_destination}

          {:error, reason} ->
            {:error, {:destination, reason}}
        end

      {:error, reason} ->
        {:error, {:source, reason}}
    end
  end

  defp copy_source_into_tempfile(source_filesystem, source_path, temp_path, opts, chunk_size) do
    if supports?(source_filesystem, :read_stream) do
      with {:ok, read_stream} <-
             Jido.VFS.read_stream(source_filesystem, source_path, Keyword.put(opts, :chunk_size, chunk_size)),
           {:ok, file} <- File.open(temp_path, [:write, :binary]) do
        try do
          Enum.each(read_stream, &IO.binwrite(file, &1))
          :ok
        rescue
          error ->
            copy_side_error(:source, source_path, error)
        catch
          kind, reason ->
            copy_side_error(:source, source_path, %{kind: kind, reason: reason})
        after
          File.close(file)
        end
      else
        {:error, reason} ->
          copy_side_error(:source, source_path, reason)
      end
    else
      with {:ok, contents} <- Jido.VFS.read(source_filesystem, source_path),
           :ok <- File.write(temp_path, contents) do
        :ok
      else
        {:error, reason} ->
          copy_side_error(:source, source_path, reason)
      end
    end
  end

  defp copy_tempfile_into_destination(
         destination_filesystem,
         destination_path,
         temp_path,
         opts,
         stream_opts,
         chunk_size
       ) do
    if supports?(destination_filesystem, :write_stream) do
      with {:ok, write_stream} <-
             open_destination_write_stream(
               destination_filesystem,
               destination_path,
               stream_opts,
               opts
             ) do
        try do
          temp_path
          |> File.stream!(chunk_size, [])
          |> Enum.into(write_stream)

          :ok
        rescue
          error ->
            copy_side_error(:destination, destination_path, error)
        catch
          kind, reason ->
            copy_side_error(:destination, destination_path, %{kind: kind, reason: reason})
        end
      else
        {:error, reason} ->
          copy_side_error(:destination, destination_path, reason)
      end
    else
      if supports?(destination_filesystem, :append) do
        with :ok <- Jido.VFS.write(destination_filesystem, destination_path, "", opts),
             {:ok, file} <- File.open(temp_path, [:read, :binary]) do
          try do
            IO.binstream(file, chunk_size)
            |> Enum.reduce_while(:ok, fn chunk, :ok ->
              case Jido.VFS.append(destination_filesystem, destination_path, chunk, opts) do
                :ok ->
                  {:cont, :ok}

                {:error, reason} ->
                  {:halt, copy_side_error(:destination, destination_path, reason)}
              end
            end)
          rescue
            error ->
              copy_side_error(:destination, destination_path, error)
          catch
            kind, reason ->
              copy_side_error(:destination, destination_path, %{kind: kind, reason: reason})
          after
            File.close(file)
          end
        else
          {:error, reason} ->
            copy_side_error(:destination, destination_path, reason)
        end
      else
        with {:ok, contents} <- File.read(temp_path),
             :ok <- Jido.VFS.write(destination_filesystem, destination_path, contents, opts) do
          :ok
        else
          {:error, reason} ->
            copy_side_error(:destination, destination_path, reason)
        end
      end
    end
  end

  defp copy_side_error(side, path, reason) do
    reason =
      case reason do
        {:error, nested_reason} -> nested_reason
        other -> other
      end

    if jido_vfs_error?(reason) do
      {:error, reason}
    else
      {:error,
       Errors.AdapterError.exception(
         adapter: __MODULE__,
         reason: %{operation: :copy_between_filesystem, side: side, path: path, reason: reason}
       )}
    end
  end

  defp open_destination_write_stream(destination_filesystem, destination_path, stream_opts, adapter_opts) do
    with :ok <- ensure_destination_write_target(destination_filesystem, destination_path, adapter_opts),
         {:ok, write_stream} <- Jido.VFS.write_stream(destination_filesystem, destination_path, stream_opts) do
      {:ok, write_stream}
    end
  end

  defp ensure_destination_write_target(destination_filesystem, destination_path, opts) do
    case Jido.VFS.write(destination_filesystem, destination_path, "", opts) do
      :ok -> :ok
      {:error, %Errors.UnsupportedOperation{operation: :write}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp jido_vfs_error?(%{__struct__: module}) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.Jido.VFS.Errors.")
  end

  defp jido_vfs_error?(_), do: false

  @doc false
  # Also used by the InMemory adapter and therefore not private
  @spec chunk(binary(), pos_integer()) :: [binary()]
  def chunk(binary, size) when is_integer(size) and size > 0 do
    do_chunk(binary, size)
  end

  def chunk(_binary, size) do
    raise ArgumentError, "chunk size must be a positive integer, got: #{inspect(size)}"
  end

  defp do_chunk("", _size), do: []

  defp do_chunk(binary, size) when byte_size(binary) >= size do
    {chunk, rest} = :erlang.split_binary(binary, size)
    [chunk | do_chunk(rest, size)]
  end

  defp do_chunk(binary, _size), do: [binary]

  defp normalize_revision_result({:ok, revisions}) when is_list(revisions) do
    {:ok, Enum.map(revisions, &to_revision_struct/1)}
  end

  defp normalize_revision_result(other), do: other

  defp to_revision_struct(%Jido.VFS.Revision{} = revision), do: revision

  defp to_revision_struct(%{revision: revision} = revision_map) do
    %Jido.VFS.Revision{
      sha: to_string(revision),
      author_name: Map.get(revision_map, :author_name, "Unknown"),
      author_email: Map.get(revision_map, :author_email, "unknown@jido.vfs.local"),
      message: Map.get(revision_map, :message, ""),
      timestamp: normalize_revision_timestamp(Map.get(revision_map, :timestamp))
    }
  end

  defp to_revision_struct(%{sha: sha} = revision_map) do
    %Jido.VFS.Revision{
      sha: to_string(sha),
      author_name: Map.get(revision_map, :author_name, "Unknown"),
      author_email: Map.get(revision_map, :author_email, "unknown@jido.vfs.local"),
      message: Map.get(revision_map, :message, ""),
      timestamp: normalize_revision_timestamp(Map.get(revision_map, :timestamp))
    }
  end

  defp to_revision_struct(other) do
    %Jido.VFS.Revision{
      sha: inspect(other),
      author_name: "Unknown",
      author_email: "unknown@jido.vfs.local",
      message: "",
      timestamp: DateTime.utc_now()
    }
  end

  defp normalize_revision_timestamp(%DateTime{} = timestamp), do: timestamp

  defp normalize_revision_timestamp(timestamp) when is_integer(timestamp) do
    DateTime.from_unix!(timestamp)
  end

  defp normalize_revision_timestamp(_), do: DateTime.utc_now()

  @spec get_versioning_module(module()) :: module() | nil
  defp get_versioning_module(adapter) when is_atom(adapter) do
    if function_exported?(adapter, :versioning_module, 0) do
      case adapter.versioning_module() do
        module when is_atom(module) -> module
        _ -> nil
      end
    else
      nil
    end
  rescue
    _ -> nil
  end

  defp get_versioning_module(_), do: nil

  @doc """
  Commit changes to a version-controlled filesystem.

  Uses the polymorphic versioning interface to support any adapter that implements
  versioning functionality (Git, ETS, InMemory).

  ## Examples

      # Git adapter
      filesystem = Jido.VFS.Adapter.Git.configure(path: "/repo", mode: :manual)
      Jido.VFS.write(filesystem, "file.txt", "content")
      :ok = Jido.VFS.commit(filesystem, "Add new file")

      # ETS adapter (uses versioning wrapper)
      filesystem = Jido.VFS.Adapter.ETS.configure(name: :test_ets)
      :ok = Jido.VFS.commit(filesystem, "Snapshot")

  """
  @spec commit(filesystem, String.t() | nil, keyword()) :: :ok | {:error, term}
  def commit({adapter, config}, message \\ nil, opts \\ []) do
    with versioning_module when not is_nil(versioning_module) <- get_versioning_module(adapter),
         true <- supports?({adapter, config}, :commit) do
      normalize_adapter_call(fn -> versioning_module.commit(config, message, opts) end)
    else
      _ -> unsupported(adapter, :commit)
    end
  end

  @doc """
  List revisions/commits for a path in a version-controlled filesystem.

  Uses the polymorphic versioning interface to support any adapter that implements
  versioning functionality. Returns a list of revision maps with standardized format.

  ## Options

    * `:limit` - Maximum number of revisions to return
    * `:since` - Only revisions after this datetime
    * `:until` - Only revisions before this datetime
    * `:author` - Only revisions by this author

  ## Examples

      # Git adapter
      filesystem = Jido.VFS.Adapter.Git.configure(path: "/repo")
      {:ok, revisions} = Jido.VFS.revisions(filesystem, "file.txt", limit: 10)

      # ETS adapter
      filesystem = Jido.VFS.Adapter.ETS.configure(name: :test_ets)
      {:ok, revisions} = Jido.VFS.revisions(filesystem, "file.txt")

  """
  @spec revisions(filesystem, Path.t(), keyword()) ::
          {:ok, [Jido.VFS.Revision.t()]} | {:error, term}
  def revisions({adapter, config}, path \\ ".", opts \\ []) do
    with versioning_module when not is_nil(versioning_module) <- get_versioning_module(adapter),
         true <- supports?({adapter, config}, :revisions),
         {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn -> versioning_module.revisions(config, normalized_path, opts) end)
      |> normalize_revision_result()
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
      _ -> unsupported(adapter, :revisions)
    end
  end

  @doc """
  Read a file as it existed at a specific revision.

  Uses the polymorphic versioning interface to support any adapter that implements
  versioning functionality.

  ## Examples

      # Git adapter
      filesystem = Jido.VFS.Adapter.Git.configure(path: "/repo")
      {:ok, content} = Jido.VFS.read_revision(filesystem, "file.txt", "abc123")

      # ETS adapter
      filesystem = Jido.VFS.Adapter.ETS.configure(name: :test_ets)
      {:ok, content} = Jido.VFS.read_revision(filesystem, "file.txt", "version_id")

  """
  @spec read_revision(filesystem, Path.t(), String.t(), keyword()) ::
          {:ok, binary()} | {:error, term}
  def read_revision({adapter, config}, path, revision, opts \\ []) do
    with versioning_module when not is_nil(versioning_module) <- get_versioning_module(adapter),
         true <- supports?({adapter, config}, :read_revision),
         {:ok, normalized_path} <- Jido.VFS.RelativePath.normalize(path) do
      normalize_adapter_call(fn ->
        versioning_module.read_revision(config, normalized_path, revision, opts)
      end)
    else
      {:error, reason} -> {:error, convert_path_error(reason, path)}
      _ -> unsupported(adapter, :read_revision)
    end
  end

  @doc """
  Rollback the filesystem to a previous revision.

  Uses the polymorphic versioning interface to support any adapter that implements
  versioning functionality.

  ## Options

    * `:path` - Only rollback changes to a specific path (if supported)

  ## Examples

      # Git adapter - full rollback
      filesystem = Jido.VFS.Adapter.Git.configure(path: "/repo")
      :ok = Jido.VFS.rollback(filesystem, "abc123")

      # ETS adapter - single file rollback
      filesystem = Jido.VFS.Adapter.ETS.configure(name: :test_ets)
      :ok = Jido.VFS.rollback(filesystem, "version_id", path: "file.txt")

  """
  @spec rollback(filesystem, String.t(), keyword()) :: :ok | {:error, term}
  def rollback({adapter, config}, revision, opts \\ []) do
    with versioning_module when not is_nil(versioning_module) <- get_versioning_module(adapter),
         true <- supports?({adapter, config}, :rollback) do
      normalize_adapter_call(fn -> versioning_module.rollback(config, revision, opts) end)
    else
      _ -> unsupported(adapter, :rollback)
    end
  end
end
