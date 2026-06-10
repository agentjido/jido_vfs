defmodule Jido.VFS.Filesystem do
  @moduledoc """
  Behaviour and delegation helpers for a `Jido.VFS` filesystem module.
  """

  @doc """
  Writes contents to the given relative path.
  """
  @callback write(path :: Path.t(), contents :: iodata(), opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Opens a writable stream for the given relative path.
  """
  @callback write_stream(path :: Path.t(), opts :: keyword()) ::
              {:ok, Collectable.t()} | {:error, term}

  @doc """
  Reads the file at the given relative path.
  """
  @callback read(path :: Path.t(), opts :: keyword()) :: {:ok, binary} | {:error, term}

  @doc """
  Opens a readable stream for the file at the given relative path.
  """
  @callback read_stream(path :: Path.t(), opts :: keyword()) ::
              {:ok, Enumerable.t()} | {:error, term}

  @doc """
  Deletes the file at the given relative path.
  """
  @callback delete(path :: Path.t(), opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Moves a file from one relative path to another.
  """
  @callback move(source :: Path.t(), destination :: Path.t(), opts :: keyword()) ::
              :ok | {:error, term}

  @doc """
  Copies a file from one relative path to another.
  """
  @callback copy(source :: Path.t(), destination :: Path.t(), opts :: keyword()) ::
              :ok | {:error, term}

  @doc """
  Checks whether the given relative path exists.
  """
  @callback file_exists(path :: Path.t(), opts :: keyword()) ::
              {:ok, :exists | :missing} | {:error, term}

  @doc """
  Lists the directory entries for the given relative path.
  """
  @callback list_contents(path :: Path.t(), opts :: keyword()) ::
              {:ok, [%Jido.VFS.Stat.Dir{} | %Jido.VFS.Stat.File{}]} | {:error, term}

  @doc """
  Creates a directory at the given relative path.
  """
  @callback create_directory(path :: Path.t(), opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Deletes a directory at the given relative path.
  """
  @callback delete_directory(path :: Path.t(), opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Clears the configured filesystem.
  """
  @callback clear(opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Sets public/private visibility for the given relative path.
  """
  @callback set_visibility(path :: Path.t(), visibility :: Jido.VFS.Visibility.t()) ::
              :ok | {:error, term}

  @doc """
  Reads public/private visibility for the given relative path.
  """
  @callback visibility(path :: Path.t()) :: {:ok, Jido.VFS.Visibility.t()} | {:error, term}

  @doc """
  Reads file or directory metadata for the given relative path.
  """
  @callback stat(path :: Path.t()) ::
              {:ok, %Jido.VFS.Stat.File{} | %Jido.VFS.Stat.Dir{}} | {:error, term}

  @doc """
  Checks access for the given relative path.
  """
  @callback access(path :: Path.t(), modes :: [:read | :write]) :: :ok | {:error, term}

  @doc """
  Appends contents to the given relative path.
  """
  @callback append(path :: Path.t(), contents :: iodata(), opts :: keyword()) ::
              :ok | {:error, term}

  @doc """
  Truncates the file at the given relative path.
  """
  @callback truncate(path :: Path.t(), new_size :: non_neg_integer()) :: :ok | {:error, term}

  @doc """
  Updates the modification time for the given relative path.
  """
  @callback utime(path :: Path.t(), mtime :: DateTime.t()) :: :ok | {:error, term}

  @doc """
  Commits changes in version-aware filesystems.
  """
  @callback commit(message :: String.t() | nil, opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Lists revisions for a relative path in version-aware filesystems.
  """
  @callback revisions(path :: Path.t(), opts :: keyword()) ::
              {:ok, [Jido.VFS.Revision.t()]} | {:error, term}

  @doc """
  Reads a file as it existed at a specific revision.
  """
  @callback read_revision(path :: Path.t(), revision :: String.t(), opts :: keyword()) ::
              {:ok, binary()} | {:error, term}

  @doc """
  Rolls a version-aware filesystem back to a revision.
  """
  @callback rollback(revision :: String.t(), opts :: keyword()) :: :ok | {:error, term}

  @doc """
  Injects a filesystem wrapper module backed by a configured adapter.
  """
  @spec __using__(Macro.t()) :: Macro.t()
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Jido.VFS.Filesystem
      {adapter, opts} = Jido.VFS.Filesystem.parse_opts(__MODULE__, opts)
      @adapter adapter
      @opts opts
      @key {Jido.VFS.Filesystem, __MODULE__}

      @doc """
      Configures and caches the filesystem instance for the current module.
      """
      @spec init() :: Jido.VFS.filesystem()
      def init do
        opts = Jido.VFS.Filesystem.merge_app_env(@opts, __MODULE__)
        filesystem = Jido.VFS.configure!(@adapter, opts)

        :persistent_term.put(@key, filesystem)

        filesystem
      end

      @doc """
      Returns the cached filesystem instance, initializing it on first access.
      """
      @spec __filesystem__() :: Jido.VFS.filesystem()
      def __filesystem__ do
        case :persistent_term.get(@key, :__jido_vfs_missing__) do
          :__jido_vfs_missing__ -> init()
          filesystem -> filesystem
        end
      end

      if @adapter.starts_processes() do
        @doc """
        Returns a child specification for adapters that manage processes.
        """
        @spec child_spec(term()) :: Supervisor.child_spec()
        def child_spec(init_arg) do
          __filesystem__()
          |> Supervisor.child_spec(init_arg)
        end
      end

      @doc """
      Writes file contents through the configured filesystem.
      """
      @spec write(Path.t(), iodata(), keyword()) :: :ok | {:error, term()}
      @impl true
      def write(path, contents, opts \\ []),
        do: Jido.VFS.write(__filesystem__(), path, contents, opts)

      @doc """
      Opens a writable stream through the configured filesystem.
      """
      @spec write_stream(Path.t(), keyword()) :: {:ok, Collectable.t()} | {:error, term()}
      @impl true
      def write_stream(path, opts \\ []),
        do: Jido.VFS.write_stream(__filesystem__(), path, opts)

      @doc """
      Reads a file through the configured filesystem.
      """
      @spec read(Path.t(), keyword()) :: {:ok, binary()} | {:error, term()}
      @impl true
      def read(path, opts \\ []),
        do: Jido.VFS.read(__filesystem__(), path, opts)

      @doc """
      Opens a readable stream through the configured filesystem.
      """
      @spec read_stream(Path.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, term()}
      @impl true
      def read_stream(path, opts \\ []),
        do: Jido.VFS.read_stream(__filesystem__(), path, opts)

      @doc """
      Deletes a file through the configured filesystem.
      """
      @spec delete(Path.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def delete(path, opts \\ []),
        do: Jido.VFS.delete(__filesystem__(), path, opts)

      @doc """
      Moves a file or directory through the configured filesystem.
      """
      @spec move(Path.t(), Path.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def move(source, destination, opts \\ []),
        do: Jido.VFS.move(__filesystem__(), source, destination, opts)

      @doc """
      Copies a file through the configured filesystem.
      """
      @spec copy(Path.t(), Path.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def copy(source, destination, opts \\ []),
        do: Jido.VFS.copy(__filesystem__(), source, destination, opts)

      @doc """
      Checks whether a path exists in the configured filesystem.
      """
      @spec file_exists(Path.t(), keyword()) :: {:ok, :exists | :missing} | {:error, term()}
      @impl true
      def file_exists(path, opts \\ []),
        do: Jido.VFS.file_exists(__filesystem__(), path, opts)

      @doc """
      Lists directory contents through the configured filesystem.
      """
      @spec list_contents(Path.t(), keyword()) ::
              {:ok, [Jido.VFS.Stat.Dir.t() | Jido.VFS.Stat.File.t()]} | {:error, term()}
      @impl true
      def list_contents(path, opts \\ []),
        do: Jido.VFS.list_contents(__filesystem__(), path, opts)

      @doc """
      Creates a directory through the configured filesystem.
      """
      @spec create_directory(Path.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def create_directory(path, opts \\ []),
        do: Jido.VFS.create_directory(__filesystem__(), path, opts)

      @doc """
      Deletes a directory through the configured filesystem.
      """
      @spec delete_directory(Path.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def delete_directory(path, opts \\ []),
        do: Jido.VFS.delete_directory(__filesystem__(), path, opts)

      @doc """
      Clears the configured filesystem.
      """
      @spec clear(keyword()) :: :ok | {:error, term()}
      @impl true
      def clear(opts \\ []),
        do: Jido.VFS.clear(__filesystem__(), opts)

      @doc """
      Sets visibility through the configured filesystem.
      """
      @spec set_visibility(Path.t(), Jido.VFS.Visibility.t()) :: :ok | {:error, term()}
      @impl true
      def set_visibility(path, visibility),
        do: Jido.VFS.set_visibility(__filesystem__(), path, visibility)

      @doc """
      Reads visibility through the configured filesystem.
      """
      @spec visibility(Path.t()) :: {:ok, Jido.VFS.Visibility.t()} | {:error, term()}
      @impl true
      def visibility(path),
        do: Jido.VFS.visibility(__filesystem__(), path)

      @doc """
      Reads file or directory metadata through the configured filesystem.
      """
      @spec stat(Path.t()) ::
              {:ok, %Jido.VFS.Stat.File{} | %Jido.VFS.Stat.Dir{}} | {:error, term()}
      @impl true
      def stat(path),
        do: Jido.VFS.stat(__filesystem__(), path)

      @doc """
      Checks access through the configured filesystem.
      """
      @spec access(Path.t(), [:read | :write]) :: :ok | {:error, term()}
      @impl true
      def access(path, modes),
        do: Jido.VFS.access(__filesystem__(), path, modes)

      @doc """
      Appends file contents through the configured filesystem.
      """
      @spec append(Path.t(), iodata(), keyword()) :: :ok | {:error, term()}
      @impl true
      def append(path, contents, opts \\ []),
        do: Jido.VFS.append(__filesystem__(), path, contents, opts)

      @doc """
      Truncates a file through the configured filesystem.
      """
      @spec truncate(Path.t(), non_neg_integer()) :: :ok | {:error, term()}
      @impl true
      def truncate(path, new_size),
        do: Jido.VFS.truncate(__filesystem__(), path, new_size)

      @doc """
      Updates modification time through the configured filesystem.
      """
      @spec utime(Path.t(), DateTime.t()) :: :ok | {:error, term()}
      @impl true
      def utime(path, mtime),
        do: Jido.VFS.utime(__filesystem__(), path, mtime)

      @doc """
      Commits changes through the configured filesystem.
      """
      @spec commit(String.t() | nil, keyword()) :: :ok | {:error, term()}
      @impl true
      def commit(message \\ nil, opts \\ []),
        do: Jido.VFS.commit(__filesystem__(), message, opts)

      @doc """
      Lists revisions through the configured filesystem.
      """
      @spec revisions(Path.t(), keyword()) :: {:ok, [Jido.VFS.Revision.t()]} | {:error, term()}
      @impl true
      def revisions(path \\ ".", opts \\ []),
        do: Jido.VFS.revisions(__filesystem__(), path, opts)

      @doc """
      Reads a file at a revision through the configured filesystem.
      """
      @spec read_revision(Path.t(), String.t(), keyword()) :: {:ok, binary()} | {:error, term()}
      @impl true
      def read_revision(path, revision, opts \\ []),
        do: Jido.VFS.read_revision(__filesystem__(), path, revision, opts)

      @doc """
      Rolls back through the configured filesystem.
      """
      @spec rollback(String.t(), keyword()) :: :ok | {:error, term()}
      @impl true
      def rollback(revision, opts \\ []),
        do: Jido.VFS.rollback(__filesystem__(), revision, opts)
    end
  end

  @doc """
  Merges compile-time options with module-specific runtime configuration and extracts the adapter.
  """
  @spec parse_opts(module(), keyword()) :: {module(), keyword()}
  def parse_opts(module, opts) do
    opts
    |> merge_app_env(module)
    |> Keyword.put_new(:name, module)
    |> Keyword.pop!(:adapter)
  end

  @doc """
  Merges adapter options with application environment overrides when `:otp_app` is present.
  """
  @spec merge_app_env(keyword(), module()) :: keyword()
  def merge_app_env(opts, module) do
    case Keyword.fetch(opts, :otp_app) do
      {:ok, otp_app} ->
        config = Application.get_env(otp_app, module, [])
        Keyword.merge(opts, config)

      :error ->
        opts
    end
  end
end
