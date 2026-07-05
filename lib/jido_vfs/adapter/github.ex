defmodule Jido.VFS.Adapter.GitHub do
  @moduledoc """
  A GitHub virtual filesystem adapter for Jido.VFS.

  This adapter allows you to interact with GitHub repositories as if they were
  a local filesystem, using the GitHub API.

  ## Configuration

      config = Jido.VFS.Adapter.GitHub.configure(
        owner: "username",
        repo: "repository-name",
        ref: "main",  # branch or commit SHA
        auth: %{access_token: "github_pat_token"}
      )

  ## Application Configuration

  You can also configure GitHub credentials in your application config:

      # config/config.exs
      config :jido_vfs, :github,
        access_token: "github_pat_token",
        name: "Your Name",
        email: "your@email.com"

  When configured this way, the adapter will automatically use these credentials
  as defaults if not explicitly provided in the configure/1 call.

  ## Authentication

  Supports several authentication methods:

  - Personal Access Token: `%{access_token: "token"}`
  - Username/Password: `%{user: "username", password: "password"}`
  - No authentication (public repos only): `nil`

  ## Examples

      # Read a file
      {:ok, content} = Jido.VFS.read(config, "README.md")

      # Write a file (creates a commit)
      :ok = Jido.VFS.write(config, "new_file.txt", "content", 
                        message: "Add new file", 
                        committer: %{name: "Your Name", email: "your@email.com"})

      # List directory contents
      {:ok, files} = Jido.VFS.list_contents(config, "src/")

  """

  @behaviour Jido.VFS.Adapter

  @impl Jido.VFS.Adapter
  def unsupported_operations do
    [
      :read_stream,
      :write_stream,
      :clear,
      :set_visibility,
      :visibility,
      :create_directory,
      :delete_directory,
      :copy_between
    ]
  end

  @impl Jido.VFS.Adapter
  def versioning_module, do: nil

  alias Jido.VFS.Errors
  alias Jido.VFS.Adapter.GitHub.Client
  alias Jido.VFS.Stat.File
  alias Jido.VFS.Stat.Dir

  defstruct [:owner, :repo, :ref, :client, :commit_info]

  @type t :: %__MODULE__{
          owner: String.t(),
          repo: String.t(),
          ref: String.t(),
          client: term(),
          commit_info: %{
            message: String.t(),
            committer: %{name: String.t(), email: String.t()},
            author: %{name: String.t(), email: String.t()}
          }
        }

  @type config :: t()

  @doc """
  Configure the GitHub adapter.

  ## Options

  - `:owner` - GitHub username or organization (required)
  - `:repo` - Repository name (required)
  - `:ref` - Branch name or commit SHA (default: "main")
  - `:auth` - Authentication info (optional for public repos)
  - `:commit_info` - Default commit information for write operations

  """
  @impl true
  def configure(opts) do
    owner = Keyword.fetch!(opts, :owner)
    repo = Keyword.fetch!(opts, :repo)
    ref = Keyword.get(opts, :ref, "main")
    auth = Keyword.get(opts, :auth) || get_config_auth()

    client =
      case auth do
        nil -> Client.new()
        auth_map -> Client.new(auth_map)
      end

    commit_info =
      Keyword.get(opts, :commit_info) || get_config_commit_info()

    config = %__MODULE__{
      owner: owner,
      repo: repo,
      ref: ref,
      client: client,
      commit_info: commit_info
    }

    {__MODULE__, config}
  end

  @impl true
  def read(%__MODULE__{} = config, path) do
    case Client.get_content(config.client, config.owner, config.repo, path, config.ref) do
      {200, %{"content" => content, "encoding" => "base64"}, _response} ->
        case Base.decode64(content, ignore: :whitespace) do
          {:ok, decoded_content} ->
            {:ok, decoded_content}

          :error ->
            {:error,
             Errors.AdapterError.exception(
               adapter: __MODULE__,
               reason: %{status: 200, body: :invalid_base64}
             )}
        end

      {404, _body, _response} ->
        {:error, Errors.FileNotFound.exception(file_path: path)}

      {status, body, _response} ->
        {:error, api_error(status, body)}
    end
  end

  @impl true
  def write(%__MODULE__{} = config, path, content, opts) do
    message = Keyword.get(opts, :message, config.commit_info.message)
    committer = Keyword.get(opts, :committer, config.commit_info.committer)
    author = Keyword.get(opts, :author, config.commit_info.author)

    # Get current file SHA if it exists (required for updates)
    current_sha =
      case Client.get_content(config.client, config.owner, config.repo, path, config.ref) do
        {200, %{"sha" => sha}, _response} -> sha
        _ -> nil
      end

    params = %{
      message: message,
      content: Base.encode64(IO.iodata_to_binary(content)),
      committer: committer,
      author: author
    }

    params = if current_sha, do: Map.put(params, :sha, current_sha), else: params

    case Client.put_content(config.client, config.owner, config.repo, path, params) do
      {201, _body, _response} ->
        :ok

      # Update case
      {200, _body, _response} ->
        :ok

      {status, body, _response} ->
        {:error, api_error(status, body)}
    end
  end

  @impl true
  def delete(%__MODULE__{} = config, path) do
    message = "Delete #{path} via Jido.VFS"
    committer = config.commit_info.committer
    author = config.commit_info.author

    # Get current file SHA (required for deletion)
    case Client.get_content(config.client, config.owner, config.repo, path, config.ref) do
      {200, %{"sha" => sha}, _response} ->
        params = %{
          message: message,
          sha: sha,
          committer: committer,
          author: author
        }

        case Client.delete_content(config.client, config.owner, config.repo, path, params) do
          {200, _body, _response} ->
            :ok

          {status, body, _response} ->
            {:error, api_error(status, body)}
        end

      {404, _body, _response} ->
        {:error, Errors.FileNotFound.exception(file_path: path)}

      {status, body, _response} ->
        {:error, api_error(status, body)}
    end
  end

  @impl true
  def copy(%__MODULE__{} = config, source, destination, opts) do
    with {:ok, content} <- read(config, source) do
      write(config, destination, content, opts)
    end
  end

  @impl true
  def move(%__MODULE__{} = config, source, destination, opts) do
    with {:ok, content} <- read(config, source),
         :ok <- write(config, destination, content, opts),
         :ok <- delete(config, source) do
      :ok
    end
  end

  @impl true
  def list_contents(%__MODULE__{} = config, path \\ "") do
    # Normalize path - GitHub API expects no leading slash for root
    normalized_path =
      case String.trim_leading(path, "/") do
        "" -> ""
        p -> p
      end

    case Client.get_content(
           config.client,
           config.owner,
           config.repo,
           normalized_path,
           config.ref
         ) do
      {200, contents, _response} when is_list(contents) ->
        stats = Enum.map(contents, &content_to_stat/1)
        {:ok, stats}

      {200, %{"type" => "file"} = file_content, _response} ->
        # Single file case
        stat = content_to_stat(file_content)
        {:ok, [stat]}

      {404, _body, _response} ->
        {:error, Errors.DirectoryNotFound.exception(dir_path: normalized_path)}

      {status, body, _response} ->
        {:error, api_error(status, body)}
    end
  end

  @impl true
  def file_exists(%__MODULE__{} = config, path) do
    case Client.get_content(config.client, config.owner, config.repo, path, config.ref) do
      {200, %{"type" => "file"}, _response} -> {:ok, :exists}
      {200, %{"type" => "dir"}, _response} -> {:ok, :missing}
      {404, _body, _response} -> {:ok, :missing}
      {status, body, _response} -> {:error, api_error(status, body)}
    end
  end

  # GitHub doesn't support creating empty directories
  @impl true
  def create_directory(%__MODULE__{}, _path, _opts) do
    unsupported(:create_directory)
  end

  # GitHub doesn't support deleting directories directly
  @impl true
  def delete_directory(%__MODULE__{}, _path, _opts) do
    unsupported(:delete_directory)
  end

  # Add required behavior functions
  @impl true
  def starts_processes(), do: false

  @impl true
  def read_stream(%__MODULE__{}, _path, _opts) do
    unsupported(:read_stream)
  end

  @impl true
  def write_stream(%__MODULE__{}, _path, _opts) do
    unsupported(:write_stream)
  end

  @impl true
  def clear(%__MODULE__{}) do
    unsupported(:clear)
  end

  @impl true
  def set_visibility(%__MODULE__{}, _path, _visibility) do
    unsupported(:set_visibility)
  end

  @impl true
  def visibility(%__MODULE__{}, _path) do
    unsupported(:visibility)
  end

  # Need to implement copy/5 for cross-adapter copying
  @impl true
  def copy(_source_config, _source, _dest_config, _dest, _opts) do
    unsupported(:copy_between)
  end

  # Get authentication from application config
  defp get_config_auth do
    case Application.get_env(:jido_vfs, :github, []) do
      config when is_list(config) ->
        case Keyword.get(config, :access_token) do
          nil -> nil
          token -> %{access_token: token}
        end

      _ ->
        nil
    end
  end

  # Get commit info from application config
  defp get_config_commit_info do
    config = Application.get_env(:jido_vfs, :github, [])
    name = Keyword.get(config, :name, "Jido.VFS")
    email = Keyword.get(config, :email, "jido.vfs@example.com")

    %{
      message: "Update via Jido.VFS",
      committer: %{name: name, email: email},
      author: %{name: name, email: email}
    }
  end

  defp unsupported(operation) do
    {:error, Errors.UnsupportedOperation.exception(operation: operation, adapter: __MODULE__)}
  end

  defp api_error(status, body) do
    Errors.AdapterError.exception(adapter: __MODULE__, reason: %{status: status, body: body})
  end

  # Convert GitHub API content response to Jido.VFS.Stat
  defp content_to_stat(%{"type" => "file", "name" => name, "size" => size}) do
    %File{
      name: name,
      size: size,
      # GitHub doesn't provide mtime in contents API
      mtime: DateTime.utc_now()
    }
  end

  defp content_to_stat(%{"type" => "dir", "name" => name}) do
    %Dir{
      name: name
    }
  end

  defp content_to_stat(%{"name" => name}) do
    # Fallback for entries without explicit type
    %File{
      name: name,
      size: 0,
      mtime: DateTime.utc_now()
    }
  end
end
