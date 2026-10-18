defmodule Jido.VFS.Adapter.Sprite do
  @moduledoc """
  Jido.VFS adapter for Fly.io Sprites.

  This adapter executes shell commands on a Sprite through `sprites-ex`
  and maps the results into the `Jido.VFS.Adapter` behaviour.

  ## Configuration

      filesystem =
        Jido.VFS.Adapter.Sprite.configure(
          sprite_name: "my-sprite",
          token: System.fetch_env!("SPRITES_TOKEN"),
          root: "/workspace",
          encoding: :base64
        )

  ### Options

  - `:sprite` - Existing Sprite handle (optional)
  - `:sprite_name` - Sprite name when `:sprite` is not given
  - `:token` - Sprites API token (falls back to `SPRITES_TOKEN`)
  - `:base_url` - API base URL (default: `"https://api.sprites.dev"`)
  - `:encoding` - `:base64` (binary-safe, default) or `:raw` (text-only)
  - `:root` - Root path on sprite filesystem (default: `"/"`)
  - `:create_on_demand` - Create sprite in `configure/1` (default: `false`)
  - `:client` - Client module (default: `Sprites`)
  - `:client_opts` - Extra options passed to `client.new/2` (default: `[]`)
  - `:probe_commands` - Validate required shell primitives during configure (default: `true`)
  - `:required_commands` - Override command primitives to probe
  """

  import Bitwise

  alias Jido.VFS.Adapter.Sprite.CommandTransport
  alias Jido.VFS.Errors
  alias Jido.VFS.Stat.Dir
  alias Jido.VFS.Stat.File

  @behaviour Jido.VFS.Adapter

  @impl Jido.VFS.Adapter
  def unsupported_operations, do: []

  @impl Jido.VFS.Adapter
  def versioning_module, do: Jido.VFS.Adapter.Sprite.Versioning

  @default_base_url "https://api.sprites.dev"
  @default_client :"Elixir.Sprites"
  @find_print_format "%y\t%s\t%T@\t%m\t%p\n"
  @write_base64_script ~s(printf "%s" "$JIDO_VFS_DATA" | base64 -d > "$1")
  @append_base64_script ~s(printf "%s" "$JIDO_VFS_DATA" | base64 -d >> "$1")
  @write_raw_script ~s(printf "%s" "$JIDO_VFS_DATA" > "$1")
  @append_raw_script ~s(printf "%s" "$JIDO_VFS_DATA" >> "$1")
  @clear_script ~s(find "$1" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +)
  @required_commands ~w(base64 cat chmod cp find mkdir mv rm rmdir sh stat test)

  defmodule Config do
    @moduledoc """
    Runtime configuration for the Sprite adapter.
    """

    @type encoding :: :base64 | :raw

    @type t :: %__MODULE__{
            sprite: term(),
            sprite_name: String.t() | nil,
            token: String.t() | nil,
            base_url: String.t(),
            encoding: encoding(),
            root: String.t(),
            client: module()
          }

    defstruct sprite: nil,
              sprite_name: nil,
              token: nil,
              base_url: "https://api.sprites.dev",
              encoding: :base64,
              root: "/",
              client: :"Elixir.Sprites"
  end

  defmodule WriteStream do
    @moduledoc """
    Collectable stream used to buffer writes before sending them to a Sprite.
    """

    @type t :: %__MODULE__{
            config: Config.t(),
            path: String.t(),
            opts: keyword()
          }

    @enforce_keys [:config, :path]
    defstruct config: nil, path: nil, opts: []

    defimpl Collectable do
      def into(%{config: config, path: path, opts: opts} = stream) do
        collector_fun = fn
          list, {:cont, elem} ->
            [elem | list]

          list, :done ->
            content = IO.iodata_to_binary(:lists.reverse(list))

            case Jido.VFS.Adapter.Sprite.write(config, path, content, opts) do
              :ok ->
                stream

              {:error, %_{} = error} ->
                raise error
            end

          _list, :halt ->
            :ok
        end

        {[], collector_fun}
      end
    end
  end

  @impl Jido.VFS.Adapter
  def starts_processes, do: false

  @impl Jido.VFS.Adapter
  def configure(opts) do
    client = Keyword.get(opts, :client, @default_client)

    client_opts =
      case Keyword.get(opts, :client_opts, []) do
        list when is_list(list) -> list
        _ -> []
      end

    encoding = normalize_encoding(Keyword.get(opts, :encoding, :base64))
    base_url = Keyword.get(opts, :base_url, @default_base_url)
    root = normalize_root(Keyword.get(opts, :root, "/"))
    sprite_name = Keyword.get(opts, :sprite_name)
    token = Keyword.get(opts, :token) || System.get_env("SPRITES_TOKEN")
    create_on_demand = Keyword.get(opts, :create_on_demand, false)
    probe_commands? = Keyword.get(opts, :probe_commands, true)
    required_commands = Keyword.get(opts, :required_commands, @required_commands)

    sprite =
      case Keyword.get(opts, :sprite) do
        nil ->
          ensure_client_loaded!(client)

          client_handle =
            apply(client, :new, [token || missing_token!(), [base_url: base_url] ++ client_opts])

          if create_on_demand do
            create_or_connect_sprite(client, client_handle, sprite_name || missing_sprite_name!())
          else
            apply(client, :sprite, [client_handle, sprite_name || missing_sprite_name!()])
          end

        sprite ->
          sprite
      end

    config = %Config{
      sprite: sprite,
      sprite_name: sprite_name,
      token: token,
      base_url: base_url,
      encoding: encoding,
      root: root,
      client: client
    }

    if probe_commands? do
      case CommandTransport.probe_required_commands(config, required_commands) do
        :ok -> {__MODULE__, config}
        {:error, error} -> raise error
      end
    else
      {__MODULE__, config}
    end
  end

  @impl Jido.VFS.Adapter
  def write(%Config{} = config, path, contents, opts) do
    target = full_path(config, path)
    payload = IO.iodata_to_binary(contents)

    with :ok <- ensure_parent_directory(config, target, opts),
         :ok <- run_write_command(config, target, payload, :write),
         :ok <- maybe_set_mode(config, target, Keyword.get(opts, :visibility), :file) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def write_stream(%Config{} = config, path, opts) do
    {:ok, %WriteStream{config: config, path: path, opts: opts}}
  end

  @impl Jido.VFS.Adapter
  def read(%Config{} = config, path) do
    target = full_path(config, path)

    case config.encoding do
      :base64 ->
        with {:ok, encoded} <- run_output_command(config, :read, target, "base64", ["-w0", target]),
             {:ok, decoded} <- decode_base64(encoded, target) do
          {:ok, decoded}
        end

      :raw ->
        run_output_command(config, :read, target, "cat", [target])
    end
  end

  @impl Jido.VFS.Adapter
  def read_stream(%Config{} = config, path, opts) do
    chunk_size = normalize_chunk_size(Keyword.get(opts, :chunk_size, 1024))

    case read(config, path) do
      {:ok, content} ->
        chunks =
          if chunk_size == :line do
            split_lines(content)
          else
            Jido.VFS.chunk(content, chunk_size)
          end

        {:ok, chunks}

      error ->
        error
    end
  end

  @impl Jido.VFS.Adapter
  def delete(%Config{} = config, path) do
    target = full_path(config, path)

    run_ok_command(config, :delete, target, "rm", ["-f", "--", target])
  end

  @impl Jido.VFS.Adapter
  def move(%Config{} = config, source, destination, opts) do
    source_path = full_path(config, source)
    destination_path = full_path(config, destination)

    with :ok <- ensure_parent_directory(config, destination_path, opts),
         :ok <-
           run_ok_command(config, :move, source_path, "mv", ["--", source_path, destination_path]) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def copy(%Config{} = config, source, destination, opts) do
    source_path = full_path(config, source)
    destination_path = full_path(config, destination)

    with :ok <- ensure_parent_directory(config, destination_path, opts),
         :ok <-
           run_ok_command(config, :copy, source_path, "cp", [
             "-a",
             "--",
             source_path,
             destination_path
           ]) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def copy(
        %Config{} = source_config,
        source,
        %Config{} = destination_config,
        destination,
        opts
      ) do
    with {:ok, content} <- read(source_config, source),
         :ok <- write(destination_config, destination, content, opts) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def file_exists(%Config{} = config, path) do
    target = full_path(config, path)

    case CommandTransport.execute(config, "test", ["-e", target]) do
      {:ok, {_output, 0}} ->
        {:ok, :exists}

      {:ok, {output, 1}} ->
        if contains?(output, "permission denied") do
          {:error, Errors.PermissionDenied.exception(target_path: target, operation: "file_exists")}
        else
          {:ok, :missing}
        end

      {:ok, {output, code}} ->
        {:error, map_command_error(:file_exists, target, output, code)}

      {:error, reason} ->
        {:error, adapter_error(reason)}
    end
  end

  @impl Jido.VFS.Adapter
  def list_contents(%Config{} = config, path) do
    target = full_path(config, path)

    with {:ok, output} <-
           run_output_command(config, :list_contents, target, "find", [
             target,
             "-mindepth",
             "1",
             "-maxdepth",
             "1",
             "-printf",
             @find_print_format
           ]) do
      parse_find_output(output)
    end
  end

  @impl Jido.VFS.Adapter
  def create_directory(%Config{} = config, path, opts) do
    target = full_path(config, path)

    with :ok <- run_ok_command(config, :create_directory, target, "mkdir", ["-p", "--", target]),
         :ok <-
           maybe_set_mode(config, target, Keyword.get(opts, :directory_visibility), :directory) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def delete_directory(%Config{} = config, path, opts) do
    target = full_path(config, path)

    if Keyword.get(opts, :recursive, false) do
      run_ok_command(config, :delete_directory, target, "rm", ["-rf", "--", target])
    else
      run_ok_command(config, :delete_directory, target, "rmdir", [target])
    end
  end

  @impl Jido.VFS.Adapter
  def clear(%Config{} = config) do
    root = full_path(config, ".")

    run_ok_command(config, :clear, root, "sh", ["-c", @clear_script, "_", root])
  end

  @impl Jido.VFS.Adapter
  def set_visibility(%Config{} = config, path, visibility) do
    target = full_path(config, path)

    with {:ok, portable_visibility} <- normalize_visibility(visibility),
         {:ok, stat} <- stat(config, path),
         mode <- mode_for_struct(portable_visibility, stat),
         :ok <- run_ok_command(config, :set_visibility, target, "chmod", [mode, "--", target]) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def visibility(%Config{} = config, path) do
    target = full_path(config, path)

    with {:ok, mode_string} <- run_output_command(config, :visibility, target, "stat", ["-c", "%a", "--", target]),
         {:ok, mode} <- parse_mode(mode_string, target) do
      {:ok, visibility_for_mode(mode)}
    end
  end

  @impl Jido.VFS.Adapter
  def stat(%Config{} = config, path) do
    target = full_path(config, path)

    with {:ok, output} <-
           run_output_command(config, :stat, target, "stat", [
             "--format=%F\t%s\t%Y\t%a",
             "--",
             target
           ]),
         {:ok, stat} <- parse_stat(output, target) do
      {:ok, stat}
    end
  end

  @impl Jido.VFS.Adapter
  def append(%Config{} = config, path, contents, opts) do
    target = full_path(config, path)
    payload = IO.iodata_to_binary(contents)

    with :ok <- ensure_parent_directory(config, target, opts),
         :ok <- run_write_command(config, target, payload, :append),
         :ok <- maybe_set_mode(config, target, Keyword.get(opts, :visibility), :file) do
      :ok
    end
  end

  @impl Jido.VFS.Adapter
  def write_version(%Config{} = config, path, contents, opts) do
    with :ok <- write(config, path, contents, opts),
         {:ok, version_id} <-
           create_checkpoint(config,
             comment: "write_version path=#{path} ts=#{DateTime.utc_now() |> DateTime.to_iso8601()}"
           ) do
      {:ok, version_id}
    end
  end

  @impl Jido.VFS.Adapter
  def read_version(%Config{} = config, path, version_id) do
    temp_comment = "__jido_vfs_read_version_tmp__ #{System.unique_integer([:positive, :monotonic])}"

    with {:ok, temporary_version} <- create_checkpoint(config, comment: temp_comment),
         :ok <- restore_checkpoint(config, version_id) do
      read_result = read(config, path)
      restore_result = restore_checkpoint(config, temporary_version)
      _ = maybe_delete_checkpoint(config, temporary_version)

      case restore_result do
        :ok ->
          read_result

        {:error, reason} ->
          {:error,
           Errors.AdapterError.exception(
             adapter: __MODULE__,
             reason: %{
               operation: :read_version,
               restore_after_read_failed: true,
               version_id: version_id,
               rollback_to: temporary_version,
               reason: reason
             }
           )}
      end
    end
  end

  @impl Jido.VFS.Adapter
  def list_versions(%Config{} = config, _path) do
    with {:ok, checkpoints} <- list_checkpoints(config) do
      versions =
        checkpoints
        |> Enum.map(fn checkpoint ->
          %{
            version_id: checkpoint_id(checkpoint),
            timestamp: checkpoint_timestamp(checkpoint)
          }
        end)
        |> Enum.reject(&is_nil(&1.version_id))
        |> Enum.sort_by(& &1.timestamp, :desc)

      {:ok, versions}
    end
  end

  @impl Jido.VFS.Adapter
  def delete_version(%Config{} = config, _path, version_id) do
    delete_checkpoint(config, version_id)
  end

  @impl Jido.VFS.Adapter
  def get_latest_version(%Config{} = config, path) do
    case list_versions(config, path) do
      {:ok, [%{version_id: version_id} | _]} ->
        {:ok, version_id}

      {:ok, []} ->
        {:error, Errors.FileNotFound.exception(file_path: path)}

      error ->
        error
    end
  end

  @impl Jido.VFS.Adapter
  def restore_version(%Config{} = config, _path, version_id) do
    restore_checkpoint(config, version_id)
  end

  @doc false
  def create_checkpoint(%Config{} = config, opts \\ []) do
    before_ids = list_checkpoint_ids(config)
    comment = Keyword.get(opts, :comment)
    checkpoint_opts = if is_binary(comment), do: [comment: comment], else: []

    with {:ok, _response} <- create_checkpoint_raw(config, checkpoint_opts),
         {:ok, checkpoints} <- list_checkpoints(config),
         {:ok, before_set} <- before_ids do
      case newest_checkpoint_id(checkpoints, before_set) do
        nil ->
          {:error,
           Errors.AdapterError.exception(
             adapter: __MODULE__,
             reason: %{operation: :create_checkpoint, reason: :checkpoint_not_visible_after_create}
           )}

        version_id ->
          {:ok, version_id}
      end
    end
  end

  defp normalize_encoding(:base64), do: :base64
  defp normalize_encoding(:raw), do: :raw

  defp normalize_encoding(value) do
    raise ArgumentError,
          "invalid Sprite encoding #{inspect(value)}. Expected :base64 or :raw"
  end

  defp normalize_root(root) do
    root = root |> to_string() |> String.trim()

    root =
      cond do
        root == "" -> "/"
        String.starts_with?(root, "/") -> root
        true -> "/" <> root
      end

    case String.trim_trailing(root, "/") do
      "" -> "/"
      trimmed -> trimmed
    end
  end

  defp normalize_visibility(visibility) do
    case Jido.VFS.Visibility.guard_portable(visibility) do
      {:ok, portable_visibility} ->
        {:ok, portable_visibility}

      :error ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: {:invalid_visibility, visibility}
         )}
    end
  end

  defp create_or_connect_sprite(client, client_handle, sprite_name) do
    case maybe_create_sprite(client, client_handle, sprite_name) do
      {:ok, sprite} ->
        sprite

      {:error, reason} ->
        if sprite_already_exists?(reason) do
          apply(client, :sprite, [client_handle, sprite_name])
        else
          raise ArgumentError,
                "unable to create sprite #{inspect(sprite_name)}: #{inspect(reason)}"
        end
    end
  end

  defp maybe_create_sprite(client, client_handle, sprite_name) do
    cond do
      function_exported?(client, :create, 3) ->
        apply(client, :create, [client_handle, sprite_name, []])

      function_exported?(client, :create, 2) ->
        apply(client, :create, [client_handle, sprite_name])

      true ->
        {:error, :create_not_supported}
    end
  end

  defp sprite_already_exists?(reason) when is_binary(reason) do
    contains?(reason, "already exists")
  end

  defp sprite_already_exists?(%{message: message}) when is_binary(message),
    do: sprite_already_exists?(message)

  defp sprite_already_exists?(%{reason: reason}), do: sprite_already_exists?(reason)
  defp sprite_already_exists?({:error, reason}), do: sprite_already_exists?(reason)
  defp sprite_already_exists?(_reason), do: false

  defp missing_token! do
    raise ArgumentError,
          "Sprite adapter requires :token (or SPRITES_TOKEN env var) when :sprite is not provided"
  end

  defp missing_sprite_name! do
    raise ArgumentError, "Sprite adapter requires :sprite_name when :sprite is not provided"
  end

  defp ensure_client_loaded!(client) do
    case Code.ensure_loaded(client) do
      {:module, _module} ->
        :ok

      {:error, reason} ->
        raise ArgumentError,
              "Sprite client module #{inspect(client)} is not available: #{inspect(reason)}"
    end
  end

  defp full_path(%Config{} = config, path) do
    config.root
    |> Jido.VFS.RelativePath.join_prefix(path)
    |> ensure_absolute_path()
  end

  defp ensure_absolute_path(path) do
    if String.starts_with?(path, "/"), do: path, else: "/" <> path
  end

  defp ensure_parent_directory(%Config{} = config, target_path, opts) do
    parent = Path.dirname(target_path)

    with :ok <- run_ok_command(config, :create_directory, parent, "mkdir", ["-p", "--", parent]),
         :ok <-
           maybe_set_mode(config, parent, Keyword.get(opts, :directory_visibility), :directory) do
      :ok
    end
  end

  defp run_write_command(%Config{} = config, target, payload, action) do
    {script, content} =
      case {config.encoding, action} do
        {:base64, :write} -> {@write_base64_script, Base.encode64(payload)}
        {:base64, :append} -> {@append_base64_script, Base.encode64(payload)}
        {:raw, :write} -> {@write_raw_script, payload}
        {:raw, :append} -> {@append_raw_script, payload}
      end

    opts = [env: [{"JIDO_VFS_DATA", content}]]
    run_ok_command(config, action, target, "sh", ["-c", script, "_", target], opts)
  end

  defp maybe_set_mode(_config, _path, nil, _kind), do: :ok

  defp maybe_set_mode(%Config{} = config, path, visibility, kind) do
    with {:ok, portable_visibility} <- normalize_visibility(visibility),
         mode <- mode_for_kind(portable_visibility, kind) do
      run_ok_command(config, :set_visibility, path, "chmod", [mode, "--", path])
    end
  end

  defp list_checkpoints(%Config{} = config) do
    cond do
      function_exported?(config.client, :list_checkpoints, 2) ->
        case apply(config.client, :list_checkpoints, [config.sprite, []]) do
          {:ok, checkpoints} when is_list(checkpoints) -> {:ok, checkpoints}
          {:error, reason} -> {:error, adapter_error({:list_checkpoints, reason})}
          other -> {:error, adapter_error({:list_checkpoints, :unexpected_response, other})}
        end

      function_exported?(config.client, :list_checkpoints, 1) ->
        case apply(config.client, :list_checkpoints, [config.sprite]) do
          {:ok, checkpoints} when is_list(checkpoints) -> {:ok, checkpoints}
          checkpoints when is_list(checkpoints) -> {:ok, checkpoints}
          {:error, reason} -> {:error, adapter_error({:list_checkpoints, reason})}
          other -> {:error, adapter_error({:list_checkpoints, :unexpected_response, other})}
        end

      true ->
        {:error, Errors.UnsupportedOperation.exception(operation: :list_versions, adapter: __MODULE__)}
    end
  end

  defp create_checkpoint_raw(%Config{} = config, opts) do
    cond do
      function_exported?(config.client, :create_checkpoint, 2) ->
        case apply(config.client, :create_checkpoint, [config.sprite, opts]) do
          {:ok, response} ->
            consume_checkpoint_stream(response)

          {:error, reason} ->
            {:error, adapter_error({:create_checkpoint, reason})}

          other ->
            {:error, adapter_error({:create_checkpoint, :unexpected_response, other})}
        end

      function_exported?(config.client, :create_checkpoint, 1) ->
        case apply(config.client, :create_checkpoint, [config.sprite]) do
          {:ok, response} -> consume_checkpoint_stream(response)
          {:error, reason} -> {:error, adapter_error({:create_checkpoint, reason})}
          other -> {:error, adapter_error({:create_checkpoint, :unexpected_response, other})}
        end

      true ->
        {:error, Errors.UnsupportedOperation.exception(operation: :write_version, adapter: __MODULE__)}
    end
  end

  defp restore_checkpoint(%Config{} = config, checkpoint_id) do
    cond do
      function_exported?(config.client, :restore_checkpoint, 2) ->
        case apply(config.client, :restore_checkpoint, [config.sprite, checkpoint_id]) do
          {:ok, response} ->
            case consume_checkpoint_stream(response) do
              {:ok, _} -> :ok
              {:error, reason} -> {:error, reason}
            end

          {:error, reason} ->
            {:error, adapter_error({:restore_checkpoint, reason})}

          other ->
            {:error, adapter_error({:restore_checkpoint, :unexpected_response, other})}
        end

      true ->
        {:error, Errors.UnsupportedOperation.exception(operation: :restore_version, adapter: __MODULE__)}
    end
  end

  defp delete_checkpoint(%Config{} = config, checkpoint_id) do
    cond do
      function_exported?(config.client, :delete_checkpoint, 3) ->
        case apply(config.client, :delete_checkpoint, [config.sprite, checkpoint_id, []]) do
          :ok -> :ok
          {:ok, _} -> :ok
          {:error, reason} -> {:error, adapter_error({:delete_checkpoint, reason})}
          other -> {:error, adapter_error({:delete_checkpoint, :unexpected_response, other})}
        end

      function_exported?(config.client, :delete_checkpoint, 2) ->
        case apply(config.client, :delete_checkpoint, [config.sprite, checkpoint_id]) do
          :ok -> :ok
          {:ok, _} -> :ok
          {:error, reason} -> {:error, adapter_error({:delete_checkpoint, reason})}
          other -> {:error, adapter_error({:delete_checkpoint, :unexpected_response, other})}
        end

      true ->
        {:error, Errors.UnsupportedOperation.exception(operation: :delete_version, adapter: __MODULE__)}
    end
  end

  defp maybe_delete_checkpoint(%Config{} = config, checkpoint_id) do
    case delete_checkpoint(config, checkpoint_id) do
      {:error, %Errors.UnsupportedOperation{}} -> :ok
      _ -> :ok
    end
  end

  defp consume_checkpoint_stream(response) do
    cond do
      is_list(response) ->
        validate_checkpoint_messages(response)

      match?(%Stream{}, response) ->
        response |> Enum.to_list() |> validate_checkpoint_messages()

      Enumerable.impl_for(response) != nil ->
        response |> Enum.to_list() |> validate_checkpoint_messages()

      true ->
        {:ok, response}
    end
  end

  defp validate_checkpoint_messages(messages) do
    maybe_error =
      Enum.find(messages, fn message ->
        message_type(message) == "error"
      end)

    case maybe_error do
      nil ->
        {:ok, messages}

      message ->
        normalized = message_to_map(message)
        error_data = Map.get(normalized, :error) || Map.get(normalized, "error") || normalized

        {:error, adapter_error({:checkpoint_stream_error, error_data})}
    end
  end

  defp message_type(%{type: type}) when is_binary(type), do: String.downcase(type)
  defp message_type(%{"type" => type}) when is_binary(type), do: String.downcase(type)
  defp message_type(_), do: ""

  defp message_to_map(%_{} = struct), do: Map.from_struct(struct)
  defp message_to_map(%{} = map), do: map
  defp message_to_map(other), do: %{data: other}

  defp list_checkpoint_ids(%Config{} = config) do
    with {:ok, checkpoints} <- list_checkpoints(config) do
      ids =
        checkpoints
        |> Enum.map(&checkpoint_id/1)
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      {:ok, ids}
    end
  end

  defp newest_checkpoint_id(checkpoints, previous_ids) do
    checkpoints
    |> Enum.reject(fn checkpoint ->
      id = checkpoint_id(checkpoint)
      is_nil(id) or MapSet.member?(previous_ids, id)
    end)
    |> Enum.sort_by(&checkpoint_timestamp/1, :desc)
    |> List.first()
    |> checkpoint_id()
  end

  defp checkpoint_id(%{id: id}) when is_binary(id), do: id
  defp checkpoint_id(%{"id" => id}) when is_binary(id), do: id
  defp checkpoint_id(_), do: nil

  defp checkpoint_timestamp(%{create_time: %DateTime{} = dt}), do: DateTime.to_unix(dt)
  defp checkpoint_timestamp(%{"create_time" => %DateTime{} = dt}), do: DateTime.to_unix(dt)

  defp checkpoint_timestamp(%{create_time: create_time}) when is_binary(create_time) do
    parse_checkpoint_time(create_time)
  end

  defp checkpoint_timestamp(%{"create_time" => create_time}) when is_binary(create_time) do
    parse_checkpoint_time(create_time)
  end

  defp checkpoint_timestamp(%{timestamp: ts}) when is_integer(ts), do: ts
  defp checkpoint_timestamp(%{"timestamp" => ts}) when is_integer(ts), do: ts
  defp checkpoint_timestamp(_), do: 0

  defp parse_checkpoint_time(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime)
      _ -> 0
    end
  end

  defp mode_for_kind(:public, :file), do: "644"
  defp mode_for_kind(:private, :file), do: "600"
  defp mode_for_kind(:public, :directory), do: "755"
  defp mode_for_kind(:private, :directory), do: "700"

  defp mode_for_struct(visibility, %Dir{}), do: mode_for_kind(visibility, :directory)
  defp mode_for_struct(visibility, %File{}), do: mode_for_kind(visibility, :file)

  defp run_ok_command(%Config{} = config, operation, path, command, args, opts \\ []) do
    case CommandTransport.execute(config, command, args, opts) do
      {:ok, {_output, 0}} ->
        :ok

      {:ok, {output, code}} ->
        {:error, map_command_error(operation, path, output, code)}

      {:error, reason} ->
        {:error, adapter_error(reason)}
    end
  end

  defp run_output_command(%Config{} = config, operation, path, command, args, opts \\ []) do
    case CommandTransport.execute(config, command, args, opts) do
      {:ok, {output, 0}} ->
        {:ok, output}

      {:ok, {output, code}} ->
        {:error, map_command_error(operation, path, output, code)}

      {:error, reason} ->
        {:error, adapter_error(reason)}
    end
  end

  defp parse_find_output(output) do
    entries =
      output
      |> String.split("\n", trim: true)
      |> Enum.reduce_while([], fn line, acc ->
        case parse_find_line(line) do
          {:ok, stat} ->
            {:cont, [stat | acc]}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case entries do
      {:error, reason} ->
        {:error, reason}

      list ->
        {:ok, Enum.reverse(list)}
    end
  end

  defp parse_find_line(line) do
    case String.split(line, "\t", parts: 5) do
      [type, size_text, mtime_text, mode_text, full_path] ->
        with {:ok, size} <- parse_integer(size_text),
             {:ok, mtime} <- parse_timestamp(mtime_text),
             {:ok, mode} <- parse_mode(mode_text, full_path) do
          struct =
            case type do
              "d" -> Dir
              _ -> File
            end

          {:ok,
           struct!(struct,
             name: path_name(full_path),
             size: size,
             mtime: mtime,
             visibility: visibility_for_mode(mode)
           )}
        else
          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: {:invalid_find_output, line}
         )}
    end
  end

  defp parse_stat(output, path) do
    case output |> String.trim() |> String.split("\t", parts: 4) do
      [kind_text, size_text, mtime_text, mode_text] ->
        with {:ok, size} <- parse_integer(size_text),
             {:ok, mtime} <- parse_integer(mtime_text),
             {:ok, mode} <- parse_mode(mode_text, path) do
          struct =
            if String.contains?(String.downcase(kind_text), "directory") do
              Dir
            else
              File
            end

          {:ok,
           struct!(struct,
             name: path_name(path),
             size: size,
             mtime: mtime,
             visibility: visibility_for_mode(mode)
           )}
        end

      _ ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: {:invalid_stat_output, output}
         )}
    end
  end

  defp path_name(path) do
    normalized = String.trim_trailing(path, "/")
    if normalized == "", do: "/", else: Path.basename(normalized)
  end

  defp parse_integer(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, adapter_error({:invalid_integer, value})}
    end
  end

  defp parse_timestamp(value) do
    value = String.trim(value)

    case Float.parse(value) do
      {parsed, _rest} ->
        {:ok, trunc(parsed)}

      :error ->
        parse_integer(value)
    end
  end

  defp parse_mode(mode_text, path) do
    trimmed = mode_text |> String.trim() |> String.trim_leading("0")
    effective_mode = if trimmed == "", do: "0", else: trimmed

    try do
      {:ok, String.to_integer(effective_mode, 8)}
    rescue
      ArgumentError ->
        {:error, adapter_error({:invalid_mode, path, mode_text})}
    end
  end

  defp visibility_for_mode(mode) do
    if (mode &&& 0o077) == 0 do
      :private
    else
      :public
    end
  end

  defp decode_base64(encoded, path) do
    encoded =
      encoded
      |> String.trim()
      |> String.replace("\n", "")

    case Base.decode64(encoded) do
      {:ok, decoded} ->
        {:ok, decoded}

      :error ->
        {:error,
         Errors.AdapterError.exception(
           adapter: __MODULE__,
           reason: {:invalid_base64_payload, path}
         )}
    end
  end

  defp normalize_chunk_size(size) when is_integer(size) and size > 0, do: size
  defp normalize_chunk_size(:line), do: :line
  defp normalize_chunk_size(_), do: 1024

  defp split_lines(content) do
    if content == "" do
      []
    else
      String.split(content, ~r/(?<=\n)/, trim: true)
    end
  end

  defp map_command_error(operation, path, output, exit_code) do
    cond do
      contains?(output, "no such file") or
        contains?(output, "cannot stat") or
          contains?(output, "not found") ->
        not_found_error(operation, path)

      contains?(output, "permission denied") ->
        Errors.PermissionDenied.exception(target_path: path, operation: Atom.to_string(operation))

      contains?(output, "not a directory") ->
        Errors.NotDirectory.exception(not_dir_path: path)

      contains?(output, "is a directory") ->
        Errors.InvalidPath.exception(invalid_path: path, reason: "is a directory")

      contains?(output, "directory not empty") ->
        Errors.DirectoryNotEmpty.exception(dir_path: path)

      true ->
        Errors.AdapterError.exception(
          adapter: __MODULE__,
          reason: %{
            operation: operation,
            path: path,
            exit_code: exit_code,
            output: output
          }
        )
    end
  end

  defp not_found_error(operation, path)
       when operation in [:list_contents, :create_directory, :delete_directory] do
    Errors.DirectoryNotFound.exception(dir_path: path)
  end

  defp not_found_error(_operation, path), do: Errors.FileNotFound.exception(file_path: path)

  defp contains?(value, expected) when is_binary(value) do
    value
    |> String.downcase()
    |> String.contains?(String.downcase(expected))
  end

  defp adapter_error(reason) do
    Errors.AdapterError.exception(adapter: __MODULE__, reason: reason)
  end
end
