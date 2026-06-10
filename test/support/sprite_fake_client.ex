defmodule JidoVfsTest.SpriteFakeClient do
  @moduledoc false
  import Bitwise

  @write_base64_script ~s(printf "%s" "$JIDO_VFS_DATA" | base64 -d > "$1")
  @append_base64_script ~s(printf "%s" "$JIDO_VFS_DATA" | base64 -d >> "$1")
  @write_raw_script ~s(printf "%s" "$JIDO_VFS_DATA" > "$1")
  @append_raw_script ~s(printf "%s" "$JIDO_VFS_DATA" >> "$1")
  @clear_script ~s(find "$1" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +)
  @probe_marker "__JIDO_VFS_MISSING__"
  @checkpoint_table :jido_vfs_sprite_fake_checkpoints

  defmodule Client do
    @moduledoc false
    defstruct token: nil, base_url: "https://api.sprites.dev", missing_commands: []
  end

  defmodule Sprite do
    @moduledoc false
    defstruct client: nil, name: nil, root: nil
  end

  def new(token, opts \\ []) do
    missing_commands =
      opts
      |> Keyword.get(:missing_commands, [])
      |> List.wrap()
      |> Enum.map(&to_string/1)
      |> Enum.uniq()

    %Client{
      token: token,
      base_url: Keyword.get(opts, :base_url, "https://api.sprites.dev"),
      missing_commands: missing_commands
    }
  end

  def sprite(%Client{} = client, name) do
    root = sprite_root(name)
    File.mkdir_p!(root)
    %Sprite{client: client, name: name, root: root}
  end

  def create(%Client{} = client, name, _opts \\ []) do
    {:ok, sprite(client, name)}
  end

  def destroy(%Sprite{} = sprite) do
    File.rm_rf(sprite.root)
    clear_checkpoint_entries(sprite)
    :ok
  end

  def list_checkpoints(%Sprite{} = sprite, _opts \\ []) do
    checkpoints =
      sprite
      |> checkpoint_entries()
      |> Enum.map(fn checkpoint ->
        %{
          id: checkpoint.id,
          create_time: checkpoint.create_time,
          comment: checkpoint.comment,
          history: checkpoint.history
        }
      end)
      |> Enum.sort_by(&DateTime.to_unix(&1.create_time), :desc)

    {:ok, checkpoints}
  end

  def create_checkpoint(%Sprite{} = sprite, opts \\ []) do
    ensure_checkpoint_table()

    checkpoint_id = "cp_#{System.unique_integer([:positive, :monotonic])}"
    checkpoint_dir = checkpoint_dir(sprite, checkpoint_id)
    create_time = DateTime.utc_now() |> DateTime.truncate(:second)
    comment = Keyword.get(opts, :comment)

    File.rm_rf(checkpoint_dir)
    File.mkdir_p!(checkpoint_dir)
    snapshot_directory(sprite.root, checkpoint_dir)

    entry = %{
      id: checkpoint_id,
      create_time: create_time,
      comment: comment,
      history: []
    }

    put_checkpoint_entry(sprite, entry)

    {:ok, [%{type: "info", data: "checkpoint #{checkpoint_id} created"}]}
  end

  def restore_checkpoint(%Sprite{} = sprite, checkpoint_id) do
    case Enum.find(checkpoint_entries(sprite), &(&1.id == checkpoint_id)) do
      nil ->
        {:error, {:api_error, 404, %{error: "checkpoint not found", checkpoint_id: checkpoint_id}}}

      _entry ->
        restore_from_checkpoint(sprite, checkpoint_id)
        {:ok, [%{type: "info", data: "checkpoint #{checkpoint_id} restored"}]}
    end
  end

  def delete_checkpoint(%Sprite{} = sprite, checkpoint_id, _opts \\ []) do
    ensure_checkpoint_table()

    entries =
      checkpoint_entries(sprite)
      |> Enum.reject(&(&1.id == checkpoint_id))

    put_checkpoint_entries(sprite, entries)
    File.rm_rf(checkpoint_dir(sprite, checkpoint_id))
    :ok
  end

  def cmd(sprite, command, args \\ [], opts \\ [])

  def cmd(%Sprite{} = sprite, "cat", [path], _opts) do
    local_path = local_path(sprite, path)

    cond do
      File.dir?(local_path) ->
        {"cat: #{path}: Is a directory\n", 1}

      true ->
        case File.read(local_path) do
          {:ok, body} -> {body, 0}
          {:error, :enoent} -> {"cat: #{path}: No such file or directory\n", 1}
          {:error, reason} -> {"cat: #{path}: #{inspect(reason)}\n", 1}
        end
    end
  end

  def cmd(%Sprite{} = sprite, "base64", args, _opts) do
    path = List.last(args)
    local_path = local_path(sprite, path)

    cond do
      File.dir?(local_path) ->
        {"base64: #{path}: Is a directory\n", 1}

      true ->
        case File.read(local_path) do
          {:ok, body} -> {Base.encode64(body), 0}
          {:error, :enoent} -> {"base64: #{path}: No such file or directory\n", 1}
          {:error, reason} -> {"base64: #{path}: #{inspect(reason)}\n", 1}
        end
    end
  end

  def cmd(%Sprite{} = sprite, "rm", args, _opts) do
    recursive? = Enum.member?(args, "-rf")
    force? = recursive? || Enum.member?(args, "-f")
    path = extract_path_argument(args)
    local_path = local_path(sprite, path)

    cond do
      recursive? ->
        File.rm_rf(local_path)
        {"", 0}

      File.dir?(local_path) ->
        {"rm: #{path}: Is a directory\n", 1}

      true ->
        case File.rm(local_path) do
          :ok ->
            {"", 0}

          {:error, :enoent} when force? ->
            {"", 0}

          {:error, :enoent} ->
            {"rm: #{path}: No such file or directory\n", 1}

          {:error, reason} ->
            {"rm: #{path}: #{inspect(reason)}\n", 1}
        end
    end
  end

  def cmd(%Sprite{} = sprite, "mv", args, _opts) do
    [source, destination] = extract_positional_args(args)
    source_path = local_path(sprite, source)
    destination_path = local_path(sprite, destination)

    if File.exists?(source_path) do
      case File.rename(source_path, destination_path) do
        :ok -> {"", 0}
        {:error, reason} -> {"mv: cannot move #{source}: #{inspect(reason)}\n", 1}
      end
    else
      {"mv: cannot stat '#{source}': No such file or directory\n", 1}
    end
  end

  def cmd(%Sprite{} = sprite, "cp", args, _opts) do
    [source, destination] = extract_positional_args(args)
    source_path = local_path(sprite, source)
    destination_path = local_path(sprite, destination)

    cond do
      not File.exists?(source_path) ->
        {"cp: cannot stat '#{source}': No such file or directory\n", 1}

      File.dir?(source_path) ->
        case File.cp_r(source_path, destination_path) do
          {:ok, _} -> {"", 0}
          {:error, reason, _} -> {"cp: #{inspect(reason)}\n", 1}
        end

      true ->
        case File.cp(source_path, destination_path) do
          :ok -> {"", 0}
          {:error, reason} -> {"cp: #{inspect(reason)}\n", 1}
        end
    end
  end

  def cmd(%Sprite{} = sprite, "test", ["-e", path], _opts) do
    if File.exists?(local_path(sprite, path)), do: {"", 0}, else: {"", 1}
  end

  def cmd(%Sprite{} = sprite, "find", [path, "-mindepth", "1", "-maxdepth", "1", "-printf", _format], _opts) do
    local_path = local_path(sprite, path)

    cond do
      not File.exists?(local_path) ->
        {"find: '#{path}': No such file or directory\n", 1}

      not File.dir?(local_path) ->
        {"find: '#{path}': Not a directory\n", 1}

      true ->
        output =
          local_path
          |> File.ls!()
          |> Enum.sort()
          |> Enum.map_join("", fn entry ->
            entry_path = Path.join(local_path, entry)
            {:ok, stat} = File.stat(entry_path, time: :posix)
            type = if stat.type == :directory, do: "d", else: "f"
            sprite_path = join_sprite_path(path, entry)
            mode = Integer.to_string(stat.mode &&& 0o777, 8)
            "#{type}\t#{stat.size}\t#{stat.mtime}.0\t#{mode}\t#{sprite_path}\n"
          end)

        {output, 0}
    end
  end

  def cmd(%Sprite{} = sprite, "mkdir", args, _opts) do
    path = extract_path_argument(args)
    local_path = local_path(sprite, path)

    case File.mkdir_p(local_path) do
      :ok -> {"", 0}
      {:error, reason} -> {"mkdir: cannot create directory '#{path}': #{inspect(reason)}\n", 1}
    end
  end

  def cmd(%Sprite{} = sprite, "rmdir", [path], _opts) do
    local_path = local_path(sprite, path)

    cond do
      not File.exists?(local_path) ->
        {"rmdir: failed to remove '#{path}': No such file or directory\n", 1}

      not File.dir?(local_path) ->
        {"rmdir: failed to remove '#{path}': Not a directory\n", 1}

      true ->
        case File.rmdir(local_path) do
          :ok ->
            {"", 0}

          {:error, :enotempty} ->
            {"rmdir: failed to remove '#{path}': Directory not empty\n", 1}

          {:error, reason} ->
            {"rmdir: failed to remove '#{path}': #{inspect(reason)}\n", 1}
        end
    end
  end

  def cmd(%Sprite{} = sprite, "stat", ["-c", "%a", "--", path], _opts) do
    do_stat_mode(sprite, path)
  end

  def cmd(%Sprite{} = sprite, "stat", [format_arg, "--", path], _opts) do
    format = String.replace_prefix(format_arg, "--format=", "")
    do_stat_format(sprite, path, format)
  end

  def cmd(%Sprite{} = sprite, "chmod", [mode, "--", path], _opts) do
    local_path = local_path(sprite, path)

    if File.exists?(local_path) do
      mode_int = String.to_integer(mode, 8)

      case File.chmod(local_path, mode_int) do
        :ok -> {"", 0}
        {:error, reason} -> {"chmod: #{path}: #{inspect(reason)}\n", 1}
      end
    else
      {"chmod: cannot access '#{path}': No such file or directory\n", 1}
    end
  end

  def cmd(%Sprite{} = sprite, "sh", ["-c", script, "_", path], opts) do
    env = opts |> Keyword.get(:env, []) |> Enum.into(%{})
    local_path = local_path(sprite, path)

    case script do
      @write_base64_script ->
        case Base.decode64(Map.get(env, "JIDO_VFS_DATA", "")) do
          {:ok, data} -> write_file(local_path, data, :write)
          :error -> {"base64: invalid input\n", 1}
        end

      @append_base64_script ->
        case Base.decode64(Map.get(env, "JIDO_VFS_DATA", "")) do
          {:ok, data} -> write_file(local_path, data, :append)
          :error -> {"base64: invalid input\n", 1}
        end

      @write_raw_script ->
        write_file(local_path, Map.get(env, "JIDO_VFS_DATA", ""), :write)

      @append_raw_script ->
        write_file(local_path, Map.get(env, "JIDO_VFS_DATA", ""), :append)

      @clear_script ->
        clear_directory(local_path)

      script when is_binary(script) ->
        if String.contains?(script, @probe_marker) do
          probe_required_commands(sprite)
        else
          {"sh: unsupported script\n", 1}
        end
    end
  end

  def cmd(_sprite, command, _args, _opts) do
    {"unsupported command: #{command}\n", 1}
  end

  defp do_stat_mode(%Sprite{} = sprite, path) do
    local_path = local_path(sprite, path)

    if File.exists?(local_path) do
      {:ok, stat} = File.stat(local_path, time: :posix)
      mode = Integer.to_string(stat.mode &&& 0o777, 8)
      {mode <> "\n", 0}
    else
      {"stat: cannot stat '#{path}': No such file or directory\n", 1}
    end
  end

  defp do_stat_format(%Sprite{} = sprite, path, format) do
    local_path = local_path(sprite, path)

    if File.exists?(local_path) do
      {:ok, stat} = File.stat(local_path, time: :posix)
      kind = if stat.type == :directory, do: "directory", else: "regular file"
      size = Integer.to_string(stat.size)
      mtime = Integer.to_string(stat.mtime)
      mode = Integer.to_string(stat.mode &&& 0o777, 8)

      output =
        format
        |> String.replace("%F", kind)
        |> String.replace("%s", size)
        |> String.replace("%Y", mtime)
        |> String.replace("%a", mode)

      {output <> "\n", 0}
    else
      {"stat: cannot stat '#{path}': No such file or directory\n", 1}
    end
  end

  defp write_file(path, data, mode) do
    write_mode = if mode == :append, do: [:append, :binary], else: [:write, :binary]

    case File.open(path, write_mode) do
      {:ok, io_device} ->
        :ok = IO.binwrite(io_device, data)
        File.close(io_device)
        {"", 0}

      {:error, :enoent} ->
        {"write error: No such file or directory\n", 1}

      {:error, reason} ->
        {"write error: #{inspect(reason)}\n", 1}
    end
  end

  defp clear_directory(path) do
    cond do
      not File.exists?(path) ->
        {"find: '#{path}': No such file or directory\n", 1}

      not File.dir?(path) ->
        {"find: '#{path}': Not a directory\n", 1}

      true ->
        path
        |> File.ls!()
        |> Enum.each(fn entry -> File.rm_rf(Path.join(path, entry)) end)

        {"", 0}
    end
  end

  defp probe_required_commands(%Sprite{client: %Client{missing_commands: []}}), do: {"", 0}

  defp probe_required_commands(%Sprite{client: %Client{missing_commands: missing_commands}}) do
    {"#{@probe_marker}#{Enum.join(missing_commands, " ")}\n", 42}
  end

  defp extract_positional_args(args) do
    args
    |> Enum.reject(&(&1 in ["-a", "--"]))
    |> Enum.take(-2)
  end

  defp extract_path_argument(args) do
    args
    |> Enum.reject(&(&1 in ["-p", "-f", "-rf", "--"]))
    |> List.last()
  end

  defp sprite_root(name) do
    safe_name = name |> to_string() |> String.replace(~r/[^a-zA-Z0-9_\-]/, "_")
    Path.join([System.tmp_dir!(), "jido_vfs_sprite_fake", safe_name])
  end

  defp local_path(%Sprite{root: root}, path) do
    relative =
      path
      |> to_string()
      |> String.trim()
      |> String.trim_leading("/")

    target = Path.expand(Path.join(root, relative))
    expanded_root = Path.expand(root)

    if target == expanded_root or String.starts_with?(target, expanded_root <> "/") do
      target
    else
      raise ArgumentError, "path escaped fake sprite root: #{inspect(path)}"
    end
  end

  defp join_sprite_path(path, entry) do
    trimmed = String.trim_trailing(path, "/")

    normalized =
      cond do
        trimmed == "" -> "/"
        String.starts_with?(trimmed, "/") -> trimmed
        true -> "/" <> trimmed
      end

    if normalized == "/" do
      "/" <> entry
    else
      normalized <> "/" <> entry
    end
  end

  defp ensure_checkpoint_table do
    case :ets.whereis(@checkpoint_table) do
      :undefined ->
        :ets.new(@checkpoint_table, [:named_table, :public, :set])
        :ok

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp checkpoint_entries(%Sprite{name: name}) do
    ensure_checkpoint_table()

    case :ets.lookup(@checkpoint_table, name) do
      [{^name, entries}] -> entries
      [] -> []
    end
  end

  defp put_checkpoint_entry(%Sprite{} = sprite, entry) do
    entries = [entry | checkpoint_entries(sprite)]
    put_checkpoint_entries(sprite, entries)
  end

  defp put_checkpoint_entries(%Sprite{name: name}, entries) do
    ensure_checkpoint_table()
    :ets.insert(@checkpoint_table, {name, entries})
    :ok
  end

  defp clear_checkpoint_entries(%Sprite{name: name}) do
    ensure_checkpoint_table()
    :ets.delete(@checkpoint_table, name)
    File.rm_rf(checkpoint_base_dir(name))
    :ok
  end

  defp checkpoint_base_dir(name) do
    safe_name = name |> to_string() |> String.replace(~r/[^a-zA-Z0-9_\-]/, "_")
    Path.join([System.tmp_dir!(), "jido_vfs_sprite_fake_checkpoints", safe_name])
  end

  defp checkpoint_dir(%Sprite{name: name}, checkpoint_id) do
    Path.join([checkpoint_base_dir(name), checkpoint_id])
  end

  defp snapshot_directory(source_root, destination_root) do
    File.mkdir_p!(destination_root)

    source_root
    |> File.ls!()
    |> Enum.each(fn entry ->
      source = Path.join(source_root, entry)
      destination = Path.join(destination_root, entry)

      if File.dir?(source) do
        File.cp_r!(source, destination)
      else
        File.cp!(source, destination)
      end
    end)
  end

  defp restore_from_checkpoint(%Sprite{} = sprite, checkpoint_id) do
    snapshot_root = checkpoint_dir(sprite, checkpoint_id)

    File.rm_rf(sprite.root)
    File.mkdir_p!(sprite.root)

    if File.exists?(snapshot_root) do
      snapshot_root
      |> File.ls!()
      |> Enum.each(fn entry ->
        source = Path.join(snapshot_root, entry)
        destination = Path.join(sprite.root, entry)

        if File.dir?(source) do
          File.cp_r!(source, destination)
        else
          File.cp!(source, destination)
        end
      end)
    end
  end
end
