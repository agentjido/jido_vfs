defmodule Jido.VFS.Adapter.Sprite.CommandTransport do
  @moduledoc """
  Helpers for executing and validating shell commands against a Sprite client.
  """

  alias Jido.VFS.Adapter.Sprite.Config
  alias Jido.VFS.Errors

  @missing_marker "__JIDO_VFS_MISSING__"

  @doc """
  Executes a command through the configured Sprite client and normalizes the result.
  """
  @spec execute(Config.t(), String.t(), [String.t()], keyword()) ::
          {:ok, {binary(), integer()}} | {:error, term()}
  def execute(%Config{} = config, command, args, opts \\ []) do
    cmd_opts = Keyword.merge([stderr_to_stdout: true], opts)

    try do
      result =
        apply(config.client, :cmd, [config.sprite, command, Enum.map(args, &to_string/1), cmd_opts])

      case result do
        {output, code} when is_binary(output) and is_integer(code) ->
          {:ok, {output, code}}

        {output, code} when is_integer(code) ->
          {:ok, {IO.iodata_to_binary(output), code}}

        other ->
          {:error, {:unexpected_cmd_result, other}}
      end
    rescue
      exception ->
        {:error, exception}
    catch
      kind, reason ->
        {:error, {kind, reason}}
    end
  end

  @doc """
  Verifies that the configured Sprite environment exposes the required shell commands.
  """
  @spec probe_required_commands(Config.t(), [String.t()] | term()) :: :ok | {:error, term()}
  def probe_required_commands(%Config{} = config, commands) when is_list(commands) do
    unique_commands =
      commands
      |> Enum.map(&normalize_command/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if unique_commands == [] do
      :ok
    else
      script = probe_script(unique_commands)

      case execute(config, "sh", ["-c", script, "_", "/"]) do
        {:ok, {_output, 0}} ->
          :ok

        {:ok, {output, code}} ->
          {:error, probe_command_error(output, code)}

        {:error, reason} ->
          {:error,
           Errors.AdapterError.exception(
             adapter: Jido.VFS.Adapter.Sprite,
             reason: %{operation: :probe_required_commands, reason: reason}
           )}
      end
    end
  end

  def probe_required_commands(%Config{}, commands) do
    {:error,
     Errors.AdapterError.exception(
       adapter: Jido.VFS.Adapter.Sprite,
       reason: %{
         operation: :probe_required_commands,
         reason: :invalid_probe_commands,
         commands: commands
       }
     )}
  end

  defp probe_script(commands) do
    command_list = Enum.join(commands, " ")

    """
    missing=""
    for cmd in #{command_list}; do
      command -v "$cmd" >/dev/null 2>&1 || missing="$missing $cmd"
    done
    if [ -n "$missing" ]; then
      echo "#{@missing_marker}${missing# }"
      exit 42
    fi
    """
  end

  defp normalize_command(command) do
    command
    |> to_string()
    |> String.trim()
    |> case do
      "" -> nil
      normalized -> normalized
    end
  end

  defp probe_command_error(output, code) do
    missing_commands = parse_missing_commands(output)

    if missing_commands == [] do
      Errors.AdapterError.exception(
        adapter: Jido.VFS.Adapter.Sprite,
        reason: %{
          operation: :probe_required_commands,
          reason: :probe_command_failed,
          exit_code: code,
          output: output
        }
      )
    else
      Errors.AdapterError.exception(
        adapter: Jido.VFS.Adapter.Sprite,
        reason: %{
          operation: :probe_required_commands,
          reason: :missing_command_primitives,
          missing_commands: missing_commands,
          exit_code: code,
          output: output
        }
      )
    end
  end

  defp parse_missing_commands(output) when is_binary(output) do
    case String.split(output, @missing_marker, parts: 2) do
      [_before, missing] ->
        missing
        |> String.trim()
        |> String.split(~r/\s+/, trim: true)

      _ ->
        []
    end
  end
end
