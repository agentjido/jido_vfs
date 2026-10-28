defmodule JidoVfsTest.Minio do
  @moduledoc false
  def start_link do
    cur = Process.flag(:trap_exit, true)

    # Set environment variables for minio
    System.delete_env("MINIO_ACCESS_KEY")
    System.delete_env("MINIO_SECRET_KEY")
    System.put_env("MINIO_ROOT_USER", "minio_key")
    System.put_env("MINIO_ROOT_PASSWORD", "minio_secret")
    System.put_env("MINIO_BROWSER", "off")

    # Suppress minio JSON logs by setting quiet mode
    original_level = Logger.level()
    Logger.configure(level: :error)

    try do
      {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(socket)
      :ok = :gen_tcp.close(socket)
      suffix = Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
      directory = Path.join(System.tmp_dir!(), "jido-vfs-minio-" <> suffix)
      options = Keyword.put(config(), :port, port)
      Application.put_env(:jido_vfs, :test_minio_config, options)

      server_options =
        options
        |> Keyword.put(:minio_path, directory)
        |> Keyword.put(:ui, false)
        |> Keyword.delete(:console_address)

      server_options =
        case System.find_executable("minio") do
          nil -> server_options
          executable -> Keyword.put(server_options, :minio_executable, executable)
        end

      {:ok, pid} = MinioServer.start_link(server_options)

      ExUnit.after_suite(fn _ ->
        if Process.alive?(pid), do: Supervisor.stop(pid)
        File.rm_rf!(directory)
      end)

      {:ok, pid}
    rescue
      exception in [MatchError] ->
        case exception.term do
          {:error, {:shutdown, {:failed_to_start_child, MuonTrap.Daemon, {:enoent, _}}}} ->
            reraise """
                    Minio binaries not available.

                    Make sure to to run:
                    $ MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest
                    """,
                    __STACKTRACE__

          _ ->
            reraise exception, __STACKTRACE__
        end
    after
      Logger.configure(level: original_level)
      Process.flag(:trap_exit, cur)
    end
  end

  def config do
    Application.get_env(:jido_vfs, :test_minio_config,
      access_key_id: "minio_key",
      secret_access_key: "minio_secret",
      http_client: ExAws.Request.Req,
      scheme: "http://",
      region: "local",
      host: "127.0.0.1",
      port: 9000,
      console_address: ":9001"
    )
  end

  def wait_for_ready(timeout \\ 5000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for_ready_loop(deadline, Keyword.fetch!(config(), :host), Keyword.fetch!(config(), :port))
  end

  defp wait_for_ready_loop(deadline, host, port) do
    if System.monotonic_time(:millisecond) < deadline do
      if port_open?(host, port) do
        case ExAws.S3.list_buckets() |> ExAws.request(config()) do
          {:ok, _} -> :ok
          _ -> retry_wait_for_ready(deadline, host, port)
        end
      else
        retry_wait_for_ready(deadline, host, port)
      end
    else
      {:error, :timeout}
    end
  end

  defp retry_wait_for_ready(deadline, host, port) do
    Process.sleep(100)
    wait_for_ready_loop(deadline, host, port)
  end

  defp port_open?(host, port) do
    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 100) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      _ ->
        false
    end
  end

  def initialize_bucket(name) do
    ExAws.S3.put_bucket(name, Keyword.fetch!(config(), :region))
    |> ExAws.request(config())
  end

  def recreate_bucket(name) do
    ExAws.S3.delete_bucket(name)
    |> ExAws.request(config())

    {:ok, _} =
      ExAws.S3.put_bucket(name, Keyword.fetch!(config(), :region))
      |> ExAws.request(config())
  end

  def clean_bucket(name) do
    with {:ok, keys} <- list_object_keys(name) do
      for key <- keys do
        ExAws.S3.delete_object(name, key) |> ExAws.request(config())
      end
    end
  end

  defp list_object_keys(name, continuation_token \\ nil, keys \\ []) do
    opts =
      if continuation_token,
        do: [continuation_token: continuation_token],
        else: []

    case ExAws.S3.list_objects_v2(name, opts) |> ExAws.request(config()) do
      {:ok, %{body: body}} ->
        next_keys = keys ++ Enum.map(Map.get(body, :contents, []), & &1.key)

        truncated? =
          case Map.get(body, :is_truncated, false) do
            "true" -> true
            true -> true
            _ -> false
          end

        if truncated? do
          list_object_keys(name, Map.get(body, :next_continuation_token), next_keys)
        else
          {:ok, next_keys}
        end

      error ->
        error
    end
  end
end
