defmodule Jido.VFS.Adapter.S3 do
  @moduledoc """
  Jido.VFS adapter for Amazon S3 compatible storage.

  ## Direct usage

      config = [
        access_key_id: "key",
        secret_access_key: "secret",
        scheme: "https://",
        region: "eu-west-1",
        host: "s3.eu-west-1.amazonaws.com",
        port: 443
      ]
      filesystem = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
      :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")

  ## Usage with a module

      defmodule S3FileSystem do
        use Jido.VFS.Filesystem,
          adapter: Jido.VFS.Adapter.S3,
          bucket: "default",
          config: [
            access_key_id: "key",
            secret_access_key: "secret",
            scheme: "https://",
            region: "eu-west-1",
            host: "s3.eu-west-1.amazonaws.com",
            port: 443
          ]
      end

      S3FileSystem.write("test.txt", "Hello World")
      {:ok, "Hello World"} = S3FileSystem.read("test.txt")
  """

  alias Jido.VFS.Errors

  defmodule Config do
    @moduledoc """
    Runtime configuration for the S3 adapter.
    """

    @type t :: %__MODULE__{
            config: keyword() | map() | nil,
            bucket: String.t() | nil,
            prefix: String.t() | nil
          }

    defstruct config: nil, bucket: nil, prefix: nil
  end

  defmodule StreamUpload do
    @moduledoc """
    Collectable stream for multipart uploads into S3-compatible storage.
    """

    @type t :: %__MODULE__{
            config: Config.t(),
            path: String.t(),
            opts: keyword()
          }

    @enforce_keys [:config, :path]
    defstruct config: nil, path: nil, opts: []

    defimpl Collectable do
      # Minimum part size for S3 multipart upload is 5MB
      @min_part_size 5 * 1024 * 1024

      defp adapter_error(reason) do
        Errors.AdapterError.exception(adapter: Jido.VFS.Adapter.S3, reason: reason)
      end

      defp upload_part(config, path, id, index, data, opts) do
        operation = ExAws.S3.upload_part(config.bucket, path, id, index + 1, data, opts)

        case ExAws.request(operation, config.config) do
          {:ok, %{headers: headers}} ->
            etag =
              Enum.find_value(headers, fn {k, value} ->
                if String.downcase(to_string(k)) == "etag", do: value
              end)

            {:ok, etag}

          {:error, error} ->
            {:error, adapter_error(error)}

          error ->
            {:error, adapter_error(error)}
        end
      end

      defp complete_upload(config, path, upload_id, etags) do
        operation =
          ExAws.S3.complete_multipart_upload(
            config.bucket,
            path,
            upload_id,
            Enum.sort_by(etags, &elem(&1, 0))
          )

        case ExAws.request(operation, config.config) do
          {:ok, response} ->
            {:ok, response}

          {:error, error} ->
            {:error, adapter_error(error)}
        end
      end

      defp abort_upload(config, path, upload_id) do
        ExAws.S3.abort_multipart_upload(config.bucket, path, upload_id)
        |> ExAws.request(config.config)

        :ok
      rescue
        _ -> :ok
      end

      def into(%{config: config, path: path, opts: opts} = stream) do
        case ExAws.S3.initiate_multipart_upload(config.bucket, path, opts)
             |> ExAws.request(config.config) do
          {:ok, %{body: %{upload_id: upload_id}}} ->
            collector_fun = fn
              %{acc: acc, index: index, etags: etags} = data, {:cont, elem} ->
                binary = IO.iodata_to_binary(elem)

                if byte_size(acc) + byte_size(binary) >= @min_part_size do
                  chunk = acc <> binary

                  case upload_part(config, path, upload_id, index, chunk, opts) do
                    {:ok, etag} ->
                      %{data | acc: "", index: index + 1, etags: [{index + 1, etag} | etags]}

                    {:error, error} ->
                      abort_upload(config, path, upload_id)
                      throw({:upload_part_failed, error})
                  end
                else
                  %{data | acc: acc <> binary}
                end

              %{acc: acc, index: index, etags: etags}, :done when byte_size(acc) > 0 ->
                with {:ok, etag} <- upload_part(config, path, upload_id, index, acc, opts),
                     {:ok, _} <- complete_upload(config, path, upload_id, [{index + 1, etag} | etags]) do
                  stream
                else
                  {:error, error} ->
                    abort_upload(config, path, upload_id)
                    throw({:complete_upload_failed, error})
                end

              %{etags: []}, :done ->
                abort_upload(config, path, upload_id)
                stream

              %{etags: etags}, :done ->
                case complete_upload(config, path, upload_id, etags) do
                  {:ok, _} ->
                    stream

                  {:error, error} ->
                    abort_upload(config, path, upload_id)
                    throw({:complete_upload_failed, error})
                end

              _data, :halt ->
                abort_upload(config, path, upload_id)
                :ok
            end

            {%{upload_id: upload_id, acc: "", index: 0, etags: []}, collector_fun}

          {:error, error} ->
            raise adapter_error(error)

          error ->
            raise adapter_error(error)
        end
      end
    end
  end

  @behaviour Jido.VFS.Adapter
  @visibility_table :jido_vfs_s3_visibility_store

  @impl Jido.VFS.Adapter
  def unsupported_operations, do: []

  @impl Jido.VFS.Adapter
  def versioning_module, do: nil

  defp ensure_visibility_table do
    case :ets.whereis(@visibility_table) do
      :undefined ->
        try do
          :ets.new(@visibility_table, [
            :named_table,
            :public,
            :set,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError ->
            :ok
        end

      _ ->
        :ok
    end

    @visibility_table
  end

  defp visibility_key(%Config{} = config, path) do
    {config.bucket, config.prefix, path}
  end

  defp store_visibility(%Config{} = config, path, visibility) do
    table = ensure_visibility_table()
    :ets.insert(table, {visibility_key(config, path), visibility})
  end

  defp get_stored_visibility(%Config{} = config, path) do
    table = ensure_visibility_table()
    resolve_visibility(table, config, path)
  end

  defp delete_stored_visibility(%Config{} = config, path) do
    table = ensure_visibility_table()
    :ets.delete(table, visibility_key(config, path))
    :ok
  end

  defp delete_stored_visibility_prefix(%Config{} = config, path_prefix) do
    table = ensure_visibility_table()
    normalized_prefix = String.trim_trailing(path_prefix, "/")

    :ets.foldl(
      fn
        {{bucket, configured_prefix, path} = key, _visibility}, :ok ->
          if bucket == config.bucket and configured_prefix == config.prefix and
               visibility_key_matches_prefix?(path, path_prefix, normalized_prefix) do
            :ets.delete(table, key)
          end

          :ok

        _, :ok ->
          :ok
      end,
      :ok,
      table
    )

    :ok
  end

  def reset_visibility_store do
    case :ets.whereis(@visibility_table) do
      :undefined ->
        :ok

      table ->
        :ets.delete_all_objects(table)
        :ok
    end
  end

  @impl Jido.VFS.Adapter
  def starts_processes, do: false

  @impl Jido.VFS.Adapter
  def configure(opts) do
    config = %Config{
      config: Keyword.fetch!(opts, :config),
      bucket: Keyword.fetch!(opts, :bucket),
      prefix: Keyword.get(opts, :prefix, "/")
    }

    {__MODULE__, config}
  end

  @impl Jido.VFS.Adapter
  def write(%Config{} = config, path, contents, opts) do
    path = object_path(config, path)

    if visibility = Keyword.get(opts, :visibility) do
      store_visibility(config, path, visibility)
    end

    if dir_visibility = Keyword.get(opts, :directory_visibility) do
      dir_path =
        if String.ends_with?(path, "/"),
          do: path,
          else: parent_directory_path(path)

      if is_binary(dir_path) do
        store_visibility(config, dir_path, dir_visibility)
      end
    end

    opts = maybe_add_acl(opts)
    operation = ExAws.S3.put_object(config.bucket, path, contents, opts)

    case ExAws.request(operation, config.config) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  @impl Jido.VFS.Adapter
  def write_stream(%Config{} = config, path, opts) do
    path = object_path(config, path)

    {:ok,
     %StreamUpload{
       config: config,
       path: path,
       opts: maybe_add_acl(opts)
     }}
  end

  @impl Jido.VFS.Adapter
  def read(%Config{} = config, path) do
    path = object_path(config, path)

    operation = ExAws.S3.get_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, %{body: body}} ->
        {:ok, body}

      {:error, {:http_error, 404, _}} ->
        {:error, %Errors.FileNotFound{file_path: path}}

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}

      error ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  @impl Jido.VFS.Adapter
  def read_stream(%Config{} = config, path, opts) do
    path = object_path(config, path)

    with {:ok, :exists} <- object_exists(config, path) do
      op = ExAws.S3.download_file(config.bucket, path, "", opts)

      stream =
        op
        |> ExAws.S3.Download.build_chunk_stream(config.config)
        |> Task.async_stream(
          fn boundaries ->
            ExAws.S3.Download.get_chunk(op, boundaries, config.config)
          end,
          max_concurrency: Keyword.get(op.opts, :max_concurrency, 8),
          timeout: Keyword.get(op.opts, :timeout, 60_000),
          on_timeout: :kill_task
        )
        |> Stream.map(&stream_chunk_or_raise/1)

      {:ok, stream}
    else
      {:ok, :missing} ->
        {:error, %Errors.FileNotFound{file_path: path}}

      {:error, _} = error ->
        error
    end
  end

  @impl Jido.VFS.Adapter
  def delete(%Config{} = config, path) do
    path = object_path(config, path)

    delete_object(config, path)
  end

  @impl Jido.VFS.Adapter
  def move(%Config{} = config, source, destination, opts) do
    with :ok <- copy(config, source, destination, config, opts) do
      delete(config, source)
    end
  end

  @impl Jido.VFS.Adapter
  def copy(%Config{} = source_config, source, %Config{} = dest_config, destination, opts) do
    source_path = object_path(source_config, source)
    destination_path = object_path(dest_config, destination)

    case {source_config.config, dest_config.config} do
      {config, config} ->
        do_copy(config, {source_config.bucket, source_path}, {dest_config.bucket, destination_path}, opts)

      _ ->
        with {:ok, content} <- read(source_config, source),
             :ok <- write(dest_config, destination, content, opts) do
          :ok
        end
    end
  end

  @impl Jido.VFS.Adapter
  def copy(%Config{} = source_config, source, destination, dest_config, opts) do
    copy(source_config, source, dest_config, destination, opts)
  end

  @impl Jido.VFS.Adapter
  def copy(%Config{} = config, source, destination, opts) do
    copy(config, source, destination, config, opts)
  end

  defp do_copy(config, {source_bucket, source_path}, {destination_bucket, destination_path}, opts) do
    operation =
      ExAws.S3.put_object_copy(
        destination_bucket,
        destination_path,
        source_bucket,
        source_path,
        maybe_add_acl(opts)
      )

    case ExAws.request(operation, config) do
      {:ok, _} ->
        :ok

      {:error, {:http_error, 404, _}} ->
        {:error, %Errors.FileNotFound{file_path: source_path}}

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  @impl Jido.VFS.Adapter
  def file_exists(%Config{} = config, path) do
    object_exists(config, object_path(config, path))
  end

  @impl Jido.VFS.Adapter
  def list_contents(%Config{} = config, path) do
    base_path = object_path(config, path)
    list_prefix = if path in ["", "."], do: clear_prefix(config), else: base_path

    list_contents_at_prefix(config, list_prefix)
  end

  defp list_contents_at_prefix(config, list_prefix) do
    with {:ok, %{contents: contents, common_prefixes: prefixes}} <-
           list_objects_paginated(config, list_prefix, "/") do
      directories =
        prefixes
        |> Enum.map(fn %{prefix: prefix} ->
          name =
            prefix
            |> String.trim_trailing("/")
            |> String.split("/")
            |> List.last()

          visibility = get_stored_visibility(config, prefix)

          %Jido.VFS.Stat.Dir{
            name: name,
            size: 0,
            visibility: visibility,
            mtime: DateTime.utc_now()
          }
        end)

      files =
        contents
        |> Enum.reject(fn file -> String.ends_with?(file.key, "/") end)
        |> Enum.map(fn file ->
          name = Path.basename(file.key)
          visibility = get_stored_visibility(config, file.key)

          %Jido.VFS.Stat.File{
            name: name,
            size: parse_s3_size(file.size),
            visibility: visibility,
            mtime: parse_s3_mtime(file.last_modified)
          }
        end)

      {:ok, directories ++ files}
    end
  end

  @impl Jido.VFS.Adapter
  def create_directory(%Config{} = config, path, opts) do
    path = if String.ends_with?(path, "/"), do: path, else: path <> "/"
    write(config, path, "", opts)
  end

  @impl Jido.VFS.Adapter
  def delete_directory(config, path, opts) do
    path = object_path(config, path)
    path = if String.ends_with?(path, "/"), do: path, else: path <> "/"

    if Keyword.get(opts, :recursive, false) do
      case delete_objects_by_prefix(config, path) do
        :ok ->
          delete_stored_visibility_prefix(config, path)
          :ok

        error ->
          error
      end
    else
      case list_contents_at_prefix(config, path) do
        {:ok, []} ->
          case delete_object(config, path) do
            :ok ->
              delete_stored_visibility_prefix(config, path)
              :ok

            error ->
              error
          end

        {:ok, _} ->
          {:error, %Errors.DirectoryNotEmpty{dir_path: path}}

        error ->
          error
      end
    end
  end

  @impl Jido.VFS.Adapter
  def clear(config) do
    path_prefix = clear_prefix(config)

    case delete_objects_by_prefix(config, path_prefix) do
      :ok ->
        delete_stored_visibility_prefix(config, path_prefix)
        :ok

      error ->
        error
    end
  end

  @impl Jido.VFS.Adapter
  def set_visibility(%Config{} = config, path, visibility) do
    normalized_path = object_path(config, path)
    store_visibility(config, normalized_path, visibility)
    :ok
  end

  @impl Jido.VFS.Adapter
  def visibility(%Config{} = config, path) do
    normalized_path = object_path(config, path)
    visibility = get_stored_visibility(config, normalized_path)
    {:ok, visibility}
  end

  # Helper Functions

  defp object_path(%Config{} = config, path) do
    config.prefix
    |> Jido.VFS.RelativePath.join_prefix(path)
    |> clean_path()
  end

  defp clean_path(path) do
    case path do
      "/" ->
        ""

      "." ->
        ""

      path ->
        path
        |> String.trim_leading("/")
        |> String.replace(~r|/+|, "/")
    end
  end

  defp parent_directory_path(path) do
    normalized = String.trim_trailing(path, "/")

    case Path.dirname(normalized) do
      "." -> nil
      "" -> nil
      dirname -> dirname <> "/"
    end
  end

  defp resolve_visibility(table, %Config{} = config, path) do
    key = visibility_key(config, path)

    case :ets.lookup(table, key) do
      [{^key, visibility}] ->
        visibility

      [] ->
        case parent_directory_path(path) do
          nil -> :public
          parent_path -> resolve_visibility(table, config, parent_path)
        end
    end
  end

  defp visibility_key_matches_prefix?(path, path_prefix, normalized_prefix) do
    path == normalized_prefix or String.starts_with?(path, path_prefix)
  end

  defp object_exists(%Config{} = config, path) do
    operation = ExAws.S3.head_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, _} ->
        {:ok, :exists}

      {:error, {:http_error, 404, _}} ->
        {:ok, :missing}

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  defp delete_object(%Config{} = config, path) do
    operation = ExAws.S3.delete_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, _} ->
        delete_stored_visibility(config, path)
        :ok

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  defp stream_chunk_or_raise({:ok, {start_byte, chunk}}) when is_integer(start_byte), do: chunk
  defp stream_chunk_or_raise({:ok, {:ok, {_start_byte, chunk}}}), do: chunk

  defp stream_chunk_or_raise({:ok, {:error, reason}}) do
    raise Errors.AdapterError.exception(adapter: __MODULE__, reason: {:read_stream_chunk_failed, reason})
  end

  defp stream_chunk_or_raise({:exit, reason}) do
    raise Errors.AdapterError.exception(
            adapter: __MODULE__,
            reason: {:read_stream_chunk_task_exit, reason}
          )
  end

  defp stream_chunk_or_raise(result) do
    raise Errors.AdapterError.exception(
            adapter: __MODULE__,
            reason: {:read_stream_invalid_chunk_result, result}
          )
  end

  defp parse_s3_size(size) when is_integer(size), do: size
  defp parse_s3_size(size) when is_binary(size), do: String.to_integer(size)

  defp parse_s3_mtime(timestamp) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, datetime, _offset} -> datetime
      _ -> DateTime.utc_now()
    end
  end

  defp parse_s3_mtime(_), do: DateTime.utc_now()

  defp clear_prefix(%Config{} = config) do
    cleaned_prefix = clean_path(config.prefix || "/")

    cond do
      cleaned_prefix in ["", "."] -> ""
      String.ends_with?(cleaned_prefix, "/") -> cleaned_prefix
      true -> cleaned_prefix <> "/"
    end
  end

  defp delete_objects_by_prefix(%Config{} = config, prefix) do
    with {:ok, %{contents: contents}} <- list_objects_paginated(config, prefix, nil) do
      contents
      |> Enum.map(& &1.key)
      |> Enum.uniq()
      |> Enum.reduce_while(:ok, fn key, :ok ->
        case ExAws.S3.delete_object(config.bucket, key) |> ExAws.request(config.config) do
          {:ok, _} ->
            {:cont, :ok}

          {:error, {:http_error, 404, _}} ->
            {:cont, :ok}

          {:error, error} ->
            {:halt, {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}}
        end
      end)
    end
  end

  defp list_objects_paginated(config, prefix, delimiter) do
    do_list_objects_paginated(config, prefix, delimiter, nil, [], MapSet.new())
  end

  defp do_list_objects_paginated(config, prefix, delimiter, continuation_token, contents_acc, prefixes_acc) do
    opts = [prefix: prefix]
    opts = if delimiter, do: Keyword.put(opts, :delimiter, delimiter), else: opts

    opts =
      if continuation_token,
        do: Keyword.put(opts, :continuation_token, continuation_token),
        else: opts

    operation = ExAws.S3.list_objects_v2(config.bucket, opts)

    case ExAws.request(operation, config.config) do
      {:ok, %{body: body}} ->
        next_contents = contents_acc ++ Map.get(body, :contents, [])

        next_prefixes =
          body
          |> Map.get(:common_prefixes, [])
          |> Enum.reduce(prefixes_acc, fn %{prefix: object_prefix}, acc ->
            MapSet.put(acc, object_prefix)
          end)

        truncated? =
          case Map.get(body, :is_truncated, false) do
            "true" -> true
            true -> true
            _ -> false
          end

        next_token = Map.get(body, :next_continuation_token)

        if truncated? && is_binary(next_token) && next_token != "" do
          do_list_objects_paginated(config, prefix, delimiter, next_token, next_contents, next_prefixes)
        else
          {:ok,
           %{
             contents: next_contents,
             common_prefixes:
               next_prefixes
               |> MapSet.to_list()
               |> Enum.sort()
               |> Enum.map(&%{prefix: &1})
           }}
        end

      {:error, error} ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}

      error ->
        {:error, %Errors.AdapterError{adapter: __MODULE__, reason: error}}
    end
  end

  defp maybe_add_acl(opts) do
    case Keyword.get(opts, :visibility) do
      :public -> Keyword.put(opts, :acl, "public-read")
      :private -> Keyword.put(opts, :acl, "private")
      _ -> opts
    end
  end
end
