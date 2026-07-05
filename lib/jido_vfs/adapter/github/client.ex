defmodule Jido.VFS.Adapter.GitHub.Client do
  @moduledoc false

  defstruct auth: nil, endpoint: "https://api.github.com"

  @type auth :: %{
          optional(:access_token) => String.t(),
          optional(:user) => String.t(),
          optional(:password) => String.t()
        }
  @type t :: %__MODULE__{auth: auth() | nil, endpoint: String.t()}

  @spec new(auth() | nil, keyword()) :: t()
  def new(auth \\ nil, opts \\ []) do
    %__MODULE__{
      auth: auth,
      endpoint: Keyword.get(opts, :endpoint, "https://api.github.com")
    }
  end

  @spec get_content(t(), String.t(), String.t(), String.t(), String.t()) :: response()
  def get_content(%__MODULE__{} = client, owner, repo, path, ref) do
    request(client, :get, contents_path(owner, repo, path), params: [ref: ref])
  end

  @spec put_content(t(), String.t(), String.t(), String.t(), map()) :: response()
  def put_content(%__MODULE__{} = client, owner, repo, path, params) do
    request(client, :put, contents_path(owner, repo, path), json: params)
  end

  @spec delete_content(t(), String.t(), String.t(), String.t(), map()) :: response()
  def delete_content(%__MODULE__{} = client, owner, repo, path, params) do
    request(client, :delete, contents_path(owner, repo, path), json: params)
  end

  @type response :: {non_neg_integer(), term(), map()}

  defp request(client, method, path, opts) do
    client
    |> req()
    |> Req.request(Keyword.merge([method: method, url: path], opts))
    |> case do
      {:ok, %Req.Response{status: status, body: body, headers: headers}} ->
        {status, body, %{headers: headers}}

      {:error, reason} ->
        {0, %{"message" => error_message(reason)}, %{error: reason}}
    end
  end

  defp req(%__MODULE__{} = client) do
    Req.new(
      base_url: client.endpoint,
      headers: headers(client)
    )
  end

  defp headers(%__MODULE__{} = client) do
    [
      {"accept", "application/vnd.github+json"},
      {"x-github-api-version", "2022-11-28"},
      {"user-agent", "jido_vfs"}
    ] ++ auth_headers(client.auth)
  end

  defp auth_headers(nil), do: []

  defp auth_headers(%{access_token: token}) when is_binary(token) do
    [{"authorization", "Bearer #{token}"}]
  end

  defp auth_headers(%{user: user, password: password}) when is_binary(user) and is_binary(password) do
    [{"authorization", "Basic #{Base.encode64("#{user}:#{password}")}"}]
  end

  defp auth_headers(_auth), do: []

  defp contents_path(owner, repo, path) do
    base = "/repos/#{encode_segment(owner)}/#{encode_segment(repo)}/contents"

    case encode_content_path(path) do
      "" -> base
      encoded_path -> "#{base}/#{encoded_path}"
    end
  end

  defp encode_content_path(path) do
    path
    |> String.trim_leading("/")
    |> String.split("/", trim: true)
    |> Enum.map_join("/", &encode_segment/1)
  end

  defp encode_segment(value), do: URI.encode(value, &URI.char_unreserved?/1)

  defp error_message(%{message: message}) when is_binary(message), do: message

  defp error_message(%_{} = exception), do: Exception.message(exception)
end
