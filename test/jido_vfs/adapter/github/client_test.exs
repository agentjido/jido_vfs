defmodule Jido.VFS.Adapter.GitHub.ClientTest do
  use ExUnit.Case, async: true
  use Mimic
  alias Jido.VFS.Adapter.GitHub.Client

  setup do
    Mimic.copy(Req)
    :ok
  end

  test "content requests encode each path segment and preserve the branch name" do
    expect(Req, :request, fn request, options ->
      assert request.options.base_url == "http://example.test"
      assert request.headers["accept"] == ["application/vnd.github+json"]
      assert request.headers["x-github-api-version"] == ["2022-11-28"]
      assert request.headers["user-agent"] == ["jido_vfs"]
      refute Map.has_key?(request.headers, "authorization")
      assert options[:method] == :get
      assert options[:url] == "/repos/an%20owner/a%2Frepo/contents/a%20file/%23name"
      assert options[:params] == [ref: "feature/a"]
      {:ok, %Req.Response{status: 200, body: %{"content" => "data"}, headers: %{"etag" => ["revision"]}}}
    end)

    assert {200, %{"content" => "data"}, %{headers: %{"etag" => ["revision"]}}} =
             Client.get_content(
               Client.new(nil, endpoint: "http://example.test"),
               "an owner",
               "a/repo",
               "/a file//#name",
               "feature/a"
             )
  end

  test "writes use bearer authentication and return API errors unchanged" do
    params = %{message: "write", content: Base.encode64("bytes")}

    expect(Req, :request, fn request, options ->
      assert request.headers["authorization"] == ["Bearer secret"]
      assert options[:method] == :put
      assert options[:json] == params
      assert options[:url] == "/repos/owner/repo/contents/file"
      {:ok, %Req.Response{status: 409, body: %{"message" => "conflict"}}}
    end)

    assert {409, %{"message" => "conflict"}, _} =
             Client.put_content(Client.new(%{access_token: "secret"}), "owner", "repo", "file", params)
  end

  test "deletes use basic authentication and retain the expected revision" do
    params = %{sha: "expected", message: "delete"}

    expect(Req, :request, fn request, options ->
      assert request.headers["authorization"] == ["Basic " <> Base.encode64("user:password")]
      assert options[:method] == :delete
      assert options[:json] == params
      {:ok, %Req.Response{status: 200, body: %{}}}
    end)

    assert {200, %{}, _} =
             Client.delete_content(Client.new(%{user: "user", password: "password"}), "owner", "repo", "file", params)
  end

  test "root requests omit the trailing slash and unknown auth adds no credentials" do
    expect(Req, :request, fn request, options ->
      refute Map.has_key?(request.headers, "authorization")
      assert options[:url] == "/repos/owner/repo/contents"
      {:error, %{message: "connection failed"}}
    end)

    assert {0, %{"message" => "connection failed"}, %{error: %{message: "connection failed"}}} =
             Client.get_content(Client.new(%{unsupported: true}), "owner", "repo", "/", "main")
  end

  test "transport exceptions retain the reason and provide a readable message" do
    reason = %Req.TransportError{reason: :econnrefused}
    expect(Req, :request, fn _, _ -> {:error, reason} end)

    assert {0, %{"message" => message}, %{error: ^reason}} =
             Client.get_content(Client.new(), "owner", "repo", "", "main")

    assert message == Exception.message(reason)
  end
end
