defmodule Jido.VFS.Adapter.GitHubTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Jido.VFS.Adapter.GitHub
  alias Jido.VFS.Adapter.GitHub.Client
  alias Jido.VFS.Adapter.InMemory
  alias Jido.VFS.Stat.{File, Dir}

  @moduletag :github

  setup :copy_modules

  defp copy_modules(_context) do
    Mimic.copy(Client)
    :ok
  end

  describe "configure/1" do
    test "creates config with required options" do
      {module, config} =
        GitHub.configure(
          owner: "octocat",
          repo: "Hello-World"
        )

      assert module == GitHub

      assert %GitHub{
               owner: "octocat",
               repo: "Hello-World",
               ref: "main",
               client: %Client{auth: nil}
             } = config
    end

    test "creates config with auth token" do
      {module, config} =
        GitHub.configure(
          owner: "octocat",
          repo: "Hello-World",
          ref: "develop",
          auth: %{access_token: "test_token"}
        )

      assert module == GitHub

      assert %GitHub{
               owner: "octocat",
               repo: "Hello-World",
               ref: "develop",
               client: %Client{auth: %{access_token: "test_token"}}
             } = config
    end

    test "creates config with custom commit info" do
      commit_info = %{
        message: "Custom commit",
        committer: %{name: "Test User", email: "test@example.com"},
        author: %{name: "Test Author", email: "author@example.com"}
      }

      {_module, config} =
        GitHub.configure(
          owner: "octocat",
          repo: "Hello-World",
          commit_info: commit_info
        )

      assert config.commit_info == commit_info
    end

    test "uses :jido_vfs config namespace and ignores :hako" do
      previous_jido = Application.get_env(:jido_vfs, :github)
      previous_hako = Application.get_env(:hako, :github)

      on_exit(fn ->
        if previous_jido do
          Application.put_env(:jido_vfs, :github, previous_jido)
        else
          Application.delete_env(:jido_vfs, :github)
        end

        if previous_hako do
          Application.put_env(:hako, :github, previous_hako)
        else
          Application.delete_env(:hako, :github)
        end
      end)

      Application.put_env(:jido_vfs, :github,
        access_token: "jido_token",
        name: "Jido Name",
        email: "jido@example.com"
      )

      Application.put_env(:hako, :github,
        access_token: "legacy_token",
        name: "Legacy Name",
        email: "legacy@example.com"
      )

      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")

      assert config.client.auth == %{access_token: "jido_token"}
      assert config.commit_info.author == %{name: "Jido Name", email: "jido@example.com"}
      assert config.commit_info.committer == %{name: "Jido Name", email: "jido@example.com"}
    end
  end

  describe "read/2" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "reads file content successfully", %{config: config} do
      content = "Hello, World!"
      encoded_content = Base.encode64(content)

      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "README.md", "main" ->
        {200, %{"content" => encoded_content, "encoding" => "base64"}, %{}}
      end)

      assert {:ok, ^content} = GitHub.read(config, "README.md")
    end

    test "returns error for missing file", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "missing.txt", "main" ->
        {404, %{"message" => "Not Found"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.FileNotFound{file_path: "missing.txt"}} =
               GitHub.read(config, "missing.txt")
    end

    test "returns error for API errors", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "error.txt", "main" ->
        {500, %{"message" => "Server Error"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.read(config, "error.txt")
    end

    test "handles malformed base64 content", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "malformed.txt", "main" ->
        {200, %{"content" => "invalid-base64!", "encoding" => "base64"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.read(config, "malformed.txt")
    end
  end

  describe "write/3" do
    setup do
      {_module, config} =
        GitHub.configure(
          owner: "octocat",
          repo: "Hello-World",
          commit_info: %{
            message: "Test commit",
            committer: %{name: "Test", email: "test@example.com"},
            author: %{name: "Test", email: "test@example.com"}
          }
        )

      {:ok, config: config}
    end

    test "creates new file", %{config: config} do
      content = "New file content"

      # File doesn't exist
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "new.txt", "main" ->
        {404, %{"message" => "Not Found"}, %{}}
      end)

      # Create file
      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "new.txt", params ->
        assert params.message == "Test commit"
        assert params.content == Base.encode64(content)
        assert params.committer.name == "Test"
        refute Map.has_key?(params, :sha)

        {201, %{"commit" => %{"sha" => "abc123"}}, %{}}
      end)

      assert :ok = GitHub.write(config, "new.txt", content, [])
    end

    test "updates existing file", %{config: config} do
      content = "Updated content"
      existing_sha = "existing_sha_123"

      # File exists
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "existing.txt", "main" ->
        {200, %{"sha" => existing_sha}, %{}}
      end)

      # Update file
      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "existing.txt", params ->
        assert params.sha == existing_sha
        assert params.content == Base.encode64(content)

        {200, %{"commit" => %{"sha" => "def456"}}, %{}}
      end)

      assert :ok = GitHub.write(config, "existing.txt", content, [])
    end

    test "uses custom commit options", %{config: config} do
      content = "Content with custom commit"

      expect(Client, :get_content, fn _, _, _, _, _ ->
        {404, %{}, %{}}
      end)

      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "custom.txt", params ->
        assert params.message == "Custom message"
        assert params.committer == %{name: "Custom", email: "custom@example.com"}

        {201, %{}, %{}}
      end)

      opts = [
        message: "Custom message",
        committer: %{name: "Custom", email: "custom@example.com"}
      ]

      assert :ok = GitHub.write(config, "custom.txt", content, opts)
    end

    test "returns error for write API failures", %{config: config} do
      content = "Content that will fail"

      expect(Client, :get_content, fn _, _, _, _, _ ->
        {404, %{}, %{}}
      end)

      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "fail.txt", _params ->
        {422, %{"message" => "Validation Failed"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.write(config, "fail.txt", content, [])
    end

    test "handles file lookup errors gracefully by treating as new file", %{config: config} do
      content = "Content for file with lookup issues"

      # File lookup fails (treated as file doesn't exist)
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "lookup_error.txt", "main" ->
        {500, %{"message" => "Server Error"}, %{}}
      end)

      # Create file (since lookup failed, sha is nil)
      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "lookup_error.txt", params ->
        # No sha means new file
        refute Map.has_key?(params, :sha)
        {201, %{"commit" => %{"sha" => "abc123"}}, %{}}
      end)

      assert :ok = GitHub.write(config, "lookup_error.txt", content, [])
    end
  end

  describe "delete/2" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "deletes existing file", %{config: config} do
      file_sha = "file_sha_123"

      # File exists
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "delete_me.txt", "main" ->
        {200, %{"sha" => file_sha}, %{}}
      end)

      # Delete file
      expect(Client, :delete_content, fn _client, "octocat", "Hello-World", "delete_me.txt", params ->
        assert params.sha == file_sha
        assert params.message == "Delete delete_me.txt via Jido.VFS"

        {200, %{"commit" => %{"sha" => "ghi789"}}, %{}}
      end)

      assert :ok = GitHub.delete(config, "delete_me.txt")
    end

    test "returns error for missing file", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "missing.txt", "main" ->
        {404, %{"message" => "Not Found"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.FileNotFound{file_path: "missing.txt"}} =
               GitHub.delete(config, "missing.txt")
    end

    test "returns error for delete API failures", %{config: config} do
      file_sha = "file_sha_123"

      # File exists
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "error_delete.txt", "main" ->
        {200, %{"sha" => file_sha}, %{}}
      end)

      # Delete fails
      expect(Client, :delete_content, fn _client, "octocat", "Hello-World", "error_delete.txt", _params ->
        {422, %{"message" => "Validation Failed"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.delete(config, "error_delete.txt")
    end

    test "returns error when file lookup for delete fails", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "lookup_error.txt", "main" ->
        {500, %{"message" => "Server Error"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.delete(config, "lookup_error.txt")
    end
  end

  describe "list_contents/2" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "lists directory contents", %{config: config} do
      contents = [
        %{"type" => "file", "name" => "README.md", "size" => 1024},
        %{"type" => "dir", "name" => "src"},
        %{"type" => "file", "name" => "package.json", "size" => 512}
      ]

      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "", "main" ->
        {200, contents, %{}}
      end)

      assert {:ok, stats} = GitHub.list_contents(config, "")

      assert length(stats) == 3

      assert %File{name: "README.md", size: 1024} = Enum.find(stats, &(&1.name == "README.md"))
      assert %Dir{name: "src"} = Enum.find(stats, &(&1.name == "src"))

      assert %File{name: "package.json", size: 512} =
               Enum.find(stats, &(&1.name == "package.json"))
    end

    test "handles single file response", %{config: config} do
      file_content = %{"type" => "file", "name" => "single.txt", "size" => 256}

      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "single.txt", "main" ->
        {200, file_content, %{}}
      end)

      assert {:ok, [%File{name: "single.txt", size: 256}]} =
               GitHub.list_contents(config, "single.txt")
    end

    test "normalizes path for subdirectories", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "src/lib", "main" ->
        {200, [], %{}}
      end)

      assert {:ok, []} = GitHub.list_contents(config, "/src/lib")
    end

    test "returns error for missing directory", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "missing", "main" ->
        {404, %{"message" => "Not Found"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.DirectoryNotFound{dir_path: "missing"}} =
               GitHub.list_contents(config, "missing")
    end

    test "returns error for API failures", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "error_dir", "main" ->
        {500, %{"message" => "Server Error"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.list_contents(config, "error_dir")
    end
  end

  describe "file_exists?/2" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "returns true for existing file", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "exists.txt", "main" ->
        {200, %{"type" => "file"}, %{}}
      end)

      assert {:ok, :exists} = GitHub.file_exists(config, "exists.txt")
    end

    test "returns false for directory", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "directory", "main" ->
        {200, %{"type" => "dir"}, %{}}
      end)

      assert {:ok, :missing} = GitHub.file_exists(config, "directory")
    end

    test "returns false for missing file", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "missing.txt", "main" ->
        {404, %{}, %{}}
      end)

      assert {:ok, :missing} = GitHub.file_exists(config, "missing.txt")
    end

    test "returns error for API failures", %{config: config} do
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "error.txt", "main" ->
        {500, %{"message" => "Server Error"}, %{}}
      end)

      assert {:error, %Jido.VFS.Errors.AdapterError{}} = GitHub.file_exists(config, "error.txt")
    end
  end

  describe "copy/3" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "copies file by reading and writing", %{config: config} do
      content = "File to copy"

      # Read source
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "source.txt", "main" ->
        {200, %{"content" => Base.encode64(content), "encoding" => "base64"}, %{}}
      end)

      # Check if destination exists (doesn't)
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "dest.txt", "main" ->
        {404, %{}, %{}}
      end)

      # Write destination
      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "dest.txt", _params ->
        {201, %{}, %{}}
      end)

      assert :ok = GitHub.copy(config, "source.txt", "dest.txt", [])
    end
  end

  describe "move/3" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "moves file by copying and deleting", %{config: config} do
      content = "File to move"
      source_sha = "source_sha_123"

      # Read source
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "source.txt", "main" ->
        {200, %{"content" => Base.encode64(content), "encoding" => "base64"}, %{}}
      end)

      # Check if destination exists (doesn't)
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "dest.txt", "main" ->
        {404, %{}, %{}}
      end)

      # Write destination
      expect(Client, :put_content, fn _client, "octocat", "Hello-World", "dest.txt", _params ->
        {201, %{}, %{}}
      end)

      # Get source for deletion
      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "source.txt", "main" ->
        {200, %{"sha" => source_sha}, %{}}
      end)

      # Delete source
      expect(Client, :delete_content, fn _client, "octocat", "Hello-World", "source.txt", _params ->
        {200, %{}, %{}}
      end)

      assert :ok = GitHub.move(config, "source.txt", "dest.txt", [])
    end
  end

  describe "copy_between_filesystem/3 fallback behavior" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "falls back to read/write when streaming is unsupported", %{config: config} do
      destination_fs = InMemory.configure(name: :"github_copy_#{System.unique_integer([:positive])}")
      start_supervised!(destination_fs)

      content = "fallback copy content"

      expect(Client, :get_content, fn _client, "octocat", "Hello-World", "source.txt", "main" ->
        {200, %{"content" => Base.encode64(content), "encoding" => "base64"}, %{}}
      end)

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {{GitHub, config}, "source.txt"},
                 {destination_fs, "dest.txt"}
               )

      assert {:ok, ^content} = Jido.VFS.read(destination_fs, "dest.txt")
    end
  end

  describe "unsupported operations" do
    setup do
      {_module, config} = GitHub.configure(owner: "octocat", repo: "Hello-World")
      {:ok, config: config}
    end

    test "create_directory returns error", %{config: config} do
      assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :create_directory}} =
               GitHub.create_directory(config, "new_dir", [])
    end

    test "delete_directory returns error", %{config: config} do
      assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :delete_directory}} =
               GitHub.delete_directory(config, "some_dir", [])
    end
  end
end
