defmodule Jido.VFS.Adapter.SpriteFailuresTest do
  use ExUnit.Case, async: true
  alias Jido.VFS.Adapter.Sprite
  alias Jido.VFS.Errors

  defmodule Client do
    def cmd(response, _, _, _), do: response
    def list_checkpoints(responses, _), do: responses.list
    def create_checkpoint(responses, _), do: responses.create
    def restore_checkpoint(responses, _), do: responses.restore
    def delete_checkpoint(responses, _, _), do: responses.delete
  end

  defmodule LegacyClient do
    def list_checkpoints(responses), do: responses.list
    def create_checkpoint(responses), do: responses.create
    def restore_checkpoint(responses, _), do: responses.restore
    def delete_checkpoint(responses, _), do: responses.delete
  end

  defmodule NoCheckpointClient do
    def cmd(_, _, _, _), do: {"", 0}
  end

  defmodule CreationClient do
    def new(result, _), do: result
    def create(result, _), do: result
    def sprite(_, name), do: {:existing, name}
  end

  defmodule NoCreationClient do
    def new(result, _), do: result
    def sprite(_, name), do: {:existing, name}
  end

  test "creation connects to an existing sprite only for an already exists response" do
    for reason <- [
          "already exists",
          %{message: "already exists"},
          %{reason: "already exists"},
          {:error, "already exists"}
        ] do
      assert {Sprite, %{sprite: {:existing, "name"}}} =
               Sprite.configure(
                 client: CreationClient,
                 token: {:error, reason},
                 sprite_name: "name",
                 create_on_demand: true,
                 probe_commands: false
               )
    end

    assert_raise ArgumentError, ~r/unable to create sprite/, fn ->
      Sprite.configure(client: CreationClient, token: {:error, :offline}, sprite_name: "name", create_on_demand: true)
    end

    assert_raise ArgumentError, ~r/create_not_supported/, fn ->
      Sprite.configure(client: NoCreationClient, token: "token", sprite_name: "name", create_on_demand: true)
    end

    assert_raise ArgumentError, ~r/requires :sprite_name/, fn ->
      Sprite.configure(client: CreationClient, token: "token")
    end

    assert {Sprite, %{sprite: {:existing, "name"}}} =
             Sprite.configure(client: CreationClient, token: "token", sprite_name: "name", probe_commands: false)
  end

  test "versioning filters checkpoint dates before it applies the result limit" do
    alias Jido.VFS.Adapter.Sprite.Versioning
    checkpoints = [%{id: "old", timestamp: 1}, %{id: "middle", timestamp: 2}, %{id: "new", timestamp: 3}]
    config = %Sprite.Config{client: Client, sprite: %{list: {:ok, checkpoints}}}

    assert {:ok, [%{sha: "middle"}]} =
             Versioning.revisions(config, ".", since: DateTime.from_unix!(2), until: DateTime.from_unix!(3), limit: -1)

    failed = %{config | sprite: %{list: {:error, :offline}, create: {:error, :offline}}}
    assert {:error, %Errors.AdapterError{}} = Versioning.revisions(failed)
    assert {:error, %Errors.AdapterError{}} = Versioning.commit(failed)
  end

  test "shell failures return specific errors for file and directory operations" do
    for {message, type} <- [
          {"permission denied", Errors.PermissionDenied},
          {"not a directory", Errors.NotDirectory},
          {"is a directory", Errors.InvalidPath},
          {"directory not empty", Errors.DirectoryNotEmpty},
          {"unknown failure", Errors.AdapterError}
        ] do
      config = config({message, 2})
      assert {:error, error} = Sprite.read(config, "file")
      assert error.__struct__ == type
      assert {:error, error} = Sprite.delete(config, "file")
      assert error.__struct__ == type
    end

    assert {:error, %Errors.DirectoryNotFound{}} = Sprite.list_contents(config({"no such file", 2}), "dir")
    assert {:error, %Errors.PermissionDenied{}} = Sprite.file_exists(config({"permission denied", 1}), "file")
    assert {:error, %Errors.AdapterError{}} = Sprite.file_exists(config({"failure", 2}), "file")

    for action <- [&Sprite.read(&1, "file"), &Sprite.delete(&1, "file"), &Sprite.file_exists(&1, "file")] do
      assert {:error, %Errors.AdapterError{}} = action.(config({:error, :offline}))
    end
  end

  test "malformed remote metadata and content are refused" do
    for output <- ["bad", "f\tbad\t1\t644\t/file", "f\t1\tbad\t644\t/file", "f\t1\t1\tbad\t/file"] do
      assert {:error, %Errors.AdapterError{}} = Sprite.list_contents(config({output, 0}), ".")
    end

    for output <- ["bad", "regular file\tbad\t1\t644", "regular file\t1\t1\tbad"] do
      assert {:error, %Errors.AdapterError{}} = Sprite.stat(config({output, 0}), "file")
    end

    assert {:error, %Errors.AdapterError{}} = Sprite.read(config({"not base64!", 0}), "file")
    assert {:ok, :private} = Sprite.visibility(config({"000", 0}), "file")
    assert {:error, %Errors.AdapterError{}} = Sprite.set_visibility(config({"", 0}), "file", :unknown)
    assert {:error, %Errors.AdapterError{}} = Sprite.read_stream(config({"failure", 2}), "file", [])
  end

  test "line streams retain line endings and invalid chunk sizes use the default" do
    config = config({Base.encode64("one\ntwo\n"), 0})
    assert {:ok, ["one\n", "two\n"]} = Sprite.read_stream(config, "file", chunk_size: :line)
    assert {:ok, ["one\ntwo\n"]} = Sprite.read_stream(config, "file", chunk_size: 0)
    assert {:ok, []} = Sprite.read_stream(config({"", 0}), "file", chunk_size: :line)
  end

  test "checkpoint API errors retain the failed operation for current and legacy clients" do
    for client <- [Client, LegacyClient], response <- [{:error, :offline}, :invalid_response] do
      config = %Sprite.Config{
        client: client,
        sprite: %{list: response, create: response, restore: response, delete: response}
      }

      for action <- [
            &Sprite.list_versions(&1, "file"),
            &Sprite.create_checkpoint/1,
            &Sprite.restore_version(&1, "file", "version"),
            &Sprite.delete_version(&1, "file", "version"),
            &Sprite.get_latest_version(&1, "file")
          ] do
        assert {:error, %Errors.AdapterError{}} = action.(config)
      end
    end
  end

  test "clients without checkpoint APIs report unsupported operations" do
    config = %Sprite.Config{client: NoCheckpointClient}
    assert {:error, %Errors.UnsupportedOperation{operation: :list_versions}} = Sprite.list_versions(config, "file")
    assert {:error, %Errors.UnsupportedOperation{operation: :write_version}} = Sprite.create_checkpoint(config)

    assert {:error, %Errors.UnsupportedOperation{operation: :restore_version}} =
             Sprite.restore_version(config, "file", "id")

    assert {:error, %Errors.UnsupportedOperation{operation: :delete_version}} =
             Sprite.delete_version(config, "file", "id")
  end

  test "checkpoint stream errors are detected in lists and lazy streams" do
    for response <- [[%{type: "ERROR", error: "failed"}], Stream.map([%{"type" => "error", "error" => "failed"}], & &1)] do
      config = %Sprite.Config{
        client: Client,
        sprite: %{list: {:ok, []}, create: {:ok, response}, restore: {:ok, response}}
      }

      assert {:error, %Errors.AdapterError{reason: {:checkpoint_stream_error, "failed"}}} =
               Sprite.create_checkpoint(config)

      assert {:error, %Errors.AdapterError{}} = Sprite.restore_version(config, "file", "id")
    end
  end

  test "checkpoint lists accept legacy metadata and sort valid version IDs" do
    checkpoints = [
      %{"id" => "a", "create_time" => "2024-01-01T00:00:00Z"},
      %{id: "b", create_time: ~U[2025-01-01 00:00:00Z]},
      %{"id" => "c", "create_time" => ~U[2026-01-01 00:00:00Z]},
      %{id: "d", timestamp: 1},
      %{"id" => "e", "timestamp" => 2},
      %{id: "f", create_time: "invalid"},
      %{"id" => "g"},
      %{}
    ]

    config = %Sprite.Config{client: LegacyClient, sprite: %{list: checkpoints, restore: {:ok, :done}, delete: :ok}}
    assert {:ok, versions} = Sprite.list_versions(config, "file")
    assert Enum.map(versions, & &1.version_id) == ["c", "b", "a", "e", "d", "f", "g"]
    assert :ok = Sprite.restore_version(config, "file", "a")
    assert :ok = Sprite.delete_version(config, "file", "a")
    config = %{config | sprite: %{list: []}}
    assert {:error, %Errors.FileNotFound{}} = Sprite.get_latest_version(config, "file")

    assert {:error, %Errors.AdapterError{reason: %{reason: :checkpoint_not_visible_after_create}}} =
             Sprite.create_checkpoint(%{config | sprite: %{list: [], create: {:ok, :done}}})
  end

  test "configuration accepts an existing handle and rejects an invalid encoding or client" do
    assert {Sprite, %{root: "/relative", sprite: :handle}} =
             Sprite.configure(sprite: :handle, root: "relative/", probe_commands: false, client_opts: :invalid)

    assert {Sprite, %{root: "/"}} = Sprite.configure(sprite: :handle, root: " ", probe_commands: false)
    assert_raise ArgumentError, fn -> Sprite.configure(sprite: :handle, encoding: :invalid) end
    assert_raise ArgumentError, fn -> Sprite.configure(client: MissingSpriteClient, token: "token") end
  end

  defp config(response), do: %Sprite.Config{client: Client, sprite: response, root: "/workspace"}
end
