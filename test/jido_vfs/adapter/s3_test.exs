defmodule Jido.VFS.Adapter.S3Test do
  use ExUnit.Case
  import Jido.VFS.AdapterTest

  @moduletag :s3
  @minio_available Application.compile_env(:jido_vfs, :minio_available, false)

  if not @minio_available do
    @moduletag skip: "Minio not available"
  end

  setup_all do
    {:ok, raw_config: JidoVfsTest.Minio.config()}
  end

  setup %{raw_config: config} do
    JidoVfsTest.Minio.clean_bucket("default")
    JidoVfsTest.Minio.recreate_bucket("default")

    on_exit(fn ->
      JidoVfsTest.Minio.clean_bucket("default")
    end)

    {:ok, config: config, bucket: "default"}
  end

  adapter_test %{config: config} do
    filesystem = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default")
    {:ok, filesystem: filesystem}
  end

  describe "prefix handling" do
    test "read_stream honors configured prefix", %{config: config} do
      filesystem = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "stream_prefix")
      content = :crypto.strong_rand_bytes(16_384)

      :ok = Jido.VFS.write(filesystem, "file.bin", content)

      assert {:ok, stream} = Jido.VFS.read_stream(filesystem, "file.bin")
      assert Enum.into(stream, <<>>) == content
    end

    test "non-recursive delete_directory honors configured prefix", %{config: config} do
      filesystem_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "delete_a")
      filesystem_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "delete_b")

      :ok = Jido.VFS.create_directory(filesystem_a, "empty/")
      :ok = Jido.VFS.create_directory(filesystem_b, "empty/")

      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_a, "empty/")
      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_b, "empty/")

      assert :ok = Jido.VFS.delete_directory(filesystem_a, "empty/")

      assert {:ok, :missing} = Jido.VFS.file_exists(filesystem_a, "empty/")
      assert {:ok, :exists} = Jido.VFS.file_exists(filesystem_b, "empty/")
    end

    test "root list_contents only returns entries under configured prefix", %{config: config} do
      filesystem_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "list_a")
      filesystem_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "list_b")

      :ok = Jido.VFS.write(filesystem_a, "a.txt", "A")
      :ok = Jido.VFS.write(filesystem_b, "b.txt", "B")

      assert {:ok, contents_a} = Jido.VFS.list_contents(filesystem_a, ".")
      assert {:ok, contents_b} = Jido.VFS.list_contents(filesystem_b, ".")

      assert Enum.map(contents_a, & &1.name) == ["a.txt"]
      assert Enum.map(contents_b, & &1.name) == ["b.txt"]
    end

    test "native copy_between_filesystem honors source and destination prefixes", %{config: config} do
      filesystem_a = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "copy_a")
      filesystem_b = Jido.VFS.Adapter.S3.configure(config: config, bucket: "default", prefix: "copy_b")

      :ok = Jido.VFS.write(filesystem_a, "source.txt", "prefixed copy")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "source.txt"},
                 {filesystem_b, "copied.txt"},
                 copy_between_strategy: :native
               )

      assert {:ok, "prefixed copy"} = Jido.VFS.read(filesystem_b, "copied.txt")
      assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(filesystem_b, "source.txt")
    end
  end

  describe "cross bucket" do
    setup %{config: config} do
      config_b = JidoVfsTest.Minio.config()
      JidoVfsTest.Minio.clean_bucket("secondary")
      JidoVfsTest.Minio.recreate_bucket("secondary")

      on_exit(fn ->
        JidoVfsTest.Minio.clean_bucket("secondary")
      end)

      {:ok, config_a: config, config_b: config_b}
    end

    test "copy", %{config_a: config_a, config_b: config_b} do
      filesystem_a = Jido.VFS.Adapter.S3.configure(config: config_a, bucket: "default")
      filesystem_b = Jido.VFS.Adapter.S3.configure(config: config_b, bucket: "secondary")

      :ok = Jido.VFS.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Jido.VFS.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "other.txt"}
               )

      assert {:ok, "Hello World"} = Jido.VFS.read(filesystem_b, "other.txt")
    end
  end
end
