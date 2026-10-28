defmodule Jido.VFS.AdapterTest do
  @moduledoc false
  defmacro in_list(list, match) do
    quote do
      Enum.any?(unquote(list), &match?(unquote(match), &1))
    end
  end

  defp tests do
    quote do
      test "user can write to filesystem", %{filesystem: filesystem} do
        assert :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
      end

      test "user can overwrite a file on the filesystem", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Old text")
        assert :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
      end

      test "user can stream to a filesystem", %{filesystem: filesystem} do
        case Jido.VFS.write_stream(filesystem, "test.txt") do
          {:ok, stream} ->
            Enum.into(["Hello", " ", "World"], stream)

            assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")

          {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :write_stream}} ->
            :ok
        end
      end

      test "user can check if files exist on a filesystem", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

        assert {:ok, :exists} = Jido.VFS.file_exists(filesystem, "test.txt")
        assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "not-test.txt")
      end

      test "user can read from filesystem", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

        assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
      end

      test "user can stream from filesystem", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

        case Jido.VFS.read_stream(filesystem, "test.txt") do
          {:ok, stream} ->
            assert Enum.into(stream, <<>>) == "Hello World"

          {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :read_stream}} ->
            :ok
        end
      end

      test "user can stream in a certain chunk size", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")

        case Jido.VFS.read_stream(filesystem, "test.txt", chunk_size: 2) do
          {:ok, stream} ->
            assert ["He" | _] = Enum.into(stream, [])

          {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :read_stream}} ->
            :ok
        end
      end

      test "path traversal is rejected with typed errors", %{filesystem: filesystem} do
        assert {:error, %Jido.VFS.Errors.PathTraversal{}} = Jido.VFS.read(filesystem, "../escape.txt")
      end

      test "absolute paths are rejected with typed errors", %{filesystem: filesystem} do
        assert {:error, %Jido.VFS.Errors.AbsolutePath{}} = Jido.VFS.read(filesystem, "/escape.txt")
      end

      test "supports?/2 drives typed unsupported responses", %{filesystem: filesystem} do
        unless Jido.VFS.supports?(filesystem, :stat) do
          assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :stat}} =
                   Jido.VFS.stat(filesystem, "test.txt")
        end

        unless Jido.VFS.supports?(filesystem, :access) do
          assert {:error, %Jido.VFS.Errors.UnsupportedOperation{operation: :access}} =
                   Jido.VFS.access(filesystem, "test.txt", [:read])
        end
      end

      test "user can try to read a non-existing file from filesystem", %{filesystem: filesystem} do
        assert {:error, %Jido.VFS.Errors.FileNotFound{}} = Jido.VFS.read(filesystem, "test.txt")
      end

      test "user can delete from filesystem", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        :ok = Jido.VFS.delete(filesystem, "test.txt")

        assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
      end

      test "user can delete a non-existing file from filesystem", %{filesystem: filesystem} do
        assert :ok = Jido.VFS.delete(filesystem, "test.txt")
      end

      test "user can move files", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        :ok = Jido.VFS.move(filesystem, "test.txt", "not-test.txt")

        assert {:error, _} = Jido.VFS.read(filesystem, "test.txt")
        assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
      end

      test "user can try to move a non-existing file", %{filesystem: filesystem} do
        assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
                 Jido.VFS.move(filesystem, "test.txt", "not-test.txt")
      end

      test "user can copy files", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        :ok = Jido.VFS.copy(filesystem, "test.txt", "not-test.txt")

        assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "test.txt")
        assert {:ok, "Hello World"} = Jido.VFS.read(filesystem, "not-test.txt")
      end

      test "user can try to copy a non-existing file", %{filesystem: filesystem} do
        assert {:error, %Jido.VFS.Errors.FileNotFound{}} =
                 Jido.VFS.copy(filesystem, "test.txt", "not-test.txt")
      end

      test "user can list files and folders", %{filesystem: filesystem} do
        :ok = Jido.VFS.create_directory(filesystem, "test/")
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        :ok = Jido.VFS.write(filesystem, "test-1.txt", "Hello World")
        :ok = Jido.VFS.write(filesystem, "folder/test-1.txt", "Hello World")

        {:ok, list} = Jido.VFS.list_contents(filesystem, ".")

        assert in_list(list, %Jido.VFS.Stat.Dir{name: "test"})
        assert in_list(list, %Jido.VFS.Stat.Dir{name: "folder"})
        assert in_list(list, %Jido.VFS.Stat.File{name: "test.txt"})
        assert in_list(list, %Jido.VFS.Stat.File{name: "test-1.txt"})

        refute in_list(list, %Jido.VFS.Stat.File{name: "folder/test-1.txt"})

        assert length(list) == 4
      end

      test "directory listings include visibility", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "visible.txt", "Hello World", visibility: :public)
        :ok = Jido.VFS.write(filesystem, "invisible.txt", "Hello World", visibility: :private)
        :ok = Jido.VFS.create_directory(filesystem, "visible-dir/", directory_visibility: :public)
        :ok = Jido.VFS.create_directory(filesystem, "invisible-dir/", directory_visibility: :private)

        {:ok, list} = Jido.VFS.list_contents(filesystem, ".")

        assert in_list(list, %Jido.VFS.Stat.Dir{name: "visible-dir", visibility: :public})
        assert in_list(list, %Jido.VFS.Stat.Dir{name: "invisible-dir", visibility: :private})
        assert in_list(list, %Jido.VFS.Stat.File{name: "visible.txt", visibility: :public})
        assert in_list(list, %Jido.VFS.Stat.File{name: "invisible.txt", visibility: :private})

        assert length(list) == 4
      end

      test "user can create directories", %{filesystem: filesystem} do
        assert :ok = Jido.VFS.create_directory(filesystem, "test/")
        assert :ok = Jido.VFS.create_directory(filesystem, "test/nested/folder/")
      end

      test "user can delete directories", %{filesystem: filesystem} do
        :ok = Jido.VFS.create_directory(filesystem, "test/")
        assert :ok = Jido.VFS.delete_directory(filesystem, "test/")
      end

      test "non empty directories are not deleted by default", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test/test.txt", "Hello World")
        assert {:error, _} = Jido.VFS.delete_directory(filesystem, "test/")
      end

      test "non empty directories are deleted with the recursive flag set", %{
        filesystem: filesystem
      } do
        :ok = Jido.VFS.write(filesystem, "test/test.txt", "Hello World")
        assert :ok = Jido.VFS.delete_directory(filesystem, "test/", recursive: true)

        :ok = Jido.VFS.create_directory(filesystem, "test/nested/folder/")
        assert :ok = Jido.VFS.delete_directory(filesystem, "test/", recursive: true)
      end

      test "files in deleted directories are no longer available", %{filesystem: filesystem} do
        :ok = Jido.VFS.write(filesystem, "test/test.txt", "Hello World")
        assert :ok = Jido.VFS.delete_directory(filesystem, "test/", recursive: true)
        assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "not-test.txt")
      end

      test "non filesystem can be cleared", %{
        filesystem: filesystem
      } do
        :ok = Jido.VFS.write(filesystem, "test.txt", "Hello World")
        :ok = Jido.VFS.write(filesystem, "test/test.txt", "Hello World")
        :ok = Jido.VFS.create_directory(filesystem, "test/nested/folder/")

        assert :ok = Jido.VFS.clear(filesystem)

        assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "test.txt")
        assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "test/test.txt")
        assert {:ok, :missing} = Jido.VFS.file_exists(filesystem, "test/")
      end

      test "set visibility", %{filesystem: filesystem} do
        :ok =
          Jido.VFS.write(filesystem, "folder/file.txt", "Hello World",
            visibility: :public,
            directory_visibility: :public
          )

        assert :ok = Jido.VFS.set_visibility(filesystem, "folder/", :private)
        assert {:ok, :private} = Jido.VFS.visibility(filesystem, "folder/")

        assert :ok = Jido.VFS.set_visibility(filesystem, "folder/file.txt", :private)
        assert {:ok, :private} = Jido.VFS.visibility(filesystem, "folder/file.txt")
      end

      test "visibility", %{filesystem: filesystem} do
        :ok =
          Jido.VFS.write(filesystem, "public/file.txt", "Hello World",
            visibility: :private,
            directory_visibility: :public
          )

        :ok =
          Jido.VFS.write(filesystem, "private/file.txt", "Hello World",
            visibility: :public,
            directory_visibility: :private
          )

        assert {:ok, :public} = Jido.VFS.visibility(filesystem, ".")
        assert {:ok, :public} = Jido.VFS.visibility(filesystem, "public/")
        assert {:ok, :private} = Jido.VFS.visibility(filesystem, "public/file.txt")
        assert {:ok, :private} = Jido.VFS.visibility(filesystem, "private/")
        assert {:ok, :public} = Jido.VFS.visibility(filesystem, "private/file.txt")
      end
    end
  end

  defmacro adapter_test(block) do
    quote do
      describe "common adapter tests" do
        setup unquote(block)

        import Jido.VFS.AdapterTest, only: [in_list: 2]
        unquote(tests())
      end
    end
  end

  defmacro adapter_test(context, block) do
    quote do
      describe "common adapter tests" do
        setup unquote(context), unquote(block)

        import Jido.VFS.AdapterTest, only: [in_list: 2]
        unquote(tests())
      end
    end
  end
end
