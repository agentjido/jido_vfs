defmodule Jido.VFS.ErrorsTest do
  use ExUnit.Case, async: true
  doctest Jido.VFS.Errors, except: [traverse_errors: 2]

  describe "traverse_errors/2" do
    test "groups error messages by path" do
      errors = [
        %Jido.VFS.Errors.InvalidPath{
          invalid_path: "name",
          reason: "is required",
          path: [:name]
        },
        %Jido.VFS.Errors.InvalidPath{
          invalid_path: "user/email",
          reason: "is invalid",
          path: [:user, :email]
        }
      ]

      assert Jido.VFS.Errors.traverse_errors(errors, &Exception.message/1) == %{
               name: ["Invalid path name: is required"],
               user: %{email: ["Invalid path user/email: is invalid"]}
             }
    end
  end

  describe "error class modules" do
    test "Invalid error class" do
      assert %Jido.VFS.Errors.Invalid{} = %Jido.VFS.Errors.Invalid{}
      assert Jido.VFS.Errors.Invalid.error_class?() == true
    end

    test "NotFound error class" do
      assert %Jido.VFS.Errors.NotFound{} = %Jido.VFS.Errors.NotFound{}
      assert Jido.VFS.Errors.NotFound.error_class?() == true
    end

    test "Forbidden error class" do
      assert %Jido.VFS.Errors.Forbidden{} = %Jido.VFS.Errors.Forbidden{}
      assert Jido.VFS.Errors.Forbidden.error_class?() == true
    end

    test "Adapter error class" do
      assert %Jido.VFS.Errors.Adapter{} = %Jido.VFS.Errors.Adapter{}
      assert Jido.VFS.Errors.Adapter.error_class?() == true
    end

    test "Unknown error class" do
      assert %Jido.VFS.Errors.Unknown{} = %Jido.VFS.Errors.Unknown{}
      assert Jido.VFS.Errors.Unknown.error_class?() == true
    end
  end

  describe "Unknown.Unknown error" do
    test "message/1 with binary error" do
      error = %Jido.VFS.Errors.Unknown.Unknown{error: "test error"}
      assert Jido.VFS.Errors.Unknown.Unknown.message(error) == "test error"
    end

    test "message/1 with non-binary error" do
      error = %Jido.VFS.Errors.Unknown.Unknown{error: {:some, :error}}
      assert Jido.VFS.Errors.Unknown.Unknown.message(error) == "{:some, :error}"
    end

    test "message/1 with atom error" do
      error = %Jido.VFS.Errors.Unknown.Unknown{error: :timeout}
      assert Jido.VFS.Errors.Unknown.Unknown.message(error) == ":timeout"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.Unknown.Unknown.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.Unknown.Unknown.splode_error?() == true
    end
  end

  describe "FileNotFound error" do
    test "message/1 formats file path" do
      error = %Jido.VFS.Errors.FileNotFound{file_path: "test.txt"}
      assert Jido.VFS.Errors.FileNotFound.message(error) == "File not found: test.txt"
    end

    test "creates error with file_path field" do
      error = %Jido.VFS.Errors.FileNotFound{file_path: "/path/to/file.txt"}
      assert error.file_path == "/path/to/file.txt"
    end
  end

  describe "DirectoryNotFound error" do
    test "message/1 formats directory path" do
      error = %Jido.VFS.Errors.DirectoryNotFound{dir_path: "some/dir"}
      assert Jido.VFS.Errors.DirectoryNotFound.message(error) == "Directory not found: some/dir"
    end

    test "creates error with dir_path field" do
      error = %Jido.VFS.Errors.DirectoryNotFound{dir_path: "/path/to/dir"}
      assert error.dir_path == "/path/to/dir"
    end
  end

  describe "DirectoryNotEmpty error" do
    test "message/1 formats directory path" do
      error = %Jido.VFS.Errors.DirectoryNotEmpty{dir_path: "some/dir"}
      assert Jido.VFS.Errors.DirectoryNotEmpty.message(error) == "Directory not empty: some/dir"
    end

    test "creates error with dir_path field" do
      error = %Jido.VFS.Errors.DirectoryNotEmpty{dir_path: "/path/to/dir"}
      assert error.dir_path == "/path/to/dir"
    end
  end

  describe "InvalidPath error" do
    test "message/1 formats path and reason" do
      error = %Jido.VFS.Errors.InvalidPath{
        invalid_path: "bad/path",
        reason: "contains invalid chars"
      }

      assert Jido.VFS.Errors.InvalidPath.message(error) ==
               "Invalid path bad/path: contains invalid chars"
    end

    test "creates error with invalid_path and reason fields" do
      error = %Jido.VFS.Errors.InvalidPath{invalid_path: "test/path", reason: "test reason"}
      assert error.invalid_path == "test/path"
      assert error.reason == "test reason"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.InvalidPath.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.InvalidPath.splode_error?() == true
    end
  end

  describe "PermissionDenied error" do
    test "message/1 formats target path and operation" do
      error = %Jido.VFS.Errors.PermissionDenied{target_path: "secret.txt", operation: "read"}

      assert Jido.VFS.Errors.PermissionDenied.message(error) ==
               "Permission denied for read on secret.txt"
    end

    test "creates error with target_path and operation fields" do
      error = %Jido.VFS.Errors.PermissionDenied{target_path: "/secret/file", operation: "write"}
      assert error.target_path == "/secret/file"
      assert error.operation == "write"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.PermissionDenied.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.PermissionDenied.splode_error?() == true
    end
  end

  describe "UnsupportedOperation error" do
    test "message/1 formats operation and adapter" do
      error = %Jido.VFS.Errors.UnsupportedOperation{operation: "symlink", adapter: "InMemory"}

      assert Jido.VFS.Errors.UnsupportedOperation.message(error) ==
               "Operation symlink not supported by adapter InMemory"
    end

    test "creates error with operation and adapter fields" do
      error = %Jido.VFS.Errors.UnsupportedOperation{operation: "test_op", adapter: "TestAdapter"}
      assert error.operation == "test_op"
      assert error.adapter == "TestAdapter"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.UnsupportedOperation.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.UnsupportedOperation.splode_error?() == true
    end
  end

  describe "AdapterError error" do
    test "message/1 formats adapter and reason" do
      error = %Jido.VFS.Errors.AdapterError{adapter: "S3", reason: "connection timeout"}

      assert Jido.VFS.Errors.AdapterError.message(error) ==
               "Adapter S3 error: \"connection timeout\""
    end

    test "message/1 with complex reason" do
      error = %Jido.VFS.Errors.AdapterError{adapter: "Local", reason: {:posix, :enoent}}
      assert Jido.VFS.Errors.AdapterError.message(error) == "Adapter Local error: {:posix, :enoent}"
    end

    test "creates error with adapter and reason fields" do
      error = %Jido.VFS.Errors.AdapterError{adapter: "TestAdapter", reason: "test error"}
      assert error.adapter == "TestAdapter"
      assert error.reason == "test error"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.AdapterError.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.AdapterError.splode_error?() == true
    end
  end

  describe "PathTraversal error" do
    test "message/1 formats attempted path" do
      error = %Jido.VFS.Errors.PathTraversal{attempted_path: "../../../etc/passwd"}

      assert Jido.VFS.Errors.PathTraversal.message(error) ==
               "Path traversal not allowed: ../../../etc/passwd"
    end

    test "creates error with attempted_path field" do
      error = %Jido.VFS.Errors.PathTraversal{attempted_path: "../malicious/path"}
      assert error.attempted_path == "../malicious/path"
    end
  end

  describe "AbsolutePath error" do
    test "message/1 formats absolute path" do
      error = %Jido.VFS.Errors.AbsolutePath{absolute_path: "/absolute/path"}

      assert Jido.VFS.Errors.AbsolutePath.message(error) ==
               "Absolute paths not allowed: /absolute/path"
    end

    test "creates error with absolute_path field" do
      error = %Jido.VFS.Errors.AbsolutePath{absolute_path: "/test/path"}
      assert error.absolute_path == "/test/path"
    end
  end

  describe "NotDirectory error" do
    test "message/1 formats not directory path" do
      error = %Jido.VFS.Errors.NotDirectory{not_dir_path: "file.txt"}
      assert Jido.VFS.Errors.NotDirectory.message(error) == "Path is not a directory: file.txt"
    end

    test "creates error with not_dir_path field" do
      error = %Jido.VFS.Errors.NotDirectory{not_dir_path: "some/file.txt"}
      assert error.not_dir_path == "some/file.txt"
    end

    test "error_class?/0 returns false for error instances" do
      assert Jido.VFS.Errors.NotDirectory.error_class?() == false
    end

    test "splode_error?/0 returns true" do
      assert Jido.VFS.Errors.NotDirectory.splode_error?() == true
    end
  end

  describe "Splode integration" do
    test "main Jido.VFS.Errors module loads properly" do
      # Test that the main module exists and has expected functions
      assert function_exported?(Jido.VFS.Errors, :to_error, 1)
      assert function_exported?(Jido.VFS.Errors, :to_class, 1)
    end

    test "error class constants are defined" do
      # Test that error class modules exist
      assert Code.ensure_loaded?(Jido.VFS.Errors.Invalid)
      assert Code.ensure_loaded?(Jido.VFS.Errors.NotFound)
      assert Code.ensure_loaded?(Jido.VFS.Errors.Forbidden)
      assert Code.ensure_loaded?(Jido.VFS.Errors.Adapter)
      assert Code.ensure_loaded?(Jido.VFS.Errors.Unknown)
    end
  end
end
