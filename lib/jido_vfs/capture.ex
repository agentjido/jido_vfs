defmodule Jido.VFS.Capture do
  @moduledoc """
  Immutable filesystem content, independent of storage lifetime and version history.

  Entries use relative paths. Directories and executable file modes are retained.
  Use `Jido.VFS.entries/1` and `Jido.VFS.read/2` to inspect captured content.
  """
  @enforce_keys [:entries]
  defstruct [:entries]
  @type t :: %__MODULE__{entries: map()}

  @doc false
  def from_tree(tree), do: %__MODULE__{entries: flatten(tree, "", %{})}

  defp flatten(tree, prefix, entries) do
    Enum.reduce(tree, entries, fn {name, {content, meta}}, acc ->
      path = if prefix == "", do: name, else: prefix <> "/" <> name

      if is_map(content) do
        flatten(content, path, Map.put(acc, path, %{type: :directory, metadata: meta}))
      else
        Map.put(acc, path, %{type: :file, content: content, metadata: Map.put_new(meta, :mode, :regular)})
      end
    end)
  end

  @doc false
  def to_tree(%__MODULE__{entries: entries}) when is_map(entries) do
    entries
    |> Enum.sort_by(fn {path, _} -> path end)
    |> Enum.reduce(%{}, fn {path, entry}, tree ->
      validate_path!(path)

      {content, meta} =
        case entry do
          %{type: :directory, metadata: meta} when is_map(meta) ->
            {%{}, Map.put_new(meta, :visibility, :private)}

          %{type: :file, content: bytes, metadata: meta} when is_binary(bytes) and is_map(meta) ->
            unless Map.get(meta, :mode, :regular) in [:regular, :executable], do: raise(ArgumentError, "invalid mode")
            {bytes, meta |> Map.put_new(:visibility, :private) |> Map.put_new(:mode, :regular)}

          _ ->
            raise ArgumentError, "invalid entry"
        end

      put(tree, String.split(path, "/"), {content, meta})
    end)
  end

  @doc false
  def edit(%__MODULE__{} = capture, operations) when is_list(operations) do
    entries = Enum.reduce(operations, capture.entries, &edit_entry/2)
    result = %__MODULE__{entries: entries}
    to_tree(result)
    result
  end

  defp edit_entry({:write, path, bytes}, entries), do: edit_entry({:write, path, bytes, []}, entries)

  defp edit_entry({:write, path, bytes, opts}, entries) when is_binary(bytes) and is_list(opts) do
    validate_path!(path)
    if match?(%{type: :directory}, entries[path]), do: raise(ArgumentError, "path is a directory")
    meta = get_in(entries, [path, :metadata]) || %{visibility: :private}
    mode = Keyword.get(opts, :mode, Map.get(meta, :mode, :regular))
    meta = Map.merge(meta, %{mode: mode, mtime: System.system_time(:second)})
    entries |> parents(path) |> Map.put(path, %{type: :file, content: bytes, metadata: meta})
  end

  defp edit_entry({:mkdir, path}, entries) do
    validate_path!(path)
    if match?(%{type: :file}, entries[path]), do: raise(ArgumentError, "path is a file")
    entries |> parents(path) |> Map.put_new(path, %{type: :directory, metadata: %{visibility: :private}})
  end

  defp edit_entry({:delete, path}, entries) do
    validate_path!(path)
    unless Map.has_key?(entries, path), do: raise(ArgumentError, "path is missing")

    if Enum.any?(Map.keys(entries), &String.starts_with?(&1, path <> "/")),
      do: raise(ArgumentError, "directory is not empty")

    Map.delete(entries, path)
  end

  defp edit_entry(_, _), do: raise(ArgumentError, "invalid edit")

  defp parents(entries, path) do
    case Path.dirname(path) do
      "." ->
        entries

      parent ->
        if match?(%{type: :file}, entries[parent]), do: raise(ArgumentError, "parent is a file")
        entries |> Map.put_new(parent, %{type: :directory, metadata: %{visibility: :private}}) |> parents(parent)
    end
  end

  defp validate_path!(path) do
    unless is_binary(path) and Jido.VFS.RelativePath.relative?(path) and
             Enum.all?(String.split(path, "/"), &(&1 not in ["", ".", ".."])),
           do: raise(ArgumentError, "invalid relative path")
  end

  defp put(tree, [name], value), do: Map.put(tree, name, value)

  defp put(tree, [name | rest], value) do
    case Map.fetch(tree, name) do
      {:ok, {children, meta}} when is_map(children) -> Map.put(tree, name, {put(children, rest, value), meta})
      _ -> raise ArgumentError, "missing directory or parent is a file"
    end
  end
end
