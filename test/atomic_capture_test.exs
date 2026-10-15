defmodule Jido.VFS.AtomicCaptureTest do
  use ExUnit.Case, async: true
  alias Jido.VFS
  alias Jido.VFS.Adapter.InMemory

  setup do
    fs = InMemory.configure(name: make_ref())
    pid = start_supervised!({InMemory, fs})
    %{fs: fs, pid: pid}
  end

  test "capture retains files, modes and empty directories after storage stops", %{fs: fs, pid: pid} do
    {:ok, _, token} = VFS.capture(fs)
    assert {:ok, _} = VFS.apply_edits(fs, token, [{:mkdir, "empty"}, {:write, "bin/run", "run", mode: :executable}])
    {:ok, capture, _} = VFS.capture(fs)
    :ok = VFS.write(fs, "bin/run", "new")
    {:ok, current, _} = VFS.capture(fs)
    assert current.entries["bin/run"].metadata.mode == :executable
    GenServer.stop(pid)
    assert {:ok, "run"} = VFS.read(capture, "bin/run")
    assert capture.entries["empty"].type == :directory
    assert capture.entries["bin/run"].metadata.mode == :executable
  end

  test "invalid edits and stale tokens leave every file unchanged", %{fs: fs} do
    :ok = VFS.write(fs, "a", "0")
    {:ok, before, token} = VFS.capture(fs)
    assert {:error, _} = VFS.apply_edits(fs, token, [{:write, "a", "1"}, {:write, "a/child", "bad"}])
    assert {:ok, ^before, ^token} = VFS.capture(fs)
    assert {:ok, next} = VFS.apply_edits(fs, token, [{:write, "a", "2"}])
    assert {:error, %VFS.Errors.StaleState{}} = VFS.replace(fs, token, before)
    assert {:ok, restored} = VFS.replace(fs, next, before)
    refute restored == token
    assert {:ok, ^before, ^restored} = VFS.capture(fs)
  end

  test "simultaneous checked batches have one winner", %{fs: fs} do
    {:ok, _, token} = VFS.capture(fs)
    owner = self()

    tasks =
      for n <- [1, 2] do
        Task.async(fn ->
          send(owner, {:ready, self()})

          receive do
            :go -> VFS.apply_edits(fs, token, [{:write, "a", "#{n}"}, {:write, "b", "#{n}"}])
          end
        end)
      end

    for task <- tasks do
      pid = task.pid
      assert_receive {:ready, ^pid}
    end

    for task <- tasks, do: send(task.pid, :go)
    results = Enum.map(tasks, &Task.await/1)
    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, %VFS.Errors.StaleState{}}, &1)) == 1
    {:ok, capture, _} = VFS.capture(fs)
    assert VFS.read(capture, "a") == VFS.read(capture, "b")
  end

  test "ordinary mutations invalidate tokens and unsupported adapters refuse capture", %{fs: fs} do
    for mutation <- [
          fn -> VFS.write(fs, "a", "data") end,
          fn -> VFS.append(fs, "a", "more") end,
          fn -> VFS.truncate(fs, "a", 1) end,
          fn -> VFS.create_directory(fs, "empty/") end,
          fn -> VFS.delete(fs, "a") end,
          fn -> VFS.clear(fs) end
        ] do
      {:ok, _, token} = VFS.capture(fs)
      assert :ok = mutation.()
      assert {:error, %VFS.Errors.StaleState{}} = VFS.apply_edits(fs, token, [])
    end

    local = VFS.Adapter.Local.configure(prefix: System.tmp_dir!())
    assert {:error, %VFS.Errors.UnsupportedOperation{}} = VFS.capture(local)
  end

  test "stream only publishes at completion", %{fs: fs} do
    :ok = VFS.write(fs, "a", "start")
    {:ok, _, token} = VFS.capture(fs)
    {:ok, stream} = VFS.write_stream(fs, "a")
    {acc, collect} = Collectable.into(stream)
    acc = collect.(acc, {:cont, "end"})
    assert {:ok, "start"} = VFS.read(fs, "a")
    assert {:ok, _, ^token} = VFS.capture(fs)
    collect.(acc, :done)
    assert {:ok, "startend"} = VFS.read(fs, "a")
    assert {:error, %VFS.Errors.StaleState{}} = VFS.apply_edits(fs, token, [])
  end

  test "tokens cannot cross instances or survive a storage restart", %{fs: fs} do
    {:ok, capture, token} = VFS.capture(fs)
    other = InMemory.configure(name: make_ref())
    {:ok, other_pid} = InMemory.start_link(other)

    try do
      assert {:error, %VFS.Errors.StaleState{}} = VFS.replace(other, token, capture)
      stop_supervised!(InMemory)
      {:ok, restarted} = InMemory.start_link(fs)

      try do
        assert {:error, %VFS.Errors.StaleState{}} = VFS.apply_edits(fs, token, [])
      after
        GenServer.stop(restarted)
      end
    after
      GenServer.stop(other_pid)
    end
  end

  test "checked edits and imported metadata keep stat and list usable", %{fs: fs, pid: pid} do
    {:ok, _, token} = VFS.capture(fs)
    {:ok, token} = VFS.apply_edits(fs, token, [{:write, "a", "x"}])
    assert {:ok, _} = VFS.stat(fs, "")
    capture = %VFS.Capture{entries: %{"a" => %{type: :file, content: "x", metadata: %{}}}}
    assert {:ok, _} = VFS.replace(fs, token, capture)
    assert {:ok, _} = VFS.stat(fs, "a")
    assert {:ok, [_]} = VFS.list_contents(fs, "")
    assert Process.alive?(pid)
  end

  test "failed ordinary mutations report errors and preserve state", %{fs: fs} do
    :ok = VFS.write(fs, "a", "x")
    {:ok, capture, token} = VFS.capture(fs)
    assert {:error, _} = VFS.create_directory(fs, "a/child/")
    assert {:error, _} = VFS.delete(fs, "a/child")
    assert {:ok, ^capture, ^token} = VFS.capture(fs)
  end

  test "all accepted memory names survive capture and replacement", %{fs: fs} do
    for name <- ["ab:c", "a\\b", "nul" <> <<0>>, <<255>>] do
      assert :ok = VFS.write(fs, name, "x")
    end

    {:ok, capture, token} = VFS.capture(fs)
    assert {:ok, token} = VFS.replace(fs, token, capture)
    assert {:ok, _} = VFS.apply_edits(fs, token, [{:write, "new", "ok"}])
    for {name, _} <- capture.entries, do: assert(VFS.read(fs, name) == {:ok, "x"})
  end

  test "checked edits reject absolute names without changing state", %{fs: fs} do
    {:ok, before, token} = VFS.capture(fs)

    for path <- ["/absolute", "C:foo", "C:/foo", "\\foo", "\\\\server\\file"] do
      assert {:error, _} = VFS.apply_edits(fs, token, [{:write, path, "bad"}])
      assert {:ok, ^before, ^token} = VFS.capture(fs)
    end
  end
end
