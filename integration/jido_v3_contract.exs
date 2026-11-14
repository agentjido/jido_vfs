# Run from a consumer that has both consolidated Jido and this VFS candidate:
# MIX_ENV=test mix run /path/to/jido_vfs/integration/jido_v3_contract.exs
ExUnit.start()

defmodule VfsV3GuideContractTest do
  use ExUnit.Case, async: true

  test "the published Action wrapper runs with core Joi and VFS" do
    guide = File.read!(Path.expand("../docs/jido-agent-integration.md", __DIR__))
    [_, example] = Regex.run(~r/```elixir\n(defmodule MyApp.Actions.ReadFile.*?)```/s, guide)
    Code.compile_string(example)
    directory = Path.join(System.tmp_dir!(), "vfs-v3-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    filesystem = Jido.VFS.Adapter.Local.configure(prefix: directory)
    assert :ok = Jido.VFS.write(filesystem, "input.txt", "v3 bytes")

    assert {:ok, %{path: "input.txt", contents: "v3 bytes"}} =
             Jido.Exec.run(MyApp.Actions.ReadFile, %{path: "input.txt"}, %{filesystem: filesystem})

    assert {:error, _} = Jido.Exec.run(MyApp.Actions.ReadFile, %{path: 123}, %{filesystem: filesystem})
    assert Application.get_application(Jido.Action) == :jido
  end
end
