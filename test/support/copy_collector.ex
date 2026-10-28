defmodule JidoVfsTest.CopyCollector do
  @moduledoc false
  defstruct [:failure]

  defimpl Collectable do
    def into(collector) do
      {[],
       fn
         _, {:cont, _} ->
           case collector.failure do
             :raise -> raise "write failed"
             :throw -> throw(:write_failed)
           end

         _, :halt ->
           :ok

         acc, :done ->
           acc
       end}
    end
  end
end
