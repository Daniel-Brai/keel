defmodule Mix.Tasks.Keel.Kv.Connection do
  use Boundary, classify_to: Keel.KV

  alias Keel.Common.Name

  def start do
    unless Node.alive?() do
      number = System.unique_integer([:positive])
      name = Name.build("keel_cli", :node, "#{number}@127.0.0.1")
      {:ok, _pid} = :net_kernel.start(name, %{name_domain: :longnames, hidden: true})
    end

    :ok
  end
end
