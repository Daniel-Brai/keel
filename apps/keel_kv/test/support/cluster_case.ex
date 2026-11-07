defmodule Keel.KV.ClusterCase do
  use ExUnit.CaseTemplate

  alias Keel.KV.TestCluster

  using do
    quote do
      @moduletag :cluster
      @moduletag :tmp_dir
      @moduletag timeout: 120_000

      alias Keel.KV
      alias Keel.KV.TestCluster
    end
  end

  setup %{tmp_dir: dir} do
    cluster = TestCluster.start!(dir)

    on_exit(fn -> TestCluster.stop(cluster) end)

    {:ok, cluster: cluster}
  end
end
