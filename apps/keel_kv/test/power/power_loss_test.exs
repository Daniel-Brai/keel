defmodule Keel.KV.PowerLossTest do
  use ExUnit.Case, async: false

  alias Keel.KV.TestUtils

  @moduletag :power
  @moduletag :tmp_dir
  @moduletag timeout: 14_400_000
  @moduletag skip: TestUtils.power_skip()

  describe "power_loss_run!/2" do
    test "loses an acknowledged write at some replay point when syncs are off (T-28.10)", %{
      tmp_dir: dir
    } do
      run = TestUtils.power_loss_run!(dir, :none)

      lossy = Enum.reject(run.points, &(&1.lost == []))

      assert lossy != []
    end

    test "loses no acknowledged write at any replay point (T-28.9)", %{tmp_dir: dir} do
      run = TestUtils.power_loss_run!(dir, :disk)

      lossy = Enum.reject(run.points, &(&1.lost == []))

      assert length(run.points) >= 200
      assert lossy == []
    end
  end
end
