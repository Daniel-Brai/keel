defmodule Keel.BootTest do
  use ExUnit.Case, async: true

  alias Keel.Boot
  alias Keel.TestUtils
  alias Keel.Transport.Stub, as: TransportStub

  @moduletag :tmp_dir

  describe "start_groups/1" do
    test "fails for a new member that neither bootstraps nor joins", %{tmp_dir: dir} do
      group = TestUtils.random_group()
      TransportStub.listen(group)
      opts = TestUtils.boot_opts(dir, group, bootstrap: false)

      started = Boot.start_groups([{group, opts}])

      assert {:error, {:group_failed, ^group, _reason}} = started
    end

    test "starts every group it is given", %{tmp_dir: dir} do
      group = TestUtils.random_group()
      TransportStub.listen(group)
      opts = TestUtils.boot_opts(dir, group)
      on_exit(fn -> Keel.stop_group(group) end)

      Boot.start_groups([{group, opts}])
      TestUtils.await_role!(group, :leader)

      status = Keel.status(group)

      assert status.member_id == "a"
    end

    test "starts nothing for an empty list" do
      started = Boot.start_groups([])

      assert started == :ok
    end
  end
end
