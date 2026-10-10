defmodule Keel.Transport do
  default = [Dist, Dist.Supervisor, Envelope, FaultProxy]

  exports =
    case Mix.env() do
      :test -> [Stub | default]
      _env -> default
    end

  use Boundary, top_level?: true, deps: [Keel.Common, Keel.Raft], exports: exports

  alias Keel.Common.T
  alias Keel.Raft.Message

  @type group :: atom()

  @type members :: %{T.member_id() => node()}

  @callback child_spec(keyword()) :: Supervisor.child_spec()

  @callback send(group(), T.member_id(), Message.t()) :: :ok

  @callback update_members(group(), members()) :: :ok
end
