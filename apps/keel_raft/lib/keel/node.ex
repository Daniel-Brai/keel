defmodule Keel.Node do
  use Boundary,
    top_level?: true,
    deps: [Keel.Common, Keel.Env, Keel.Log, Keel.Raft, Keel.Transport],
    exports: [Applier, Interpreter, Server, SnapshotReceiver, SnapshotSender, State, Supervisor]
end
