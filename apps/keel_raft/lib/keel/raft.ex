defmodule Keel.Raft do
  use Boundary,
    top_level?: true,
    deps: [Keel.Common],
    exports: [
      Config,
      Node,
      Message,
      HardState,
      Entry,
      InvariantError,
      Log,
      SnapshotMeta,
      SnapshotStream,
      Message.RequestVote,
      Message.RequestVoteReply,
      Message.AppendEntries,
      Message.AppendEntriesReply,
      Message.InstallSnapshot,
      Message.InstallSnapshotReply,
      Message.TimeoutNow,
      Message.ReadIndex,
      Message.ReadIndexReply
    ]
end
