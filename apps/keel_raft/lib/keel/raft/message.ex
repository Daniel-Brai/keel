defmodule Keel.Raft.Message do
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Message.TimeoutNow

  @type t ::
          RequestVote.t()
          | RequestVoteReply.t()
          | AppendEntries.t()
          | AppendEntriesReply.t()
          | InstallSnapshot.t()
          | InstallSnapshotReply.t()
          | TimeoutNow.t()
          | ReadIndex.t()
          | ReadIndexReply.t()
end
