defmodule Keel.Transport.Envelope do
  alias Keel.Common.T
  alias Keel.Raft.Message
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Message.TimeoutNow

  @type cluster_id :: binary()

  @type t :: {:keel, cluster_id(), Message.t()}

  @spec wrap(cluster_id(), Message.t()) :: t()
  def wrap(cluster_id, message), do: {:keel, cluster_id, message}

  @doc "A former member passes only as a reply or a vote request."
  @spec check(term(), cluster_id(), cluster_id(), T.member_id(), MapSet.t(T.member_id())) ::
          :ok | {:error, :invalid}
  def check(message, sent_id, own_id, member_id, removed \\ MapSet.new()) do
    if sent_id == own_id and addressed_to?(message, member_id) and
         not refused?(message, removed),
       do: :ok,
       else: {:error, :invalid}
  end

  @doc "A member that has no log yet hears only a leader of its cluster."
  @spec check_join(term(), cluster_id(), cluster_id(), T.member_id()) ::
          :ok | {:error, :invalid}
  def check_join(message, sent_id, join_id, member_id) do
    if sent_id == join_id and addressed_to?(message, member_id) and leader_message?(message),
      do: :ok,
      else: {:error, :invalid}
  end

  defp leader_message?(%AppendEntries{}), do: true
  defp leader_message?(%InstallSnapshot{}), do: true
  defp leader_message?(_message), do: false

  defp refused?(%RequestVote{}, _removed), do: false
  defp refused?(%RequestVoteReply{}, _removed), do: false
  defp refused?(%AppendEntriesReply{}, _removed), do: false
  defp refused?(%InstallSnapshotReply{}, _removed), do: false
  defp refused?(%ReadIndexReply{}, _removed), do: false
  defp refused?(%{from: from}, removed), do: MapSet.member?(removed, from)

  defp addressed_to?(%RequestVote{to: to}, member_id), do: to == member_id
  defp addressed_to?(%RequestVoteReply{to: to}, member_id), do: to == member_id
  defp addressed_to?(%AppendEntries{to: to}, member_id), do: to == member_id
  defp addressed_to?(%AppendEntriesReply{to: to}, member_id), do: to == member_id
  defp addressed_to?(%InstallSnapshot{to: to}, member_id), do: to == member_id
  defp addressed_to?(%InstallSnapshotReply{to: to}, member_id), do: to == member_id
  defp addressed_to?(%TimeoutNow{to: to}, member_id), do: to == member_id
  defp addressed_to?(%ReadIndex{to: to}, member_id), do: to == member_id
  defp addressed_to?(%ReadIndexReply{to: to}, member_id), do: to == member_id
  defp addressed_to?(_message, _member_id), do: false
end
