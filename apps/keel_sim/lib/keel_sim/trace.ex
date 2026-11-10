defmodule KeelSim.Trace do
  @moduledoc "One line per core step. A run that needs an event the model lacks raises."

  alias Keel.Raft.Entry
  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Node
  alias KeelSim.Swarm

  @type line :: %{String.t() => term()}

  @type result :: :skip | {:ok, line()}

  @roles %{
    follower: "Follower",
    pre_candidate: "PreCandidate",
    candidate: "Candidate",
    leader: "Leader"
  }

  @doc "A run with no snapshots or membership changes and a disk that keeps only what was synced."
  @spec options(integer(), pos_integer()) :: map()
  def options(seed, steps) do
    %{
      seed: seed,
      steps: steps,
      size: 3,
      workload: :chain,
      bootstrap: false,
      crash_profile: :strict,
      faults: ~w[drop duplicate crash partition]a,
      snapshot_threshold_bytes: 1_000_000_000,
      trace: true
    }
  end

  @spec step(Node.event(), Node.t(), Node.t(), [Node.effect()], non_neg_integer()) :: result()
  def step(:tick, before, node, effects, number) do
    case changed?(before, node) do
      true -> {:ok, line(node, "tick", effects, number)}
      false -> :skip
    end
  end

  def step({:message, message}, _before, node, effects, number) do
    encoded = message(message)
    base = line(node, "message", effects, number)
    line = Map.put(base, "msg", encoded)

    {:ok, line}
  end

  def step({:persisted, _seq}, _before, node, effects, number),
    do: {:ok, line(node, "persisted", effects, number)}

  def step({:propose, _ref, _data, _time_ms}, before, node, effects, number) do
    case before.log.last_index == node.log.last_index do
      true -> :skip
      false -> {:ok, line(node, "propose", effects, number)}
    end
  end

  def step({:applied, _index}, _before, _node, _effects, _number), do: :skip

  def step(event, _before, _node, _effects, _number),
    do: raise(ArgumentError, "the model has no action for #{inspect(event)}")

  @spec restart(Node.t(), non_neg_integer()) :: line()
  def restart(%Node{} = node, number), do: line(node, "restart", [], number)

  @doc "The first line names the cluster the trace came from."
  @spec encode(Swarm.config(), [line()]) :: String.t()
  def encode(config, lines) do
    header = %{"ev" => "init", "size" => config.size, "pre_vote" => config.pre_vote}
    all = [header | lines]
    rows = Enum.map(all, &JSON.encode!/1)
    body = Enum.join(rows, "\n")

    body <> "\n"
  end

  defp changed?(before, node), do: shape(before) != shape(node)

  defp shape(%Node{} = node) do
    {node.term, node.role, node.voted_for, node.log.last_index, node.log.last_term, node.commit,
     node.durable_index}
  end

  defp line(%Node{} = node, event, effects, number) do
    %{
      "n" => number,
      "i" => node.id,
      "ev" => event,
      "term" => node.term,
      "role" => Map.fetch!(@roles, node.role),
      "voted" => voted(node.voted_for),
      "log" => entries(node),
      "commit" => node.commit,
      "durable" => node.durable_index,
      "acks" => acks(node, effects)
    }
  end

  defp voted(nil), do: "nil"
  defp voted(id), do: id

  defp entries(%Node{log: log}) do
    first = log.first_index

    case log.last_index >= first do
      true -> Enum.map(first..log.last_index, &entry_at(log, &1))
      false -> []
    end
  end

  defp entry_at(log, index) do
    found = Log.fetch!(log, index)

    entry(found)
  end

  defp entry(%Entry{term: term, kind: kind}), do: %{"term" => term, "value" => value(kind)}

  defp value(:noop), do: "noop"
  defp value(:command), do: "cmd"
  defp value(kind), do: raise(ArgumentError, "the model has no entry kind #{inspect(kind)}")

  # Replies that claim durable state are the ones the model checks
  defp acks(node, effects) do
    for {:send, message} <- effects, ack?(message), do: ack(node, message)
  end

  defp ack?(%AppendEntriesReply{success?: true}), do: true
  defp ack?(%RequestVoteReply{granted?: true, pre_vote?: false}), do: true
  defp ack?(_message), do: false

  defp ack(node, %AppendEntriesReply{} = reply) do
    term = Log.term_at(node.log, reply.match_index)

    reply
    |> message()
    |> Map.put("mlogTerm", term_of(term))
  end

  defp ack(_node, %RequestVoteReply{} = reply), do: message(reply)

  defp term_of(nil), do: 0
  defp term_of(term), do: term

  defp message(%RequestVote{transfer?: true}),
    do: raise(ArgumentError, "the model has no transfer vote")

  defp message(%RequestVote{} = request) do
    %{
      "mtype" => vote_type(request.pre_vote?, "Request"),
      "mterm" => request.term,
      "mlastLogTerm" => request.last_log_term,
      "mlastLogIndex" => request.last_log_index,
      "msource" => request.from,
      "mdest" => request.to
    }
  end

  defp message(%RequestVoteReply{} = reply) do
    %{
      "mtype" => vote_type(reply.pre_vote?, "Response"),
      "mterm" => reply.term,
      "mvoteGranted" => reply.granted?,
      "msource" => reply.from,
      "mdest" => reply.to
    }
  end

  defp message(%AppendEntries{} = append) do
    %{
      "mtype" => "AppendEntriesRequest",
      "mterm" => append.term,
      "mprevLogIndex" => append.prev_index,
      "mprevLogTerm" => append.prev_term,
      "mentries" => Enum.map(append.entries, &entry/1),
      "mcommitIndex" => append.commit,
      "msource" => append.from,
      "mdest" => append.to
    }
  end

  defp message(%AppendEntriesReply{} = reply) do
    %{
      "mtype" => "AppendEntriesResponse",
      "mterm" => reply.term,
      "msuccess" => reply.success?,
      "mmatchIndex" => reply.match_index,
      "msource" => reply.from,
      "mdest" => reply.to
    }
  end

  defp message(other), do: raise(ArgumentError, "the model has no message #{inspect(other)}")

  defp vote_type(true, kind), do: "PreVote" <> kind
  defp vote_type(false, kind), do: "RequestVote" <> kind
end
