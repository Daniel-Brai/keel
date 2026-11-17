defmodule Keel.TestUtils do
  use ExUnitProperties

  import ExUnit.Callbacks

  alias Keel.Chain.Machine, as: ChainMachine
  alias Keel.Common.Name
  alias Keel.Env.Disk.Posix
  alias Keel.Env.Disk.Stub, as: DiskStub
  alias Keel.Log.Codec
  alias Keel.Log.Codec.Header
  alias Keel.Log.Recovery.Result
  alias Keel.Log.Snapshot
  alias Keel.Log.Store
  alias Keel.Log.Writer
  alias Keel.Machine.Stub, as: MachineStub
  alias Keel.Node.Applier
  alias Keel.Node.Interpreter
  alias Keel.Node.Server
  alias Keel.Node.SnapshotReceiver
  alias Keel.Node.SnapshotSender
  alias Keel.Node.State
  alias Keel.Node.Supervisor, as: NodeSupervisor
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.HardState
  alias Keel.Raft.Log
  alias Keel.Raft.Message.AppendEntries
  alias Keel.Raft.Message.AppendEntriesReply
  alias Keel.Raft.Message.InstallSnapshot
  alias Keel.Raft.Message.InstallSnapshotReply
  alias Keel.Raft.Message.ReadIndex
  alias Keel.Raft.Message.ReadIndexReply
  alias Keel.Raft.Message.RequestVote
  alias Keel.Raft.Message.RequestVoteReply
  alias Keel.Raft.Message.TimeoutNow
  alias Keel.Raft.Node
  alias Keel.Raft.SnapshotMeta
  alias Keel.Raft.SnapshotStream
  alias Keel.Transport.Dist
  alias Keel.Transport.Dist.Supervisor, as: DistSupervisor
  alias Keel.Transport.FaultProxy
  alias Keel.Transport.Stub, as: TransportStub

  def new_node(id, voters, opts \\ []) do
    hard_state = %HardState{term: Keyword.get(opts, :term, 0), voted_for: nil, commit: 0}

    base = %{id: id, voters: voters, seed: {1, 2, 3}, hard_state: hard_state}

    safety = %{
      pre_vote: Keyword.get(opts, :pre_vote, false),
      check_quorum: Keyword.get(opts, :pre_vote, false)
    }

    base |> Map.merge(safety) |> Node.new()
  end

  def node_in_term!(id, voters, term) do
    node = new_node(id, voters, term: term)

    %{node | persist_seq: 1, durable_seq: 1}
  end

  def tick!(node, 0), do: node

  def tick!(node, count) do
    {ticked, _effects} = Node.step(node, :tick)

    tick!(ticked, count - 1)
  end

  def timed_out_node!(id, voters) do
    node = new_node(id, voters)
    tick!(node, node.election_timeout - 1)
  end

  def candidate!(id, voters) do
    {candidate, _effects} = id |> timed_out_node!(voters) |> Node.step(:tick)
    {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})

    durable
  end

  def leader!(id, voters) do
    candidate = candidate!(id, voters)
    voter = voters |> Enum.reject(&(&1 == id)) |> hd()
    reply = %RequestVoteReply{term: candidate.term, from: voter, to: id, granted?: true}

    {leader, _effects} = Node.step(candidate, {:message, reply})

    leader
  end

  def leader_with_log!(id, entries) do
    node = follower_with_log!(id, 1, entries)
    {candidate, _effects} = node |> tick!(node.election_timeout - 1) |> Node.step(:tick)
    {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})
    reply = %RequestVoteReply{term: durable.term, from: "m2", to: id, granted?: true}

    {leader, _effects} = Node.step(durable, {:message, reply})

    leader
  end

  def snapshot_meta(index, term, opts \\ []) do
    %SnapshotMeta{
      index: index,
      term: term,
      time_ms: index * 10,
      size: Keyword.get(opts, :size, 2_048),
      checksum: Keyword.get(opts, :checksum, 7)
    }
  end

  def snapshot_stream(size, inflight) do
    SnapshotStream.new(
      meta: snapshot_meta(5, 1, size: size),
      term: 1,
      seq: 9,
      from: "m1",
      to: "m2",
      chunk_bytes: 16,
      max_inflight: inflight
    )
  end

  def random_chunk(size), do: :binary.copy(<<7>>, size)

  def install(%SnapshotMeta{} = meta, offset, size, opts \\ []) do
    %InstallSnapshot{
      term: Keyword.get(opts, :term, 1),
      from: "m1",
      to: "m2",
      meta: meta,
      offset: offset,
      data: random_chunk(size),
      done?: offset + size >= meta.size,
      seq: Keyword.get(opts, :seq, 1)
    }
  end

  # Terms run from the first pair of each term. `{10, 3}` ends a run of term one
  def terms_to(last, switch, before, later) do
    Enum.map(1..last, fn index -> if index < switch, do: {index, before}, else: {index, later} end)
  end

  def snapshotting_node!(id, pairs, opts \\ []) do
    hard_state = %HardState{term: Keyword.get(opts, :term, 1), voted_for: nil, commit: 0}

    base = %{
      id: id,
      voters: ~w[m1 m2 m3],
      seed: {1, 2, 3},
      hard_state: hard_state,
      entries: entries(pairs),
      snapshot_threshold_bytes: 1,
      snapshot_trailing_entries: 2,
      snapshot_ack_timeout_ticks: 3,
      snapshot_install_timeout_ticks: 10,
      pre_vote: false,
      check_quorum: false
    }

    node = base |> Map.merge(Map.new(opts)) |> Node.new()

    %{node | persist_seq: 1, durable_seq: 1}
  end

  def run_entries(from, last, term), do: entries(Enum.map(from..last, &{&1, term}))

  def switching_entries(last, switch, before, later),
    do: entries(terms_to(last, switch, before, later))

  def sending! do
    leader = leader_with_snapshot!()

    snapshot_sending!(leader)
  end

  def prev_indexes(effects),
    do: for(%AppendEntries{prev_index: prev} <- sent(effects, AppendEntries), do: prev)

  def kept_indexes(%Log{} = log), do: Enum.to_list(log.first_index..log.last_index//1)

  def random_log_ops do
    append =
      StreamData.tuple(
        {StreamData.constant(:append), StreamData.integer(1..9), StreamData.integer(0..2)}
      )

    truncate = StreamData.tuple({StreamData.constant(:truncate), StreamData.integer(0..99)})
    compact = StreamData.tuple({StreamData.constant(:compact), StreamData.integer(0..99)})
    op = StreamData.frequency([{4, append}, {1, truncate}, {1, compact}])

    StreamData.list_of(op, max_length: 60)
  end

  def play_log_ops(ops, chunk_entries) do
    empty = Log.new([], chunk_entries)

    Enum.reduce(ops, empty, &play_log_op/2)
  end

  defp play_log_op({:append, count, bump}, log) do
    term = max(log.last_term + bump, 1)
    pairs = for index <- (log.last_index + 1)..(log.last_index + count), do: {index, term}
    added = entries(pairs)

    Log.append(log, added)
  end

  defp play_log_op({:truncate, pick}, log) do
    span = log.last_index - log.first_index + 2
    index = log.first_index + rem(pick, span)

    Log.truncate_from(log, index)
  end

  defp play_log_op({:compact, pick}, log) do
    span = log.last_index - log.first_index + 2
    keep = log.first_index + rem(pick, span)
    term = Log.term_at(log, keep - 1)
    meta = snapshot_meta(keep - 1, term)

    Log.compact(log, meta, keep)
  end

  def log_view(%Log{} = log) do
    held = log.first_index..log.last_index//1
    count = Range.size(held)

    %{
      first: log.first_index,
      last: {log.last_index, log.last_term, log.last_time_ms},
      base: {log.base_term, log.base_time_ms},
      entries: Log.slice(log, log.first_index, log.last_index, max(count, 1)),
      terms: Enum.map((log.first_index - 1)..log.last_index//1, &Log.term_at(log, &1)),
      times: Enum.map(held, &Log.time_at(log, &1)),
      bytes: Log.bytes_after(log, log.first_index - 1)
    }
  end

  def done_flags(effects) do
    for %InstallSnapshotReply{done?: true} <- sent(effects, InstallSnapshotReply), do: true
  end

  def write_snapshot!(dir, index, term, payload \\ "state", config \\ nil) do
    {:ok, _disk, meta} =
      Snapshot.write(Posix.new(), dir, index, term, index * 10, payload, config)

    meta
  end

  def garbage_snapshot!(dir, index, term) do
    File.mkdir_p!(dir)
    path = Path.join(dir, Snapshot.name(index, term))

    File.write!(path, "not a snapshot")
  end

  def snapshot_present?(dir) do
    snap = snap_dir(dir)

    case File.ls(snap) do
      {:ok, names} -> Enum.any?(names, &String.ends_with?(&1, ".snap"))
      {:error, _reason} -> false
    end
  end

  def snapshot_files(dir), do: dir |> File.ls!() |> Enum.sort()

  def committed!(node, index), do: %{node | commit: index, apply_sent: index, applied: index}

  def received!(node, %SnapshotMeta{} = meta) do
    chunk = install(meta, 0, meta.size, term: node.term)
    {receiving, _effects} = Node.step(node, {:message, chunk})

    Node.step(receiving, {:snapshot_received, meta})
  end

  def leader_with_snapshot!(opts \\ []) do
    meta = snapshot_meta(5, 1)
    pairs = Enum.map(6..10, &{&1, 1})

    node =
      snapshotting_node!(
        "m1",
        pairs,
        Keyword.merge([first_index: 6, snapshot: meta, snapshot_threshold_bytes: 1_000_000], opts)
      )

    {candidate, _effects} = node |> tick!(node.election_timeout - 1) |> Node.step(:tick)
    {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})
    vote = %RequestVoteReply{term: durable.term, from: "m2", to: "m1", granted?: true}

    {leader, _effects} = Node.step(durable, {:message, vote})

    leader
  end

  def failed_reply(leader, from, match_index) do
    %AppendEntriesReply{
      term: leader.term,
      from: from,
      to: leader.id,
      success?: false,
      match_index: match_index,
      seq: 1
    }
  end

  def snapshot_reply(leader, from, seq, offset, done?) do
    %InstallSnapshotReply{
      term: leader.term,
      from: from,
      to: leader.id,
      offset: offset,
      done?: done?,
      seq: seq
    }
  end

  def snapshot_sending!(leader) do
    {sending, effects} = Node.step(leader, {:message, failed_reply(leader, "m2", 0)})

    [{:send_snapshot, "m2", meta, _term, seq}] =
      Enum.filter(effects, &match?({:send_snapshot, _, _, _, _}, &1))

    {sending, meta, seq}
  end

  def tick_effects!(node, count) do
    {ticked, effects} =
      Enum.reduce(1..count, {node, []}, fn _step, {acc, seen} ->
        {next, effects} = Node.step(acc, :tick)

        {next, seen ++ effects}
      end)

    {ticked, effects}
  end

  def request_vote(from, to, term),
    do: %RequestVote{term: term, from: from, to: to, last_log_index: 0, last_log_term: 0}

  def heartbeat(from, to, term) do
    %AppendEntries{
      term: term,
      from: from,
      to: to,
      prev_index: 0,
      prev_term: 0,
      entries: [],
      commit: 0,
      seq: 1
    }
  end

  def voted!(node, candidate) do
    request = request_vote(candidate, node.id, node.term)

    {voter, _effects} = Node.step(node, {:message, request})
    {durable, _effects} = Node.step(voter, {:persisted, voter.persist_seq})

    durable
  end

  def pre_vote_node!(id, voters, opts \\ []) do
    node = new_node(id, voters, Keyword.put(opts, :pre_vote, true))

    %{node | persist_seq: 1, durable_seq: 1}
  end

  def pre_candidate!(id, voters) do
    node = pre_vote_node!(id, voters)
    {pre, _effects} = node |> tick!(node.election_timeout - 1) |> Node.step(:tick)

    pre
  end

  def pre_vote_leader!(id, voters) do
    pre = pre_candidate!(id, voters)
    voter = voters |> Enum.reject(&(&1 == id)) |> hd()

    grant = %RequestVoteReply{
      term: pre.term + 1,
      from: voter,
      to: id,
      granted?: true,
      pre_vote?: true
    }

    {candidate, _effects} = Node.step(pre, {:message, grant})
    {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})
    vote = %RequestVoteReply{term: durable.term, from: voter, to: id, granted?: true}
    {leader, _effects} = Node.step(durable, {:message, vote})

    leader
  end

  def single_voter_leader!(id) do
    node = timed_out_node!(id, [id])
    {candidate, _effects} = Node.step(node, :tick)
    {leader, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})
    {committed, _effects} = Node.step(leader, {:persisted, leader.persist_seq})

    committed
  end

  def committed_leader!(id, voters) do
    leader = durable_leader!(id, voters)
    {committed, _effects} = ack(leader, "m2", leader.log.last_index)

    committed
  end

  def reelected!(follower, voter) do
    {candidate, _effects} = follower |> tick!(follower.election_timeout - 1) |> Node.step(:tick)
    {durable, _effects} = Node.step(candidate, {:persisted, candidate.persist_seq})
    vote = %RequestVoteReply{term: durable.term, from: voter, to: durable.id, granted?: true}
    {leader, _effects} = Node.step(durable, {:message, vote})

    leader
  end

  def append_reply(leader, from, seq, success? \\ true) do
    %AppendEntriesReply{
      term: leader.term,
      from: from,
      to: leader.id,
      success?: success?,
      match_index: leader.log.last_index,
      seq: seq
    }
  end

  def reply_from!(leader, from, seq, success? \\ true) do
    reply = append_reply(leader, from, seq, success?)

    Node.step(leader, {:message, reply})
  end

  def read!(leader, count) do
    Enum.reduce(1..count, {leader, [], []}, fn _step, {acc, refs, seen} ->
      ref = make_ref()
      {next, effects} = Node.step(acc, {:read_index, ref})

      {next, Enum.concat(refs, [ref]), Enum.concat(seen, effects)}
    end)
  end

  def append_entries_reply(from, to, term) do
    %AppendEntriesReply{
      term: term,
      from: from,
      to: to,
      success?: true,
      match_index: 0,
      seq: 1
    }
  end

  def read_refs(effects), do: for({:read_ready, ref, _index} <- effects, do: ref)

  def transferring!(leader, target) do
    ref = make_ref()
    {transferring, effects} = Node.step(leader, {:transfer_leadership, ref, target})

    {transferring, ref, effects}
  end

  def following!(id, voters, term, leader, opts \\ []) do
    base = new_node(id, voters, Keyword.put(opts, :term, term))
    node = %{base | persist_seq: 1, durable_seq: 1}
    {follower, _effects} = Node.step(node, {:message, heartbeat(leader, id, term)})

    follower
  end

  def forwarded_read(from, ref_id, term),
    do: %ReadIndex{term: term, from: from, to: "m1", ref_id: ref_id}

  def read_reply(to, ref_id, term, index, ok?),
    do: %ReadIndexReply{term: term, from: "m1", to: to, ref_id: ref_id, index: index, ok?: ok?}

  def timeout_now(from, to, term), do: %TimeoutNow{term: term, from: from, to: to}

  def pre_vote_request(from, to, term, opts \\ []) do
    %RequestVote{
      term: term,
      from: from,
      to: to,
      last_log_index: Keyword.get(opts, :last_log_index, 0),
      last_log_term: Keyword.get(opts, :last_log_term, 0),
      pre_vote?: Keyword.get(opts, :pre_vote?, true),
      transfer?: Keyword.get(opts, :transfer?, false)
    }
  end

  def sent(effects, type) do
    for {:send, %{__struct__: ^type} = message} <- effects, do: message
  end

  def sent_to(effects, type) do
    for {:send, %{__struct__: ^type, to: to}} <- effects, do: to
  end

  def sent_to_peer(effects, type, peer) do
    for {:send, %{__struct__: ^type, to: ^peer} = message} <- effects, do: message
  end

  def pipelining_leader!(opts \\ []) do
    entries = run_entries(1, 40, 1)
    leader = leader_with_log!("m1", entries)
    sized = Map.merge(leader.opts, Map.new(opts))
    refusal = failed_reply(leader, "m2", 10)
    {probed, _effects} = Node.step(%{leader | opts: sized}, {:message, refusal})
    {acked, _effects} = ack(probed, "m2", 26)

    acked
  end

  def entries(pairs) do
    Enum.map(pairs, fn {index, term} ->
      %Entry{index: index, term: term, kind: :command, data: <<index>>, time_ms: index * 10}
    end)
  end

  def follower_with_log!(id, term, entries) do
    hard_state = %HardState{term: term, voted_for: nil, commit: 0}

    node =
      Node.new(%{
        id: id,
        voters: ~w[m1 m2 m3],
        seed: {1, 2, 3},
        hard_state: hard_state,
        entries: entries,
        pre_vote: false,
        check_quorum: false
      })

    %{node | persist_seq: 1, durable_seq: 1}
  end

  def append(term, prev_index, prev_term, entries, commit) do
    %AppendEntries{
      term: term,
      from: "m1",
      to: "m2",
      prev_index: prev_index,
      prev_term: prev_term,
      entries: entries,
      commit: commit,
      seq: 1
    }
  end

  def durable_leader!(id, voters) do
    leader = leader!(id, voters)
    {durable, _effects} = Node.step(leader, {:persisted, leader.persist_seq})

    durable
  end

  def ack(leader, from, match_index) do
    reply = %AppendEntriesReply{
      term: leader.term,
      from: from,
      to: leader.id,
      success?: true,
      match_index: match_index,
      seq: 1
    }

    Node.step(leader, {:message, reply})
  end

  def log_identity do
    hash = :crypto.hash(:sha256, "member")
    member_tag = binary_part(hash, 0, 8)

    %{cluster_id: <<0::128>>, member_tag: member_tag, max_record_bytes: 1_000_000}
  end

  def random_entries_record do
    part =
      StreamData.tuple({
        StreamData.integer(0..9),
        StreamData.member_of(~w[command noop config]a),
        StreamData.binary(max_length: 24),
        StreamData.integer(0..1_000_000_000_000)
      })

    parts = StreamData.list_of(part, min_length: 1, max_length: 4)

    gen all(start <- StreamData.integer(1..1_000), list <- parts) do
      entries =
        list
        |> Enum.with_index(start)
        |> Enum.map(fn {{term, kind, data, time_ms}, index} ->
          %Entry{index: index, term: term, kind: kind, data: data, time_ms: time_ms}
        end)

      {:entries, entries}
    end
  end

  def random_record do
    StreamData.one_of([
      random_entries_record(),
      random_hard_state_record(),
      gen(all(from <- StreamData.integer(1..1_000), do: {:truncate, from})),
      gen all(
            segment <- StreamData.integer(1..9),
            offset <- StreamData.integer(64..9_999),
            seq <- StreamData.integer(0..9_999)
          ) do
        {:sync_mark, segment, offset, seq}
      end
    ])
  end

  def random_hard_state_record do
    gen all(
          term <- StreamData.integer(0..9_999),
          vote <-
            StreamData.one_of([
              nil,
              StreamData.string(:alphanumeric, min_length: 1, max_length: 8)
            ]),
          commit <- StreamData.integer(0..9_999)
        ) do
      {:hard_state, %HardState{term: term, voted_for: vote, commit: commit}}
    end
  end

  def flip_bit(binary, position) do
    <<head::bitstring-size(^position), bit::1, tail::bitstring>> = binary

    <<head::bitstring, 1 - bit::1, tail::bitstring>>
  end

  def undetected_flips(encoded, segment) do
    last = bit_size(encoded) - 1
    Enum.reject(0..last, &flip_detected?(encoded, &1, segment))
  end

  def undetected_header_flips(encoded) do
    last = bit_size(encoded) - 1
    Enum.reject(0..last, &header_flip_detected?(encoded, &1))
  end

  def wrong_truncations(encoded, segment) do
    last = byte_size(encoded) - 1
    Enum.reject(0..last, &truncation_detected?(encoded, &1, segment))
  end

  defp flip_detected?(encoded, position, segment) do
    flipped = flip_bit(encoded, position)
    decoded = Codec.decode_record(flipped, segment, 1_000_000)

    match?({:error, _reason}, decoded)
  end

  defp header_flip_detected?(encoded, position) do
    flipped = flip_bit(encoded, position)
    decoded = Codec.decode_header(flipped)

    match?({:error, _reason}, decoded)
  end

  defp truncation_detected?(encoded, length, segment) do
    prefix = binary_part(encoded, 0, length)
    decoded = Codec.decode_record(prefix, segment, 1_000_000)

    decoded in [{:error, :short}, :end]
  end

  def raw_record(type, payload, seq, segment, flags \\ 0) do
    head = <<byte_size(payload)::32, type, flags, 0::16, segment::32, seq::64>>
    crc = head |> :erlang.crc32() |> :erlang.crc32(payload)

    <<crc::32, head::binary, payload::binary>>
  end

  def segment_bytes(number, first_seq, bodies, opts \\ []) do
    identity = log_identity()

    header =
      Codec.encode_header(%Header{
        segment: number,
        cluster_id: identity.cluster_id,
        member_tag: identity.member_tag,
        first_seq: first_seq
      })

    records = number |> encoded_records(first_seq, bodies) |> corrupt(opts[:corrupt])
    bytes = :erlang.list_to_binary([header | records])
    size = byte_size(bytes)
    pad_to = Keyword.get(opts, :pad, 0)
    pad = max(pad_to - size, 0)

    <<bytes::binary, 0::size(pad * 8)>>
  end

  def header_bytes(number, first_seq) do
    identity = log_identity()

    Codec.encode_header(%Header{
      segment: number,
      cluster_id: identity.cluster_id,
      member_tag: identity.member_tag,
      first_seq: first_seq
    })
  end

  def record_offset(number, first_seq, bodies, position) do
    sizes = encoded_records(number, first_seq, bodies)

    64 + (sizes |> Enum.take(position) |> Enum.map(&byte_size/1) |> Enum.sum())
  end

  def pluck(list, key) when is_atom(key), do: Enum.map(list, &Map.fetch!(&1, key))
  def pluck(list, position), do: Enum.map(list, &elem(&1, position))

  def decode_class(bytes) do
    case Codec.decode_record(bytes, 1, 1_000_000) do
      {:ok, _record, _rest} -> :record
      :end -> :end
      {:error, _reason} -> :error
    end
  end

  defp encoded_records(number, first_seq, bodies) do
    bodies
    |> Enum.with_index(first_seq)
    |> Enum.map(fn {body, seq} -> Codec.encode_record(body, seq, number) end)
  end

  defp corrupt(records, nil), do: records

  defp corrupt(records, position) do
    List.update_at(records, position, fn record ->
      size = byte_size(record) - 1
      <<head::binary-size(^size), last>> = record

      <<head::binary, Bitwise.bxor(last, 255)>>
    end)
  end

  def hard_state(term, voted_for, commit),
    do: %HardState{term: term, voted_for: voted_for, commit: commit}

  def write_synced!(store, ops) do
    {:ok, written} = Store.write(store, ops)
    {:ok, synced} = Store.sync(written)

    synced
  end

  def write_batches!(store, batches), do: Enum.reduce(batches, store, &write_synced!(&2, &1))

  def filled_store!(disk, dir) do
    {store, _result} = open_store!(disk, dir, segment_bytes: 512)

    batches =
      Enum.map(0..9, fn batch ->
        entries = many_entries(batch * 3 + 1, 3, 1)

        [{:entries, entries}]
      end)

    write_batches!(store, batches)
  end

  def open_store!(disk, dir, opts \\ []) do
    {:ok, store, result} = Store.open(disk, dir, opts)

    {store, result}
  end

  def segment_files(dir), do: dir |> File.ls!() |> Enum.sort()

  def many_entries(from, count, term) do
    Enum.map(
      from..(from + count - 1),
      &%Entry{index: &1, term: term, kind: :command, data: :binary.copy(<<&1>>, 40), time_ms: &1}
    )
  end

  defp disk_or_new(nil), do: DiskStub.new()
  defp disk_or_new(disk), do: disk

  def start_writer!(dir, opts \\ []) do
    disk = opts |> Keyword.get(:disk) |> disk_or_new()
    owner = self()
    store_opts = Keyword.take(opts, [:member])
    extra = Keyword.take(opts, [:bootstrap, :join])
    base = [disk: disk, dir: dir, owner: owner, store: store_opts]
    writer_opts = Keyword.merge(base, extra)

    {:ok, writer} = Writer.start_link(writer_opts)

    writer
  end

  def persist_together!(writer, count) do
    :sys.suspend(writer)

    Enum.each(1..count, &persist_one!(writer, &1))

    :sys.resume(writer)
  end

  def persist_each!(writer, count) do
    Enum.each(1..count, &persist_one!(writer, &1))
  end

  defp persist_one!(writer, seq) do
    entries = many_entries(seq, 1, 1)

    Writer.persist(writer, seq, nil, entries)
  end

  def persisted_out_of_order(last) do
    seen = collect_persisted(last, [])
    rest = tl(seen)

    seen |> Enum.zip(rest) |> Enum.filter(fn {before, later} -> later <= before end)
  end

  defp collect_persisted(last, seen) do
    receive do
      {:persisted, ^last} -> Enum.reverse([last | seen])
      {:persisted, seq} -> collect_persisted(last, [seq | seen])
    after
      1_000 -> Enum.reverse(seen)
    end
  end

  @cluster_id <<1::128>>

  def cluster_id, do: @cluster_id

  def random_group,
    do: Name.build("g", :random, Integer.to_string(System.unique_integer([:positive])))

  def member_opts(dir, group, extra \\ []) do
    members = Keyword.get(extra, :members, %{"a" => node()})
    timing = [tick_ms: 5, heartbeat_ticks: 2, election_ticks_min: 4, election_ticks_max: 6]
    raft = Keyword.merge(timing, Keyword.get(extra, :raft, []))

    base = [
      group: group,
      member_id: "a",
      members: members,
      cluster_id: @cluster_id,
      bootstrap: true
    ]

    machine = [machine: ChainMachine, transport: TransportStub, data_dir: dir, raft: raft]
    rest = Keyword.drop(extra, [:members, :raft])

    base |> Keyword.merge(machine) |> Keyword.merge(rest)
  end

  def boot_opts(dir, group, extra \\ []) do
    opts = member_opts(dir, group, extra)

    Keyword.delete(opts, :group)
  end

  def slow_opts(dir, group, extra \\ []) do
    members = %{"a" => node(), "b" => node(), "c" => node()}
    raft = [election_ticks_min: 40, election_ticks_max: 50, pre_vote: false, check_quorum: false]

    member_opts(dir, group, Keyword.merge([members: members, raft: raft], extra))
  end

  def start_member!(dir, opts \\ []) do
    group = random_group()
    TransportStub.listen(group)
    start_supervised!({NodeSupervisor, member_opts(dir, group, opts)})

    group
  end

  def start_joining_member!(dir, opts \\ []) do
    group = random_group()
    TransportStub.listen(group)
    members = %{"a" => node(), "b" => node()}
    raft = [election_ticks_min: 4, election_ticks_max: 6]
    extra = Keyword.merge([members: members, raft: raft, bootstrap: false, join: true], opts)

    start_supervised!({NodeSupervisor, member_opts(dir, group, extra)})

    group
  end

  def identity_written?(dir) do
    log = Path.join(dir, "log")

    case File.ls(log) do
      {:ok, names} -> Enum.any?(names, &String.ends_with?(&1, ".seg"))
      {:error, :enoent} -> false
    end
  end

  def start_slow_member!(dir, opts \\ []) do
    group = random_group()
    TransportStub.listen(group)
    start_supervised!({NodeSupervisor, slow_opts(dir, group, opts)})

    group
  end

  def snap_dir(dir), do: Path.join(dir, "snap")

  def start_applier!(dir, machine, opts \\ []) do
    snap = snap_dir(dir)
    applier_opts = [machine: machine, server: self(), disk: Posix.new(), snap_dir: snap]
    spec_opts = Keyword.merge([id: make_ref()], opts)

    start_supervised!({Applier, applier_opts}, spec_opts)
  end

  def apply_chain!(applier, count) do
    pairs = Enum.map(1..count, &{&1, 1})

    Applier.apply_entries(applier, entries(pairs), %{})

    receive do
      {:applied, ^count} -> :ok
    end
  end

  def apply_config!(applier, config) do
    entry = config_entry(1, 1, config)

    Applier.apply_entries(applier, [entry], %{})

    receive do
      {:applied, 1} -> :ok
    end
  end

  def taken_snapshot!(applier) do
    Applier.take_snapshot(applier, 1)

    receive do
      {:snapshot_taken, meta} -> meta
    end
  end

  def snapshot_bytes(index, term, payload) do
    bytes = Snapshot.encode(index, term, index * 10, payload)
    {:ok, meta} = Snapshot.meta(bytes)

    {bytes, meta}
  end

  def start_receiver!(dir) do
    opts = [disk: Posix.new(), snap_dir: snap_dir(dir), server: self()]

    start_supervised!({SnapshotReceiver, opts}, id: make_ref())
  end

  def send_chunks!(receiver, meta, bytes, size) do
    total = byte_size(bytes)

    Enum.each(0..(total - 1)//size, fn offset ->
      length = min(size, total - offset)
      data = binary_part(bytes, offset, length)

      SnapshotReceiver.chunk(receiver, meta, offset, data, offset + length >= total)
    end)

    :sys.get_state(receiver)
  end

  def start_sender!(dir, meta, opts \\ []) do
    group = random_group()
    TransportStub.listen(group)

    base = [
      disk: Posix.new(),
      snap_dir: snap_dir(dir),
      meta: meta,
      term: 1,
      seq: 3,
      from: "a",
      to: "b",
      group: group,
      transport: TransportStub,
      chunk_bytes: 32,
      max_inflight: 2,
      ack_timeout_ms: 60_000
    ]

    sender = start_supervised!({SnapshotSender, Keyword.merge(base, opts)}, id: make_ref())
    :sys.get_state(sender)

    sender
  end

  def ack_sender!(sender, offset) do
    SnapshotSender.ack(sender, offset)
    :sys.get_state(sender)

    :ok
  end

  def sent_offsets do
    receive do
      {:sent, "b", %InstallSnapshot{offset: offset}} -> [offset | sent_offsets()]
    after
      0 -> []
    end
  end

  def await_offsets(count) do
    Enum.map(1..count, fn _number ->
      receive do
        {:sent, "b", %InstallSnapshot{offset: offset}} -> offset
      after
        2_000 -> :timeout
      end
    end)
  end

  def crashing_opts, do: [machine: MachineStub]

  def status(group) do
    name = Server.name(group)

    Server.status(name)
  end

  def propose(group, data) do
    name = Server.name(group)

    GenServer.call(name, {:propose, data})
  end

  def read(group, query) do
    name = Server.name(group)

    Server.read(name, query, 1_000)
  end

  def transfer(group, target) do
    name = Server.name(group)

    Server.transfer_leadership(name, target, 1_000)
  end

  def propose_async!(group, data), do: Task.async(fn -> propose(group, data) end)

  def wait_until!(check), do: wait_until!(check, 500)

  defp wait_until!(_check, 0), do: raise("condition never held")

  defp wait_until!(check, tries) do
    if check.() do
      :ok
    else
      Process.sleep(10)
      wait_until!(check, tries - 1)
    end
  end

  def await_role!(group, role), do: wait_until!(fn -> status(group).role == role end)

  def waiting(group) do
    name = Server.name(group)
    state = :sys.get_state(name)

    map_size(state.waiters)
  end

  def send_to_server(group, message) do
    name = Server.name(group)

    Kernel.send(name, message)

    :ok
  end

  def elect!(group) do
    term = status(group).term
    reply = %RequestVoteReply{term: term, from: "b", to: "a", granted?: true}

    send_to_server(group, {:keel, @cluster_id, reply})
    await_role!(group, :leader)
  end

  def await_candidate!(group) do
    await_role!(group, :candidate)

    status(group).term
  end

  def whereis_server(group), do: group |> Server.name() |> Process.whereis()

  def await_restart!(group, old) do
    wait_until!(fn ->
      pid = whereis_server(group)
      pid != nil and pid != old
    end)
  end

  def recorded_effects do
    voters = ~w[m1 m2 m3]
    ref = make_ref()
    later = make_ref()
    lost = make_ref()
    follower = follower_with_log!("m2", 1, entries([{1, 1}, {2, 1}]))
    conflict = append(2, 1, 1, entries([{2, 2}]), 0)

    ticked =
      voters
      |> then(&new_node("m1", &1))
      |> then(&tick!(&1, &1.election_timeout - 1))

    {candidate, elect} = Node.step(ticked, :tick)
    vote = %RequestVoteReply{term: candidate.term, from: "m2", to: "m1", granted?: true}
    {leader, won} = Node.step(candidate, {:message, vote})
    {durable, synced} = Node.step(leader, {:persisted, leader.persist_seq})
    {proposed, proposal} = Node.step(durable, {:propose, ref, "x", 100})
    {stored, stored_effects} = Node.step(proposed, {:persisted, proposed.persist_seq})
    {_committed, commit} = ack(stored, "m2", 2)
    {pending, queued} = Node.step(stored, {:propose, later, "y", 110})
    {_down, stepped_down} = Node.step(pending, {:message, request_vote("m3", "m1", 9)})
    {_refused, refusal} = Node.step(follower, {:propose, lost, "z", 1})
    {_truncated, truncation} = Node.step(follower, {:message, conflict})

    meta = snapshot_meta(5, 1)

    snapshots = [
      {:take_snapshot, 7},
      {:compact_log, 3},
      {:store_snapshot_chunk, meta, 0, "abc", false},
      {:install_snapshot, meta}
    ]

    [
      elect,
      won,
      synced,
      proposal,
      stored_effects,
      commit,
      queued,
      stepped_down,
      refusal,
      truncation,
      snapshots
    ]
  end

  def effect_kinds(lists) do
    kinds = for list <- lists, effect <- list, do: elem(effect, 0)

    kinds |> Enum.uniq() |> Enum.sort()
  end

  def recording_state(lists) do
    group = random_group()
    TransportStub.listen(group)
    members = %{"m1" => node(), "m2" => node(), "m3" => node()}
    refs = for list <- lists, effect <- list, ref <- effect_refs(effect), do: ref
    waiters = Map.new(refs, fn ref -> {ref, {self(), ref}} end)
    opts = [group: group, member_id: "m1", members: members, cluster_id: @cluster_id]

    recovered = %Result{
      hard_state: %HardState{term: 0, voted_for: nil, commit: 0},
      entries: [],
      last_segment: nil,
      next_seq: 1,
      tail: [],
      dropped: [],
      torn_tail?: false
    }

    ports = [
      writer: self(),
      applier: self(),
      transport: TransportStub,
      receiver: self(),
      sender_supervisor: self(),
      disk: Posix.new(),
      snap_dir: "/unused"
    ]

    merged = Keyword.merge(opts, ports)
    state = State.new(merged, recovered)

    %State{state | waiters: waiters}
  end

  def expected_ports(state, effects), do: Enum.flat_map(effects, &expected_port(state, &1))

  def received_ports do
    receive do
      {:"$gen_cast", cast} -> [cast_port(cast) | received_ports()]
      {:sent, to, message} -> [{:transport, to, message} | received_ports()]
      {ref, reply} when is_reference(ref) -> [{:reply, ref, reply} | received_ports()]
    after
      0 -> []
    end
  end

  def port_mismatches(lists) do
    state = recording_state(lists)

    Enum.reject(lists, fn effects ->
      Interpreter.run(state, effects)

      received_ports() == expected_ports(state, effects)
    end)
  end

  defp effect_refs({:apply, _entries, refs}), do: Map.values(refs)
  defp effect_refs({:reply, ref, _result}), do: [ref]
  defp effect_refs(_effect), do: []

  defp expected_port(_state, {:persist, seq, hard_state, entries}),
    do: [{:writer, :persist, seq, hard_state, entries}]

  defp expected_port(_state, {:truncate_from, index}), do: [{:writer, :truncate_from, index}]
  defp expected_port(_state, {:compact_log, keep}), do: [{:writer, :compact_log, keep}]
  defp expected_port(_state, {:take_snapshot, index}), do: [{:applier, :take_snapshot, index}]
  defp expected_port(_state, {:install_snapshot, meta}), do: [{:applier, :install_snapshot, meta}]

  defp expected_port(_state, {:store_snapshot_chunk, meta, offset, data, done?}),
    do: [{:receiver, :chunk, meta, offset, data, done?}]

  defp expected_port(_state, {:send, message}), do: [{:transport, message.to, message}]

  defp expected_port(_state, {:apply, entries, refs}) do
    callers = Map.new(refs, fn {index, ref} -> {index, {self(), ref}} end)

    [{:applier, entries, callers}]
  end

  defp expected_port(state, {:reply, ref, {:error, {:not_leader, leader}}}),
    do: [{:reply, ref, {:refused, {:not_leader, State.address(state, leader)}}}]

  defp expected_port(_state, {:reply, ref, {:error, error}}),
    do: [{:reply, ref, {:refused, error}}]

  defp expected_port(_state, {:role_changed, _role, _term, _leader}), do: []

  defp cast_port({:persist, seq, hard_state, entries}),
    do: {:writer, :persist, seq, hard_state, entries}

  defp cast_port({:truncate_from, index}), do: {:writer, :truncate_from, index}
  defp cast_port({:compact_log, keep}), do: {:writer, :compact_log, keep}
  defp cast_port({:take_snapshot, index}), do: {:applier, :take_snapshot, index}
  defp cast_port({:install_snapshot, meta}), do: {:applier, :install_snapshot, meta}

  defp cast_port({:chunk, meta, offset, data, done?}),
    do: {:receiver, :chunk, meta, offset, data, done?}

  defp cast_port({:apply, entries, callers}), do: {:applier, entries, callers}

  def first_of_two! do
    wait_until!(fn -> Process.info(self(), :message_queue_len) == {:message_queue_len, 2} end)

    receive do
      message -> message
    end
  end

  def received_terms(count) do
    Enum.map(1..count, fn _number ->
      receive do
        {:keel, _cluster, message} -> message.term
      end
    end)
  end

  def start_transport!(group, destination) do
    opts = [group: group, cluster_id: @cluster_id, destination: destination]

    start_supervised!(Dist.child_spec(opts))
    Process.register(self(), destination)
    :ok = Dist.update_members(group, %{"b" => node()})
    registry = DistSupervisor.registry(group)
    [{peer, _value}] = Registry.lookup(registry, "b")
    :sys.get_state(peer)

    :ok
  end

  def start_proxy! do
    group = random_group()
    TransportStub.listen(group)
    start_supervised!(FaultProxy.child_spec(group: group, inner: TransportStub))

    group
  end

  def change!(leader, change) do
    ref = make_ref()
    {changed, effects} = Node.step(leader, {:change_membership, ref, change})

    {changed, ref, effects}
  end

  def persisted!(node) do
    {durable, _effects} = Node.step(node, {:persisted, node.persist_seq})

    durable
  end

  def committed_change!(leader, change, from) do
    {changed, _ref, _effects} = change!(leader, change)
    durable = persisted!(changed)
    {committed, _effects} = ack(durable, from, durable.log.last_index)

    committed
  end

  def learner_leader!(id \\ "m1") do
    leader = committed_leader!(id, ~w[m1 m2 m3])

    committed_change!(leader, {:add_learner, "m4"}, "m2")
  end

  def config_entry(index, term, config),
    do: %Entry{
      index: index,
      term: term,
      kind: :config,
      data: Config.encode(config),
      time_ms: index
    }

  def random_config(opts \\ []) do
    voters = Keyword.get(opts, :voters, ~w[m1 m2 m3])
    learners = Keyword.get(opts, :learners, [])
    removed = Keyword.get(opts, :removed, [])
    addresses = Keyword.get(opts, :addresses, %{})

    %Config{
      voters: MapSet.new(voters),
      learners: MapSet.new(learners),
      removed: MapSet.new(removed),
      addresses: addresses
    }
  end

  def learner_node!(id, opts \\ []) do
    config = random_config(Keyword.put_new(opts, :voters, ~w[m1 m2 m3]))
    entry = config_entry(1, 0, %{config | learners: MapSet.new([id])})
    hard_state = %HardState{term: 0, voted_for: nil, commit: 0}

    node =
      Node.new(%{id: id, voters: [], seed: {1, 2, 3}, hard_state: hard_state, entries: [entry]})

    %{node | persist_seq: 1, durable_seq: 1}
  end

  def append_config!(follower, config, term \\ 1) do
    prev = follower.log.last_index
    entry = config_entry(prev + 1, term, config)
    message = append(term, prev, Log.term_at(follower.log, prev), [entry], 0)
    {appended, _effects} = Node.step(follower, {:message, message})

    appended
  end
end
