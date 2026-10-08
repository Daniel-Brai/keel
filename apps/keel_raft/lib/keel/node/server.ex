defmodule Keel.Node.Server do
  use GenServer

  alias Keel.Common.Name
  alias Keel.Common.T
  alias Keel.Log.Writer
  alias Keel.Node.Book
  alias Keel.Node.Interpreter
  alias Keel.Node.State
  alias Keel.Node.Waiters
  alias Keel.Raft.Config
  alias Keel.Raft.Node
  alias Keel.Transport.Envelope

  require Logger

  @type group :: atom()

  @type status :: %{
          member_id: String.t(),
          node: node(),
          role: T.role(),
          term: non_neg_integer(),
          leader: State.address(),
          commit: non_neg_integer(),
          applied: non_neg_integer(),
          first_index: non_neg_integer(),
          last_index: non_neg_integer(),
          dropped: non_neg_integer(),
          members: Keel.Transport.members(),
          voters: [String.t()],
          learners: [String.t()]
        }

  @spec name(group()) :: atom()
  def name(group), do: Name.build("keel", group)

  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)
    name = name(group)

    GenServer.start_link(__MODULE__, opts, name: name, spawn_opt: [message_queue_data: :off_heap])
  end

  @spec status(GenServer.server()) :: status()
  def status(server), do: GenServer.call(server, :status)

  @spec read(GenServer.server(), term(), timeout()) :: term()
  def read(server, query, timeout), do: GenServer.call(server, {:read, query}, timeout)

  @spec transfer_leadership(GenServer.server(), T.member_id(), timeout()) :: term()
  def transfer_leadership(server, target, timeout),
    do: GenServer.call(server, {:transfer, target}, timeout)

  @spec change_membership(GenServer.server(), Config.change(), timeout()) :: term()
  def change_membership(server, change, timeout),
    do: GenServer.call(server, {:change, change}, timeout)

  @impl GenServer
  def init(opts) do
    writer = Keyword.fetch!(opts, :writer)
    recovered = Writer.recovered(writer)
    opened? = Writer.opened?(writer)
    state = %State{State.new(opts, recovered) | joining?: not opened?}

    :ok = state.transport.update_members(state.group, state.members)
    schedule_tick(state)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:propose, data}, from, %State{} = state) do
    if Waiters.size(state.waiters) >= state.max_pending do
      {:reply, {:refused, :overloaded}, state}
    else
      ref = make_ref()
      time_ms = state.env.clock.wall_ms()
      waiters = Waiters.add(state.waiters, ref, from)

      step(%State{state | waiters: waiters}, {:propose, ref, data, time_ms})
    end
  end

  def handle_call({:read, query}, from, %State{} = state) do
    if Waiters.size(state.waiters) >= state.max_pending do
      {:reply, {:refused, :overloaded}, state}
    else
      ref = make_ref()
      waiters = Waiters.add(state.waiters, ref, from)
      reads = Map.put(state.reads, ref, query)

      step(%State{state | waiters: waiters, reads: reads}, {:read_index, ref})
    end
  end

  def handle_call({:transfer, target}, from, %State{} = state) do
    ref = make_ref()
    waiters = Waiters.add(state.waiters, ref, from)

    step(%State{state | waiters: waiters}, {:transfer_leadership, ref, target})
  end

  def handle_call({:change, change}, from, %State{} = state) do
    ref = make_ref()
    waiters = Waiters.add(state.waiters, ref, from)

    step(%State{state | waiters: waiters}, {:change_membership, ref, change})
  end

  def handle_call(:status, _from, %State{} = state), do: {:reply, status_of(state), state}

  # A joining member writes its identity when the first leader message arrives
  @impl GenServer
  def handle_info({:keel, sent_id, message}, %State{joining?: true} = state) do
    case Envelope.check_join(message, sent_id, state.cluster_id, state.member_id) do
      :ok -> join(state, message)
      {:error, :invalid} -> {:noreply, drop(state)}
    end
  end

  def handle_info({:keel, sent_id, message}, %State{} = state) do
    removed = state.core.config.removed

    case Envelope.check(message, sent_id, state.cluster_id, state.member_id, removed) do
      :ok -> step(state, {:message, message})
      {:error, :invalid} -> {:noreply, drop(state)}
    end
  end

  def handle_info(:tick, %State{} = state) do
    schedule_tick(state)
    check_mailbox(state)

    step(state, :tick)
  end

  def handle_info({:persisted, seq}, %State{} = state), do: step(state, {:persisted, seq})

  def handle_info({:applied, index}, %State{} = state) do
    {:noreply, stepped} = step(%State{state | applied: index}, {:applied, index})
    released = Interpreter.release_reads(stepped)

    {:noreply, released}
  end

  def handle_info({:snapshot_taken, meta}, %State{} = state),
    do: step(state, {:snapshot_taken, meta})

  def handle_info({:snapshot_received, meta}, %State{} = state),
    do: step(state, {:snapshot_received, meta})

  def handle_info(:snapshot_failed, %State{} = state), do: step(state, :snapshot_failed)

  defp join(%State{} = state, message) do
    :ok = Writer.open(state.writer)

    step(%State{state | joining?: false}, {:message, message})
  end

  defp step(%State{} = state, event) do
    before = state.core.config
    {core, effects} = Node.step(state.core, event)
    stepped = %State{state | core: core}
    ran = Interpreter.run(stepped, effects)

    {:noreply, follow(ran, before)}
  end

  # A new member is known as soon as its config entry is in the log
  defp follow(%State{core: %{config: config}} = state, config), do: state

  defp follow(%State{} = state, _before) do
    members = Book.follow(state.members, state.core.config)

    Interpreter.update_members(state, members)
  end

  defp schedule_tick(%State{} = state),
    do: state.env.clock.send_after(self(), :tick, state.tick_ms)

  defp check_mailbox(%State{} = state) do
    {:message_queue_len, length} = Process.info(self(), :message_queue_len)

    if length > state.max_mailbox do
      Logger.warning("member #{state.member_id} has #{length} messages waiting")
    end
  end

  defp drop(%State{} = state), do: %State{state | dropped: state.dropped + 1}

  defp status_of(%State{core: core} = state) do
    %{
      member_id: state.member_id,
      node: node(),
      role: core.role,
      term: core.term,
      leader: State.address(state, core.leader_id),
      commit: core.commit,
      applied: state.applied,
      first_index: core.log.first_index,
      last_index: core.log.last_index,
      dropped: state.dropped,
      members: state.members,
      voters: Config.voters(core.config),
      learners: Config.learners(core.config)
    }
  end
end
