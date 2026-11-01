defmodule Keel.KV.Writer do
  use GenServer

  alias Keel.Common.Name
  alias Keel.Error
  alias Keel.KV
  alias Keel.KV.Command
  alias Keel.KV.Outcome
  alias Keel.KV.Writer.Call
  alias Keel.KV.Writer.State
  alias Keel.Node.Server

  @call_timeout_ms 5_000
  @margin_ms 1_000

  @type result :: {:ok, term()} | {:error, Error.reason()}

  @spec name(atom()) :: atom()
  def name(group), do: Name.build("keel_kv_writer", group)

  def child_spec(opts) do
    group = Keyword.fetch!(opts, :group)

    %{id: {__MODULE__, group}, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)

    GenServer.start_link(__MODULE__, opts, name: name(group))
  end

  @doc "Past the `:timeout` deadline the caller gets `:unknown_outcome` if the command may have been applied"
  @spec submit(atom(), Command.op(), keyword()) :: result()
  def submit(group, op, opts) do
    timeout = Keyword.get(opts, :timeout, @call_timeout_ms)

    with {:ok, writer} <- ensure_started(group, opts) do
      call(writer, op, timeout)
    end
  end

  @doc "A read needs no session because asking again is safe."
  @spec read(atom(), Command.op(), keyword()) :: result()
  def read(group, query, opts) do
    timeout = Keyword.get(opts, :timeout, @call_timeout_ms)

    with {:ok, writer} <- ensure_started(group, opts) do
      ask(writer, query, timeout)
    end
  end

  defp ask(writer, query, timeout) do
    GenServer.call(writer, {:read, query, timeout}, timeout + @margin_ms)
  catch
    :exit, _reason -> {:error, :no_leader}
  end

  defp call(writer, op, timeout) do
    GenServer.call(writer, {:submit, op, timeout}, timeout + @margin_ms)
  catch
    :exit, _reason -> {:error, :unknown_outcome}
  end

  defp ensure_started(group, opts) do
    name = name(group)
    writer_opts = Keyword.get(opts, :writer, [])
    start_opts = Keyword.put(writer_opts, :group, group)

    case DynamicSupervisor.start_child(Keel.KV.Writers, {__MODULE__, start_opts}) do
      {:ok, _pid} -> {:ok, name}
      {:error, {:already_started, _pid}} -> {:ok, name}
      {:error, _reason} -> {:error, :unknown_outcome}
    end
  end

  @impl GenServer
  def init(opts) do
    group = Keyword.fetch!(opts, :group)

    state = %State{
      group: group,
      nodes: Keyword.get(opts, :nodes),
      route: Keyword.get(opts, :route, &route(group, &1)),
      target: nil,
      client_id: nil,
      next_seq: 1,
      outstanding: :gb_sets.new(),
      calls: %{},
      reqids: :gen_server.reqids_new(),
      counter: 0,
      heard_ms: nil,
      attempt_timeout_ms: Keyword.get(opts, :attempt_timeout_ms, 1_000),
      backoff_ms: Keyword.get(opts, :retry_backoff_ms, 20),
      backoff_max_ms: Keyword.get(opts, :retry_backoff_max_ms, 250),
      max_redirects: Keyword.get(opts, :max_redirects, 8)
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:submit, op, timeout}, from, %State{} = state) do
    id = state.counter + 1
    timer = Process.send_after(self(), {:deadline, id}, timeout)
    call = Call.new(id, from, op, timer)
    counted = %State{state | counter: id}

    {:noreply, admit(counted, call)}
  end

  def handle_call({:read, query, timeout}, from, %State{} = state) do
    id = state.counter + 1
    timer = Process.send_after(self(), {:deadline, id}, timeout)
    call = %Call{Call.new(id, from, query, timer) | read?: true}
    counted = %State{state | counter: id}

    {:noreply, dispatch(counted, call)}
  end

  @impl GenServer
  def handle_info({:deadline, id}, %State{} = state), do: {:noreply, expire(state, id)}

  def handle_info({:retry, id, attempt}, %State{} = state),
    do: {:noreply, resend(state, id, attempt)}

  def handle_info({:attempt_timeout, id, attempt}, %State{} = state),
    do: {:noreply, timed_out(state, id, attempt)}

  def handle_info(message, %State{} = state) do
    case :gen_server.check_response(message, state.reqids, true) do
      {response, {id, attempt}, reqids} ->
        heard = System.monotonic_time(:millisecond)
        updated = %State{state | reqids: reqids, heard_ms: heard}

        {:noreply, answer(updated, id, attempt, input(response))}

      _other ->
        {:noreply, state}
    end
  end

  defp route(group, node), do: {Server.name(group), node}

  defp input({:reply, reply}), do: {:reply, reply}
  defp input({:error, {reason, _server}}), do: {:down, reason}

  defp admit(%State{client_id: nil} = state, %Call{} = call) do
    queued = State.put_call(state, call)

    register(queued)
  end

  defp admit(%State{} = state, %Call{} = call), do: send_new(state, call)

  defp register(%State{calls: %{register: _call}} = state), do: state

  defp register(%State{} = state) do
    call = Call.new(:register, nil, :register, nil)
    bytes = KV.register_request()
    ready = %Call{call | bytes: bytes}

    dispatch(state, ready)
  end

  defp send_new(%State{} = state, %Call{} = call) do
    seq = state.next_seq
    outstanding = :gb_sets.add(seq, state.outstanding)
    session = %{client_id: state.client_id, seq: seq, ack: :gb_sets.smallest(outstanding)}
    bytes = KV.request(session, call.op)
    numbered = %State{state | next_seq: seq + 1, outstanding: outstanding}

    dispatch(numbered, %Call{call | seq: seq, bytes: bytes})
  end

  defp dispatch(%State{} = state, %Call{} = call) do
    target = State.target(state)
    server = state.route.(target)
    attempt = call.attempt + 1
    label = {call.id, attempt}
    request = request(call)
    reqids = :gen_server.send_request(server, request, label, state.reqids)

    Process.send_after(self(), {:attempt_timeout, call.id, attempt}, state.attempt_timeout_ms)

    waiting = %Call{call | attempt: attempt, status: :waiting}

    State.put_call(%State{state | reqids: reqids, target: target}, waiting)
  end

  defp request(%Call{read?: true, op: op}), do: {:read, op}
  defp request(%Call{bytes: bytes}), do: {:propose, bytes}

  # A target that still answers other calls is slow and not gone. Sending again would only add a copy
  defp timed_out(%State{heard_ms: nil} = state, id, attempt),
    do: answer(state, id, attempt, :timeout)

  defp timed_out(%State{} = state, id, attempt) do
    quiet_ms = System.monotonic_time(:millisecond) - state.heard_ms

    case quiet_ms < state.attempt_timeout_ms do
      true -> wait_again(state, id, attempt)
      false -> answer(state, id, attempt, :timeout)
    end
  end

  defp wait_again(%State{} = state, id, attempt) do
    case Map.fetch(state.calls, id) do
      {:ok, %Call{attempt: ^attempt, status: :waiting}} -> rearm(state, id, attempt)
      _done -> state
    end
  end

  defp rearm(%State{} = state, id, attempt) do
    Process.send_after(self(), {:attempt_timeout, id, attempt}, state.attempt_timeout_ms)

    state
  end

  defp answer(%State{} = state, id, attempt, input) do
    case Map.fetch(state.calls, id) do
      {:ok, %Call{attempt: ^attempt, status: :waiting} = call} ->
        act(state, call, Outcome.classify(input))

      _stale ->
        state
    end
  end

  defp act(%State{} = state, %Call{id: :register}, {:done, {:ok, client_id}}) do
    registered = %State{state | client_id: client_id, calls: Map.delete(state.calls, :register)}

    registered
    |> State.queued()
    |> Enum.sort_by(& &1.id)
    |> Enum.reduce(registered, &send_new(&2, &1))
  end

  defp act(%State{} = state, %Call{} = call, {:done, result}) do
    finished = finish(state, call, result)

    if call.id == :register, do: retry_later(state, call, refusal()), else: finished
  end

  defp act(%State{} = state, %Call{}, :session_lost), do: lose_session(state)

  defp act(%State{} = state, %Call{redirects: redirects} = call, {:redirect, node}) do
    if redirects >= state.max_redirects do
      retry_later(state, call, refusal())
    else
      redirected = %State{state | target: node}

      dispatch(redirected, %Call{call | redirects: redirects + 1})
    end
  end

  defp act(%State{} = state, %Call{} = call, {:retry, retry}) do
    moved = if retry.rotate?, do: State.rotate(state), else: state

    retry_later(moved, call, retry)
  end

  defp refusal, do: %{rotate?: true, maybe_applied?: false, refusal: :no_leader}

  defp retry_later(state, %Call{} = call, retry) do
    retried = Call.retried(call, retry)
    wait = State.backoff(state, call.attempt)

    Process.send_after(self(), {:retry, call.id, call.attempt}, wait)

    State.put_call(state, retried)
  end

  defp resend(%State{} = state, id, attempt) do
    case Map.fetch(state.calls, id) do
      {:ok, %Call{attempt: ^attempt, status: :backoff} = call} -> dispatch(state, call)
      _stale -> state
    end
  end

  defp expire(%State{} = state, id) do
    case Map.fetch(state.calls, id) do
      {:ok, %Call{} = call} ->
        state |> finish(call, {:error, Call.deadline_error(call)}) |> drop_idle_register()

      :error ->
        state
    end
  end

  defp drop_idle_register(%State{} = state) do
    if State.queued(state) == [],
      do: %State{state | calls: Map.delete(state.calls, :register)},
      else: state
  end

  defp finish(%State{} = state, %Call{} = call, result) do
    if call.from, do: GenServer.reply(call.from, result)
    if call.timer, do: Process.cancel_timer(call.timer)

    outstanding =
      if call.seq, do: :gb_sets.del_element(call.seq, state.outstanding), else: state.outstanding

    %State{state | calls: Map.delete(state.calls, call.id), outstanding: outstanding}
  end

  # Every command sent under the lost session may or may not have run
  defp lose_session(%State{} = state) do
    sent = State.sent(state)
    %State{} = lost = Enum.reduce(sent, state, &finish(&2, &1, {:error, :unknown_outcome}))
    reset = %State{lost | client_id: nil, outstanding: :gb_sets.new()}

    if State.queued(reset) == [], do: reset, else: register(reset)
  end
end
