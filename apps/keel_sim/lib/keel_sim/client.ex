defmodule KeelSim.Client do
  @moduledoc "The model never reads a clock so each call carries the virtual time"

  alias Keel.KV
  alias KeelSim.Client.Op
  alias KeelSim.History
  alias KeelSim.KvCheck

  @timeout_ms 3_000
  @max_attempts 3

  @enforce_keys [:slot]
  defstruct [:slot, :session, :pending, seq: 0, counter: 0, generation: 0, revisions: %{}]

  @type key_ref ::
          {:shared, non_neg_integer()} | {:own, non_neg_integer()} | {:round, non_neg_integer()}

  @type choice :: {Op.kind(), key_ref(), String.t()}

  @type pending :: %{
          type: :register | :op,
          seq: non_neg_integer(),
          data: binary(),
          attempts: pos_integer(),
          deadline: non_neg_integer(),
          op: nil | Op.t(),
          query: nil | KV.Command.op()
        }

  @type t :: %__MODULE__{
          slot: pos_integer(),
          session: nil | pos_integer(),
          pending: nil | pending(),
          seq: non_neg_integer(),
          counter: non_neg_integer(),
          generation: non_neg_integer(),
          revisions: %{binary() => non_neg_integer()}
        }

  @type output ::
          {:event, History.event()}
          | {:send, String.t(), binary(), non_neg_integer()}
          | {:query, String.t(), KV.Command.op(), non_neg_integer()}

  @type reply :: {:result, KV.Result.t()} | {:refused, term()}

  @spec new(pos_integer()) :: t()
  def new(slot), do: %__MODULE__{slot: slot}

  @spec pending?(t()) :: boolean()
  def pending?(%__MODULE__{pending: pending}), do: pending != nil

  @spec ready?(t()) :: boolean()
  def ready?(%__MODULE__{pending: nil, session: session}), do: session != nil
  def ready?(%__MODULE__{}), do: false

  @doc "A client that waits past its deadline sends again or gives up."
  @spec step(t(), non_neg_integer(), choice()) :: {t(), [output()]}
  def step(%__MODULE__{pending: nil, session: nil} = client, now, {_kind, _ref, target}),
    do: register(client, now, target)

  def step(%__MODULE__{pending: nil} = client, now, choice), do: begin(client, now, choice)

  def step(%__MODULE__{pending: %{deadline: deadline}} = client, now, {_kind, _ref, target})
      when now >= deadline,
      do: retry(client, now, target)

  def step(%__MODULE__{} = client, _now, _choice), do: {client, []}

  @spec reply(t(), non_neg_integer(), reply(), non_neg_integer()) :: {t(), [output()]}
  def reply(%__MODULE__{pending: %{type: :register}} = client, 0, reply, _now),
    do: {registered(client, reply), []}

  def reply(%__MODULE__{pending: %{type: :op, seq: seq} = pending} = client, seq, reply, now),
    do: settle(client, pending, reply, now)

  def reply(%__MODULE__{} = client, _seq, _reply, _now), do: {client, []}

  @spec finish(t(), non_neg_integer()) :: {t(), [output()]}
  def finish(%__MODULE__{pending: %{type: :op} = pending} = client, now),
    do: conclude(client, pending, :info, {:error, :unfinished}, now)

  def finish(%__MODULE__{} = client, _now), do: {%{client | pending: nil}, []}

  @spec key(t(), key_ref()) :: binary()
  def key(%__MODULE__{}, {:shared, number}), do: "s/#{number}"
  def key(%__MODULE__{slot: slot}, {:own, number}), do: "p/#{slot}/#{number}"
  def key(%__MODULE__{}, {:round, number}), do: "r/#{number}"

  defp register(client, now, target) do
    data = KV.register_request()

    pending = %{
      type: :register,
      seq: 0,
      data: data,
      attempts: 1,
      deadline: now + @timeout_ms,
      op: nil,
      query: nil
    }

    {%{client | pending: pending}, [{:send, target, data, 0}]}
  end

  defp registered(client, {:result, {:ok, id}}), do: %{client | session: id, pending: nil}
  defp registered(client, _reply), do: %{client | pending: nil}

  defp begin(client, now, {kind, key_ref, target}) do
    key = key(client, key_ref)
    counter = client.counter + 1
    seq = client.seq + 1

    op = %Op{
      process: client.slot + client.generation * 1_000,
      kind: kind,
      key: key,
      invoked_at: now,
      value: value(kind, client.slot, counter),
      expected: expected(kind, client.revisions, key)
    }

    session = %{client_id: client.session, seq: seq, ack: seq}
    data = KV.request(session, command(op))
    query = read_query(op)

    pending = %{
      type: :op,
      seq: seq,
      data: data,
      attempts: 1,
      deadline: now + @timeout_ms,
      op: op,
      query: query
    }

    began = %{client | pending: pending, seq: seq, counter: counter}

    {began, [{:event, {:invoke, now, op}}, transmit(pending, target)]}
  end

  defp read_query(%Op{kind: :get} = op), do: command(op)
  defp read_query(%Op{}), do: nil

  # A read asks the member directly and never enters the log
  defp transmit(%{query: nil} = pending, target), do: {:send, target, pending.data, pending.seq}
  defp transmit(%{query: query} = pending, target), do: {:query, target, query, pending.seq}

  defp value(kind, slot, counter) when kind in [:put, :compare_and_set],
    do: KvCheck.value(slot, counter)

  defp value(_kind, _slot, _counter), do: nil

  defp expected(:compare_and_set, revisions, key), do: Map.get(revisions, key, 0)
  defp expected(_kind, _revisions, _key), do: nil

  defp command(%Op{kind: :put, key: key, value: value}), do: {:put, key, value}
  defp command(%Op{kind: :get, key: key}), do: {:get, key}
  defp command(%Op{kind: :delete, key: key}), do: {:delete, key}

  defp command(%Op{kind: :compare_and_set} = op),
    do: {:compare_and_set, op.key, op.expected, {:put, op.value}}

  defp retry(%__MODULE__{pending: %{type: :register} = pending} = client, now, target),
    do: resend(client, pending, now, target)

  defp retry(%__MODULE__{pending: %{attempts: attempts} = pending} = client, now, target)
       when attempts < @max_attempts,
       do: resend(client, pending, now, target)

  defp retry(%__MODULE__{pending: pending} = client, now, _target),
    do: conclude(client, pending, :info, {:error, :timeout}, now)

  defp resend(client, pending, now, target) do
    again = %{pending | attempts: pending.attempts + 1, deadline: now + @timeout_ms}

    {%{client | pending: again}, [transmit(pending, target)]}
  end

  # A session that is gone makes every earlier try unknown unless this was the first
  defp settle(client, pending, {:result, {:error, reason}}, now)
       when reason in [:session_expired, :stale_command] do
    status = if pending.attempts == 1, do: :fail, else: :info

    conclude(%{client | session: nil}, pending, status, {:error, reason}, now)
  end

  defp settle(client, pending, {:result, {:error, {:revision_mismatch, _current}} = result}, now),
    do: conclude(client, pending, :ok, result, now)

  defp settle(client, pending, {:result, {:error, _reason} = result}, now),
    do: conclude(client, pending, :fail, result, now)

  defp settle(client, pending, {:result, result}, now),
    do: conclude(client, pending, :ok, result, now)

  defp settle(client, %{attempts: attempts} = pending, {:refused, {:not_leader, leader}}, now)
       when leader != nil and attempts < @max_attempts,
       do: resend(client, pending, now, leader)

  defp settle(client, %{attempts: 1} = pending, {:refused, {:not_leader, nil}}, now),
    do: conclude(client, pending, :fail, {:error, :not_leader}, now)

  defp settle(client, pending, {:refused, :leadership_lost}, now),
    do: {%{client | pending: %{pending | deadline: now}}, []}

  defp settle(client, %{query: query} = pending, {:refused, _reason}, now) when query != nil,
    do: {%{client | pending: %{pending | deadline: now}}, []}

  defp settle(client, _pending, {:refused, _reason}, _now), do: {client, []}

  defp conclude(client, pending, status, result, now) do
    %Op{} = invoked = pending.op
    op = %Op{invoked | status: status, result: result, completed_at: now}
    generation = if status == :info, do: client.generation + 1, else: client.generation
    learned = learn(client, op)

    {%{learned | pending: nil, generation: generation}, [{:event, {status, now, op}}]}
  end

  defp learn(client, %Op{status: :ok, kind: :delete, key: key}),
    do: %{client | revisions: Map.put(client.revisions, key, 0)}

  defp learn(client, %Op{status: :ok, key: key, result: result}) do
    case revision(result) do
      nil -> client
      revision -> %{client | revisions: Map.put(client.revisions, key, revision)}
    end
  end

  defp learn(client, %Op{}), do: client

  defp revision({:ok, nil}), do: 0
  defp revision({:ok, {_value, revision}}), do: revision
  defp revision({:ok, revision}), do: revision
  defp revision({:error, {:revision_mismatch, current}}), do: current
  defp revision(_result), do: nil
end
