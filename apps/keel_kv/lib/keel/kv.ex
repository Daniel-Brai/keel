defmodule Keel.KV do
  @moduledoc "A `:timeout` or `:leadership_lost` reply means the command may have been applied"

  default = [Keel, Keel.Cluster, Keel.Common, Keel.Error, Keel.Machine, Keel.Node, Keel.Raft]

  deps =
    case Mix.env() do
      :test -> [Keel.Env | default]
      _env -> default
    end

  use Boundary,
    top_level?: true,
    deps: deps,
    exports: [CLI, Command, Limits, Machine, Result, Snapshot]

  alias Keel.Error
  alias Keel.KV.Check
  alias Keel.KV.Command
  alias Keel.KV.Result
  alias Keel.KV.Writer

  @type key :: binary()

  @type value :: binary()

  @type revision :: non_neg_integer()

  @type item :: %{key: key(), value: value(), revision: revision()}

  @type page :: %{records: [item()], next: nil | key()}

  @type option :: {:timeout, timeout()} | {:read, :index | :log}

  @type opts :: [option()]

  @type change :: {:put, value()} | :delete

  @type session :: %{client_id: pos_integer(), seq: pos_integer(), ack: pos_integer()}

  @type written :: {:ok, revision()} | {:error, Error.reason()}

  @type paged :: {:ok, page()} | {:error, Error.reason()}

  @spec register_request() :: binary()
  def register_request, do: Command.encode(%Command{client_id: 0, seq: 0, ack: 0, op: :register})

  @spec request(session(), Command.op()) :: binary()
  def request(%{client_id: client_id, seq: seq, ack: ack}, op),
    do: Command.encode(%Command{client_id: client_id, seq: seq, ack: ack, op: op})

  @spec decode_result(binary()) :: {:ok, Result.t()} | {:error, :malformed_result}
  def decode_result(bytes), do: Result.decode(bytes)

  @spec start_writer(atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_writer(group, opts \\ []) do
    spec = {Writer, Keyword.put(opts, :group, group)}

    DynamicSupervisor.start_child(Keel.KV.Writers, spec)
  end

  @spec put(atom(), key(), value(), opts()) :: {:ok, revision()} | {:error, Error.reason()}
  def put(group, key, value, opts \\ []) do
    with :ok <- Check.key(key),
         :ok <- Check.value(value) do
      Writer.submit(group, {:put, key, value}, opts)
    end
  end

  @spec delete(atom(), key(), opts()) :: {:ok, revision()} | {:error, Error.reason()}
  def delete(group, key, opts \\ []) do
    with :ok <- Check.key(key) do
      Writer.submit(group, {:delete, key}, opts)
    end
  end

  @spec compare_and_set(atom(), key(), revision(), change(), opts()) :: written()
  def compare_and_set(group, key, expected, change, opts \\ []) do
    with :ok <- Check.key(key),
         :ok <- Check.change(change) do
      Writer.submit(group, {:compare_and_set, key, expected, change}, opts)
    end
  end

  @spec get(atom(), key(), opts()) :: {:ok, nil | item()} | {:error, Error.reason()}
  def get(group, key, opts \\ []) do
    with :ok <- Check.key(key),
         {:ok, found} <- fetch(group, {:get, key}, opts) do
      {:ok, record(key, found)}
    end
  end

  @doc "The keys from `from` up to but not including `to`. A `nil` bound is open."
  @spec range(atom(), nil | key(), nil | key(), pos_integer(), opts()) :: paged()
  def range(group, from, to, limit, opts \\ []) do
    lower = lower_bound(from)

    with {:ok, scanned} <- fetch(group, {:range, lower, to, limit}, opts) do
      {:ok, page(scanned)}
    end
  end

  @spec prefix(atom(), key(), nil | key(), pos_integer(), opts()) :: paged()
  def prefix(group, prefix, after_key, limit, opts \\ []) do
    with {:ok, scanned} <- fetch(group, {:prefix, prefix, after_key, limit}, opts) do
      {:ok, page(scanned)}
    end
  end

  # Reads confirm leadership without a log entry unless `read: :log` asks for one
  defp fetch(group, op, opts) do
    case Keyword.get(opts, :read, :index) do
      :index -> Writer.read(group, op, opts)
      :log -> Writer.submit(group, op, opts)
    end
  end

  defp record(_key, nil), do: nil
  defp record(key, {value, revision}), do: %{key: key, value: value, revision: revision}

  defp page({rows, next}) do
    records =
      Enum.map(rows, fn {key, value, revision} ->
        %{key: key, value: value, revision: revision}
      end)

    %{records: records, next: next}
  end

  defp lower_bound(nil), do: <<>>
  defp lower_bound(key), do: key
end
