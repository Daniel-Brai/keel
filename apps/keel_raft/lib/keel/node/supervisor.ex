defmodule Keel.Node.Supervisor do
  use Supervisor

  alias Keel.Common.Name
  alias Keel.Env.Disk.Posix
  alias Keel.Log.Store
  alias Keel.Log.Writer
  alias Keel.Node.Applier
  alias Keel.Node.Server
  alias Keel.Node.SnapshotReceiver
  alias Keel.Raft.Config
  alias Keel.Transport.Dist

  @spec name(atom()) :: atom()
  def name(group), do: Name.build("keel", group, "_supervisor")

  @spec senders_name(atom()) :: atom()
  def senders_name(group), do: Name.build("keel", group, "_snapshot_senders")

  @spec writer_name(atom()) :: atom()
  def writer_name(group), do: Name.build("keel", group, "_writer")

  def child_spec(opts) do
    %{
      id: __MODULE__,
      type: :supervisor,
      restart: :temporary,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @type option ::
          {:group, atom()}
          | {:member_id, String.t()}
          | {:members, Keel.Transport.members()}
          | {:cluster_id, binary()}
          | {:data_dir, Path.t()}
          | {:machine, module()}
          | {:bootstrap, boolean()}
          | {:join, boolean()}

  @doc "A start needs an existing log or `bootstrap: true` that names this member or `join: true`."
  @spec start_link([option() | {atom(), term()}]) :: Supervisor.on_start() | {:error, term()}
  def start_link(opts) do
    group = Keyword.fetch!(opts, :group)

    with :ok <- check_start(opts) do
      Supervisor.start_link(__MODULE__, opts, name: name(group))
    end
  end

  @impl Supervisor
  def init(opts) do
    transport = Keyword.get(opts, :transport, Dist)
    disk = Keyword.get_lazy(opts, :disk, &Posix.new/0)
    shared_opts = Keyword.put(opts, :disk, disk)
    writer = writer_opts(shared_opts)
    applier = applier_opts(shared_opts)
    receiver = receiver_opts(shared_opts)
    shared = transport_opts(opts)
    transport_spec = transport.child_spec(shared)
    server = server_opts(shared_opts, transport)
    senders = [name: senders_name(Keyword.fetch!(opts, :group)), strategy: :one_for_one]

    children = [
      %{id: :writer, start: {Writer, :start_link, [writer]}},
      %{id: :applier, start: {Applier, :start_link, [applier]}},
      %{id: :receiver, start: {SnapshotReceiver, :start_link, [receiver]}},
      {DynamicSupervisor, senders},
      transport_spec,
      %{id: :server, start: {Server, :start_link, [server]}}
    ]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 3, max_seconds: 60)
  end

  defp check_start(opts) do
    disk = Keyword.get_lazy(opts, :disk, &Posix.new/0)
    dir = opts |> Keyword.fetch!(:data_dir) |> Path.join("log")

    case Store.exists?(disk, dir) do
      {:ok, _disk, true} -> :ok
      {:ok, _disk, false} -> check_fresh(opts)
      {:error, _disk, reason} -> {:error, reason}
    end
  end

  defp check_fresh(opts) do
    members = Keyword.get(opts, :members, %{})
    named? = Map.has_key?(members, Keyword.fetch!(opts, :member_id))

    cond do
      Keyword.get(opts, :bootstrap, false) and named? -> :ok
      Keyword.get(opts, :join, false) -> :ok
      true -> {:error, :not_bootstrapped}
    end
  end

  defp bootstrap_config(opts) do
    members = Keyword.get(opts, :members, %{})
    config = members |> Map.keys() |> Config.new()
    addresses = Map.new(members, fn {id, node} -> {id, Atom.to_string(node)} end)

    case Keyword.get(opts, :bootstrap, false) do
      true -> %{config | addresses: addresses}
      false -> nil
    end
  end

  defp writer_opts(opts) do
    group = Keyword.fetch!(opts, :group)
    data_dir = Keyword.fetch!(opts, :data_dir)
    store = Keyword.get(opts, :store, [])

    identity = [
      member: Keyword.fetch!(opts, :member_id),
      cluster_id: Keyword.fetch!(opts, :cluster_id)
    ]

    [
      name: writer_name(group),
      disk: Keyword.fetch!(opts, :disk),
      dir: Path.join(data_dir, "log"),
      owner: Server.name(group),
      bootstrap: bootstrap_config(opts),
      join: Keyword.get(opts, :join, false),
      store: Keyword.merge(store, identity)
    ]
  end

  defp applier_opts(opts) do
    group = Keyword.fetch!(opts, :group)

    [
      name: Applier.name(group),
      machine: Keyword.fetch!(opts, :machine),
      machine_config: Keyword.get(opts, :machine_config, %{}),
      server: Server.name(group),
      disk: Keyword.fetch!(opts, :disk),
      snap_dir: snap_dir(opts)
    ]
  end

  defp receiver_opts(opts) do
    group = Keyword.fetch!(opts, :group)

    [
      name: SnapshotReceiver.name(group),
      disk: Keyword.fetch!(opts, :disk),
      snap_dir: snap_dir(opts),
      server: Server.name(group)
    ]
  end

  defp snap_dir(opts), do: opts |> Keyword.fetch!(:data_dir) |> Path.join("snap")

  defp transport_opts(opts) do
    group = Keyword.fetch!(opts, :group)
    extra = Keyword.get(opts, :transport_opts, [])

    base = [
      group: group,
      cluster_id: Keyword.fetch!(opts, :cluster_id),
      destination: Server.name(group)
    ]

    Keyword.merge(extra, base)
  end

  defp server_opts(opts, transport) do
    group = Keyword.fetch!(opts, :group)
    keys = ~w[group member_id members cluster_id raft env max_pending max_mailbox disk]a

    opts
    |> Keyword.take(keys)
    |> Keyword.merge(
      writer: writer_name(group),
      applier: Applier.name(group),
      receiver: SnapshotReceiver.name(group),
      sender_supervisor: senders_name(group),
      snap_dir: snap_dir(opts),
      transport: transport
    )
  end
end
