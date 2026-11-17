import Config

# A release starts one group from these variables. Without KEEL_MEMBER_ID it starts none.
# Group and node names come from the operator so the atoms they make stay few.
if config_env() != :test and System.get_env("KEEL_MEMBER_ID") do
  required = fn name ->
    case System.get_env(name) do
      nil -> raise "set #{name} to start a group at boot"
      "" -> raise "set #{name} to start a group at boot"
      value -> value
    end
  end

  member = fn pair ->
    case String.split(pair, "=", parts: 2) do
      [id, node] when id != "" and node != "" -> {id, String.to_atom(node)}
      _bad -> raise "KEEL_MEMBERS holds #{inspect(pair)}. Write each member as id=name@host"
    end
  end

  cluster_id = fn hex ->
    case Base.decode16(hex, case: :mixed) do
      {:ok, <<_id::binary-size(16)>> = id} -> id
      _bad -> raise "KEEL_CLUSTER_ID must be 32 hex characters and the same on every member"
    end
  end

  start = fn
    nil -> []
    "bootstrap" -> [bootstrap: true]
    "join" -> [join: true]
    other -> raise "KEEL_START is #{inspect(other)}. Use bootstrap or join or leave it unset"
  end

  group_name = System.get_env("KEEL_GROUP", "default")
  pairs = "KEEL_MEMBERS" |> required.() |> String.split(",", trim: true)
  start_mode = System.get_env("KEEL_START")

  base = [
    member_id: required.("KEEL_MEMBER_ID"),
    members: Map.new(pairs, member),
    cluster_id: "KEEL_CLUSTER_ID" |> required.() |> cluster_id.(),
    data_dir: System.get_env("KEEL_DATA_DIR", "/var/lib/keel/#{group_name}"),
    machine: Keel.KV.Machine
  ]

  opts = Keyword.merge(base, start.(start_mode))

  config :keel_raft, :groups, [{String.to_atom(group_name), opts}]
end
