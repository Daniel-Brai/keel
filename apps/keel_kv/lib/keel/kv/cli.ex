defmodule Keel.KV.CLI do
  alias Keel.Cluster
  alias Keel.Common.String, as: Atoms
  alias Keel.KV

  @switches [
    node: :string,
    group: :string,
    value: :string,
    value_file: :string,
    expected: :integer,
    hex: :boolean,
    base64: :boolean,
    show: :boolean,
    timeout: :integer
  ]

  @type request :: {:get | :put | :delete, [term()]}

  @type plan :: %{
          node: node(),
          group: String.t(),
          request: request(),
          show?: boolean(),
          timeout: pos_integer()
        }

  @spec plan([String.t()]) :: {:ok, plan()} | {:error, String.t()}
  def plan(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    with :ok <- check_invalid(invalid),
         {:ok, node} <- member_node(opts),
         {:ok, request} <- request(rest, opts) do
      {:ok, build(opts, node, request)}
    end
  end

  @spec status_plan([String.t()]) :: {:ok, plan()} | {:error, String.t()}
  def status_plan(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    group_opts = if rest == [], do: opts, else: Keyword.put(opts, :group, hd(rest))

    with :ok <- check_invalid(invalid),
         {:ok, node} <- member_node(group_opts) do
      {:ok, build(group_opts, node, {:status, []})}
    end
  end

  @spec execute(String.t(), :get | :put | :delete, [term()], keyword()) :: term()
  def execute(group, kind, args, opts) do
    with {:ok, atom} <- group_atom(group) do
      apply(KV, kind, [atom | args] ++ [opts])
    end
  end

  @spec status(String.t(), keyword()) :: term()
  def status(group, opts) do
    with {:ok, atom} <- group_atom(group) do
      Cluster.status(atom, opts)
    end
  end

  @spec render(term(), boolean()) :: [String.t()]
  def render({:ok, nil}, _show?), do: ["not found"]

  def render({:ok, %{value: value, revision: revision}}, show?) do
    head = "revision #{revision}, #{byte_size(value)} bytes"

    if show?, do: [head, text(value)], else: [head]
  end

  def render({:ok, revision}, _show?) when is_integer(revision), do: ["revision #{revision}"]
  def render({:error, {:revision_mismatch, current}}, _show?), do: ["revision is #{current}"]
  def render({:error, {:unknown_group, group}}, _show?), do: ["unknown group #{group}"]
  def render({:error, reason}, _show?), do: ["error: #{inspect(reason)}"]

  @spec render_status(String.t(), term()) :: String.t()
  def render_status(id, {:ok, status}) do
    leader = leader_id(status.leader)

    "#{id} #{status.node} #{status.role} term=#{status.term} leader=#{leader} commit=#{status.commit} applied=#{status.applied}"
  end

  def render_status(id, {:error, {:erpc, :timeout}}), do: "#{id} unknown"
  def render_status(id, {:error, _reason}), do: "#{id} down"

  defp leader_id(nil), do: "none"
  defp leader_id({id, _node}), do: id

  defp build(opts, node, request) do
    %{
      node: node,
      group: Keyword.get(opts, :group, "default"),
      request: request,
      show?: Keyword.get(opts, :show, false),
      timeout: Keyword.get(opts, :timeout, 5_000)
    }
  end

  defp check_invalid([]), do: :ok
  defp check_invalid([{name, _value} | _rest]), do: {:error, "unknown option #{name}"}

  defp member_node(opts) do
    env_node = System.get_env("KEEL_NODE")
    name = Keyword.get(opts, :node, env_node)

    node_atom(name)
  end

  defp node_atom(nil), do: {:error, "give a node with --node or KEEL_NODE"}

  # The node of a running cluster is named by an operator and is not known here
  # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
  defp node_atom(name), do: {:ok, String.to_atom(name)}

  defp request(["get", key], opts),
    do: with_key(key, opts, fn bytes -> {:ok, {:get, [bytes]}} end)

  defp request(["delete", key], opts) do
    with_key(key, opts, fn bytes -> {:ok, {:delete, [bytes]}} end)
  end

  defp request(["put", key], opts) do
    with_key(key, opts, fn bytes -> put_request(bytes, opts) end)
  end

  defp request(_args, _opts), do: {:error, "use get KEY or put KEY or delete KEY"}

  defp put_request(key, opts) do
    case {opts[:value], opts[:value_file]} do
      {nil, nil} -> {:error, "put needs --value or --value-file"}
      {value, nil} -> {:ok, {:put, [key, value]}}
      {nil, path} -> read_value(key, path)
      {_value, _path} -> {:error, "give only one of --value and --value-file"}
    end
  end

  defp read_value(key, path) do
    case File.read(path) do
      {:ok, value} -> {:ok, {:put, [key, value]}}
      {:error, reason} -> {:error, "cannot read #{path}: #{reason}"}
    end
  end

  defp with_key(text, opts, next) do
    case decode_key(text, opts) do
      {:ok, key} -> next.(key)
      :error -> {:error, "the key is not valid for the chosen encoding"}
    end
  end

  defp decode_key(text, opts) do
    cond do
      opts[:hex] -> Base.decode16(text, case: :mixed)
      opts[:base64] -> Base.decode64(text)
      true -> {:ok, text}
    end
  end

  defp text(value) do
    if String.valid?(value), do: value, else: Base.encode16(value, case: :lower)
  end

  defp group_atom(group) do
    {:ok, Atoms.to_atom(group)}
  rescue
    ArgumentError -> {:error, {:unknown_group, group}}
  end
end
