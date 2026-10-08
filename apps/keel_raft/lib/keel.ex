defmodule Keel do
  use Boundary,
    deps: [
      Keel.Chain,
      Keel.Common,
      Keel.Env,
      Keel.Log,
      Keel.Machine,
      Keel.Node,
      Keel.Raft,
      Keel.Transport
    ],
    exports: []

  alias Keel.Node.Server
  alias Keel.Node.Supervisor, as: NodeSupervisor

  @transfer_timeout_ms 5_000
  @change_timeout_ms 5_000
  @change_attempts 100
  @change_wait_ms 100

  @type role :: :learner | :voter

  @spec start_group(atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_group(group, opts) do
    spec = {NodeSupervisor, Keyword.put(opts, :group, group)}

    DynamicSupervisor.start_child(Keel.Groups, spec)
  end

  @spec stop_group(atom()) :: :ok | {:error, :not_found}
  def stop_group(group) do
    name = NodeSupervisor.name(group)

    case Process.whereis(name) do
      nil -> {:error, :not_found}
      pid -> DynamicSupervisor.terminate_child(Keel.Groups, pid)
    end
  end

  @spec transfer_leadership(atom(), String.t()) :: :ok | {:error, term()}
  def transfer_leadership(group, target) do
    name = Server.name(group)

    case Server.transfer_leadership(name, target, @transfer_timeout_ms) do
      {:transferred, _target} -> :ok
      {:refused, reason} -> {:error, reason}
    end
  end

  @doc "A voter is added as a learner first. It is promoted once it has caught up."
  @spec add_member(atom(), String.t(), node(), role()) :: :ok | {:error, term()}
  def add_member(group, member_id, node, :learner) do
    address = Atom.to_string(node)

    change(group, {:add_learner, member_id, address}, @change_attempts)
  end

  def add_member(group, member_id, node, :voter) do
    with :ok <- add_member(group, member_id, node, :learner) do
      promote_member(group, member_id)
    end
  end

  @spec promote_member(atom(), String.t()) :: :ok | {:error, term()}
  def promote_member(group, member_id), do: change(group, {:promote, member_id}, @change_attempts)

  @spec remove_member(atom(), String.t()) :: :ok | {:error, term()}
  def remove_member(group, member_id), do: change(group, {:remove, member_id}, @change_attempts)

  @spec status(atom()) :: Server.status()
  def status(group) do
    name = Server.name(group)

    Server.status(name)
  end

  defp change(group, change, attempts) do
    name = Server.name(group)

    case ask(name, change) do
      {:applied, _result} -> :ok
      {:refused, reason} -> retry(group, change, attempts, reason)
      {:error, reason} -> {:error, reason}
    end
  end

  defp ask(name, change) do
    Server.change_membership(name, change, @change_timeout_ms)
  catch
    :exit, _reason -> {:error, :timeout}
  end

  defp retry(group, change, attempts, reason) do
    if attempts > 1 and wait?(reason) do
      Process.sleep(@change_wait_ms)

      redirect(group, change, attempts - 1, reason)
    else
      {:error, reason}
    end
  end

  defp redirect(group, change, attempts, {:not_leader, {_id, leader}}) when leader != node() do
    name = Server.name(group)
    args = [name, change, @change_timeout_ms]
    reply = :erpc.call(leader, Server, :change_membership, args, @change_timeout_ms + 1_000)

    case reply do
      {:applied, _result} -> :ok
      {:refused, reason} -> retry(group, change, attempts, reason)
    end
  catch
    :error, {:erpc, reason} -> {:error, reason}
  end

  defp redirect(group, change, attempts, _reason), do: change(group, change, attempts)

  defp wait?({:not_leader, _leader}), do: true
  defp wait?(:not_ready), do: true
  defp wait?(:not_caught_up), do: true
  defp wait?(:change_in_progress), do: true
  defp wait?(:transfer_in_progress), do: true
  defp wait?(_reason), do: false
end
