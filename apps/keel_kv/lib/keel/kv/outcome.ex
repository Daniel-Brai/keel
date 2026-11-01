defmodule Keel.KV.Outcome do
  alias Keel.Error

  @type input :: {:reply, term()} | {:down, term()} | :timeout

  @type retry :: %{
          rotate?: boolean(),
          maybe_applied?: boolean(),
          refusal: nil | Error.reason()
        }

  @type action ::
          {:done, {:ok, term()} | {:error, Error.reason()}}
          | {:redirect, node()}
          | {:retry, retry()}
          | :session_lost

  @spec classify(input()) :: action()
  def classify({:reply, {:read, result}}), do: {:done, result}
  def classify({:reply, {:applied, {:error, :session_expired}}}), do: :session_lost
  def classify({:reply, {:applied, result}}), do: {:done, result}
  def classify({:reply, {:refused, {:not_leader, {_member, node}}}}), do: {:redirect, node}
  def classify({:reply, {:refused, {:not_leader, nil}}}), do: retry(true, false, :no_leader)
  def classify({:reply, {:refused, :leadership_lost}}), do: retry(false, true, nil)
  def classify({:reply, {:refused, :shutting_down}}), do: retry(true, false, :shutting_down)
  def classify({:reply, {:refused, :overloaded}}), do: {:done, {:error, :overloaded}}
  def classify({:reply, {:refused, reason}}), do: retry(false, false, reason)
  def classify({:down, :noproc}), do: retry(true, false, :no_leader)
  def classify({:down, _reason}), do: retry(true, true, nil)
  def classify(:timeout), do: retry(true, true, nil)

  defp retry(rotate?, maybe_applied?, refusal),
    do: {:retry, %{rotate?: rotate?, maybe_applied?: maybe_applied?, refusal: refusal}}
end
