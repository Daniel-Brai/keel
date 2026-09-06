defmodule Keel.Env.Clock.System do
  @behaviour Keel.Env.Clock

  @impl Keel.Env.Clock
  def monotonic_ms, do: System.monotonic_time(:millisecond)

  @impl Keel.Env.Clock
  def wall_ms, do: System.os_time(:millisecond)

  @impl Keel.Env.Clock
  def send_after(pid, message, ms), do: Process.send_after(pid, message, ms)
end
