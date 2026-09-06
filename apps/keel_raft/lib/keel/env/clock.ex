defmodule Keel.Env.Clock do
  @callback monotonic_ms() :: integer()

  @callback wall_ms() :: integer()

  @callback send_after(pid(), term(), non_neg_integer()) :: reference()
end
