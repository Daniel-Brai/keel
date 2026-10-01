defmodule Keel.Chain do
  use Boundary, top_level?: true, deps: [Keel.Machine, Keel.Raft], exports: [Machine]
end
