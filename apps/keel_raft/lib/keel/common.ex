defmodule Keel.Common do
  use Boundary, top_level?: true, deps: [], exports: [Name, String, T]
end
