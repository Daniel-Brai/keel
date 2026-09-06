defmodule Keel.Common.String do
  # Group names from the command line are known groups so their atoms already exist
  @spec to_atom(atom() | String.t()) :: atom()
  def to_atom(value) when is_atom(value), do: value
  def to_atom(value) when is_binary(value), do: String.to_existing_atom(value)
end
