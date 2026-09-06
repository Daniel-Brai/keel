defmodule Keel.Common.Name do
  @spec build(String.t(), atom(), String.t()) :: atom()
  def build(prefix, group, suffix \\ "") do
    # Only a group the application starts gets a name so the atoms stay few
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    :"#{prefix}_#{group}#{suffix}"
  end
end
