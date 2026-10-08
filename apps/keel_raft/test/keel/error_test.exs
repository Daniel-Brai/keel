defmodule Keel.ErrorTest do
  use ExUnit.Case, async: true

  alias Keel.Error

  describe "indefinite?/1" do
    test "is false for a refusal" do
      indefinite = Error.indefinite?({:not_leader, nil})

      refute indefinite
    end

    test "is false for a failed precondition" do
      indefinite = Error.indefinite?({:revision_mismatch, 3})

      refute indefinite
    end

    test "is true for the three unknown outcomes" do
      reasons = ~w[leadership_lost timeout unknown_outcome]a

      indefinite = Enum.all?(reasons, &Error.indefinite?/1)

      assert indefinite
    end
  end

  describe "retryable?/1" do
    test "is false for an applied failure" do
      retryable = Error.retryable?(:space_exceeded)

      refute retryable
    end

    test "is false for an unknown outcome the caller must settle" do
      retryable = Error.retryable?(:unknown_outcome)

      refute retryable
    end

    test "is true for a redirect" do
      retryable = Error.retryable?({:not_leader, nil})

      assert retryable
    end
  end
end
