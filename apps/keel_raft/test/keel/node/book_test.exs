defmodule Keel.Node.BookTest do
  use ExUnit.Case, async: true

  alias Keel.Node.Book
  alias Keel.TestUtils

  @moduletag :capture_log

  describe "follow/2" do
    test "leaves a member alone that has no address" do
      config = TestUtils.random_config(learners: ["m4"])

      members = Book.follow(%{"m1" => :a@host}, config)

      assert members == %{"m1" => :a@host}
    end

    test "leaves out an address of a node this node never heard of" do
      address = "never_heard_of_#{System.unique_integer([:positive])}@host"
      config = TestUtils.random_config(learners: ["m4"], addresses: %{"m4" => address})

      members = Book.follow(%{}, config)

      assert members == %{}
    end

    test "adds a learner with a known address" do
      node = Atom.to_string(node())
      config = TestUtils.random_config(learners: ["m4"], addresses: %{"m4" => node})

      members = Book.follow(%{}, config)

      assert members == %{"m4" => node()}
    end

    test "keeps the address of a member that is already known" do
      node = Atom.to_string(node())
      config = TestUtils.random_config(learners: ["m4"], addresses: %{"m4" => node})

      members = Book.follow(%{"m4" => :other@host}, config)

      assert members == %{"m4" => :other@host}
    end
  end

  describe "forget/2" do
    test "drops the removed members" do
      config = TestUtils.random_config(removed: ["m3"])

      members = Book.forget(%{"m1" => :a@host, "m3" => :c@host}, config)

      assert members == %{"m1" => :a@host}
    end
  end
end
