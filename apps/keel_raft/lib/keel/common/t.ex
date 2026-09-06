defmodule Keel.Common.T do
  @type group :: String.t()

  @type member_id :: String.t()

  @type term_number :: non_neg_integer()

  @type index :: non_neg_integer()

  @type persist_seq :: non_neg_integer()

  @type role :: :follower | :pre_candidate | :candidate | :leader
end
