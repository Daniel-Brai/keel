defmodule Keel.Raft.Log do
  @moduledoc "Entries below `first_index` are gone. The term and time of the entry just below it are kept."

  alias Keel.Common.T
  alias Keel.Raft.Config
  alias Keel.Raft.Entry
  alias Keel.Raft.SnapshotMeta

  # Older entries are packed into binaries that live outside the process heap.
  # So garbage collection never copies them and its pauses do not grow with the log.
  @chunk_entries 1_024

  defstruct recent: %{},
            chunks: %{},
            packed_through: 0,
            terms: [],
            chunk_entries: @chunk_entries,
            first_index: 1,
            base_term: 0,
            base_time_ms: 0,
            last_index: 0,
            last_term: 0,
            last_time_ms: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_time_ms: 0,
            snapshot_size: 0,
            snapshot_checksum: 0,
            snapshot_config: nil,
            base_config: nil,
            config_indexes: []

  @type chunk :: %{
          first: T.index(),
          last: T.index(),
          bytes: binary(),
          offsets: binary(),
          sizes: binary()
        }

  @type t :: %__MODULE__{
          recent: %{T.index() => Entry.t()},
          chunks: %{non_neg_integer() => chunk()},
          packed_through: non_neg_integer(),
          terms: [{T.index(), T.term_number()}],
          chunk_entries: pos_integer(),
          first_index: T.index(),
          base_term: T.term_number(),
          base_time_ms: non_neg_integer(),
          last_index: T.index(),
          last_term: T.term_number(),
          last_time_ms: non_neg_integer(),
          snapshot_index: T.index(),
          snapshot_term: T.term_number(),
          snapshot_time_ms: non_neg_integer(),
          snapshot_size: non_neg_integer(),
          snapshot_checksum: non_neg_integer(),
          snapshot_config: nil | Config.t(),
          base_config: nil | Config.t(),
          config_indexes: [T.index()]
        }

  @spec new([Entry.t()]) :: t()
  def new(entries), do: append(%__MODULE__{}, entries)

  @doc "A log that packs `chunk_entries` entries at a time. Tests use small chunks."
  @spec new([Entry.t()], pos_integer()) :: t()
  def new(entries, chunk_entries), do: append(%__MODULE__{chunk_entries: chunk_entries}, entries)

  @doc "The entries are the ones kept from `first_index`. The first one is dropped when its predecessor is unknown."
  @spec restore([Entry.t()], T.index(), nil | SnapshotMeta.t()) :: t()
  def restore(entries, _first_index, nil), do: new(entries)
  def restore(entries, 1, %SnapshotMeta{} = meta), do: entries |> new() |> with_snapshot(meta)

  def restore(entries, first_index, %SnapshotMeta{index: index} = meta)
      when first_index == index + 1 do
    empty = reset(%__MODULE__{}, meta)

    empty |> append(entries) |> with_snapshot(meta)
  end

  def restore([head | rest], _first_index, %SnapshotMeta{} = meta) do
    base = %__MODULE__{
      first_index: head.index + 1,
      base_term: head.term,
      base_time_ms: head.time_ms,
      last_index: head.index,
      last_term: head.term,
      last_time_ms: head.time_ms
    }

    base |> append(rest) |> with_snapshot(meta)
  end

  @spec snapshot_meta(t()) :: SnapshotMeta.t()
  def snapshot_meta(%__MODULE__{} = log) do
    %SnapshotMeta{
      index: log.snapshot_index,
      term: log.snapshot_term,
      time_ms: log.snapshot_time_ms,
      size: log.snapshot_size,
      checksum: log.snapshot_checksum,
      config: log.snapshot_config
    }
  end

  @doc "The config a log has when it holds no config entry and its snapshot has none."
  @spec put_base_config(t(), Config.t()) :: t()
  def put_base_config(%__MODULE__{} = log, %Config{} = config), do: %{log | base_config: config}

  @spec last_config_index(t()) :: T.index()
  def last_config_index(%__MODULE__{config_indexes: []}), do: 0
  def last_config_index(%__MODULE__{config_indexes: [index | _older]}), do: index

  @spec last_config_entry(t()) :: nil | Entry.t()
  def last_config_entry(%__MODULE__{config_indexes: []}), do: nil

  def last_config_entry(%__MODULE__{config_indexes: [index | _older]} = log),
    do: fetch!(log, index)

  @doc "Entries below `keep_from` are dropped. The snapshot becomes the one given."
  @spec compact(t(), SnapshotMeta.t(), T.index()) :: t()
  def compact(%__MODULE__{} = log, %SnapshotMeta{} = meta, keep_from) do
    keep = max(keep_from, log.first_index)
    base_term = term_at(log, keep - 1)
    base_time = time_at(log, keep - 1)
    recent = Map.reject(log.recent, fn {at, _entry} -> at < keep end)
    chunks = Map.reject(log.chunks, fn {_block, chunk} -> chunk.last < keep end)
    configs = Enum.filter(log.config_indexes, &(&1 >= keep))

    compacted = %{
      log
      | recent: recent,
        chunks: chunks,
        packed_through: max(log.packed_through, keep - 1),
        terms: terms_from(log.terms, keep),
        first_index: keep,
        base_term: base_term,
        base_time_ms: base_time,
        config_indexes: configs
    }

    with_snapshot(compacted, meta)
  end

  @doc "Every entry goes. The log ends where the snapshot does."
  @spec reset(t(), SnapshotMeta.t()) :: t()
  def reset(%__MODULE__{} = log, %SnapshotMeta{} = meta) do
    empty = %__MODULE__{
      base_config: log.base_config,
      chunk_entries: log.chunk_entries,
      packed_through: meta.index,
      first_index: meta.index + 1,
      base_term: meta.term,
      base_time_ms: meta.time_ms,
      last_index: meta.index,
      last_term: meta.term,
      last_time_ms: meta.time_ms
    }

    with_snapshot(empty, meta)
  end

  @spec bytes_after(t(), T.index()) :: non_neg_integer()
  def bytes_after(%__MODULE__{} = log, index) do
    recent = for {at, entry} <- log.recent, at > index, do: Entry.bytes(entry)

    packed =
      for {_block, chunk} <- log.chunks,
          at <- chunk.first..chunk.last//1,
          at > index,
          do: size_at(chunk, at)

    Enum.sum(recent) + Enum.sum(packed)
  end

  @spec term_at(t(), T.index()) :: nil | T.term_number()
  def term_at(%__MODULE__{first_index: first, base_term: term}, index) when index == first - 1,
    do: term

  def term_at(%__MODULE__{first_index: first, last_index: last}, index)
      when index < first or index > last,
      do: nil

  def term_at(%__MODULE__{terms: terms}, index) do
    {_from, term} = Enum.find(terms, fn {from, _term} -> from <= index end)

    term
  end

  @spec time_at(t(), T.index()) :: non_neg_integer()
  def time_at(%__MODULE__{first_index: first, base_time_ms: time}, index) when index == first - 1,
    do: time

  def time_at(%__MODULE__{} = log, index) do
    %Entry{time_ms: time_ms} = fetch!(log, index)

    time_ms
  end

  @spec fetch!(t(), T.index()) :: Entry.t()
  def fetch!(%__MODULE__{packed_through: packed} = log, index) when index > packed,
    do: Map.fetch!(log.recent, index)

  def fetch!(%__MODULE__{} = log, index) do
    block = div(index - 1, log.chunk_entries)
    chunk = Map.fetch!(log.chunks, block)

    unpack(chunk, index)
  end

  @doc "Entries from `from` to `to` inclusive. At most `max` of them."
  @spec slice(t(), T.index(), T.index(), pos_integer()) :: [Entry.t()]
  def slice(%__MODULE__{} = log, from, to, max) do
    last = min(to, from + max - 1)
    if from > last, do: [], else: Enum.map(from..last//1, &fetch!(log, &1))
  end

  @spec append(t(), [Entry.t()]) :: t()
  def append(%__MODULE__{} = log, []), do: log

  def append(%__MODULE__{} = log, entries) do
    last = List.last(entries)
    added = Map.new(entries, &{&1.index, &1})
    merged = Map.merge(log.recent, added)

    appended = %{
      log
      | recent: merged,
        terms: Enum.reduce(entries, log.terms, &add_term/2),
        last_index: last.index,
        last_term: last.term,
        last_time_ms: last.time_ms,
        config_indexes: config_indexes(log, entries)
    }

    pack(appended)
  end

  @spec truncate_from(t(), T.index()) :: t()
  def truncate_from(%__MODULE__{} = log, index) when index > log.last_index, do: log

  def truncate_from(%__MODULE__{} = log, index) do
    unpacked = unpack_from(log, index)
    kept = Map.reject(unpacked.recent, fn {at, _entry} -> at >= index end)
    last = index - 1

    %{
      unpacked
      | recent: kept,
        terms: Enum.reject(log.terms, fn {from, _term} -> from >= index end),
        last_index: last,
        last_term: term_at(log, last),
        last_time_ms: time_at(log, last),
        config_indexes: Enum.filter(log.config_indexes, &(&1 < index))
    }
  end

  defp with_snapshot(%__MODULE__{} = log, %SnapshotMeta{} = meta) do
    %{
      log
      | snapshot_index: meta.index,
        snapshot_term: meta.term,
        snapshot_time_ms: meta.time_ms,
        snapshot_size: meta.size,
        snapshot_checksum: meta.checksum,
        snapshot_config: meta.config
    }
  end

  # Newest first. Each pair is the first index of a run of entries with one term
  defp add_term(%Entry{term: term}, [{_from, term} | _older] = terms), do: terms
  defp add_term(%Entry{index: index, term: term}, terms), do: [{index, term} | terms]

  # The run that holds `keep` stays. Older runs go
  defp terms_from(terms, keep) do
    {newer, older} = Enum.split_while(terms, fn {from, _term} -> from > keep end)

    Enum.concat(newer, Enum.take(older, 1))
  end

  # The newest chunk of entries always stays unpacked so new appends and sends are cheap
  defp pack(%__MODULE__{chunk_entries: size} = log) do
    packed = max(log.packed_through, log.first_index - 1)
    block_end = (div(packed, size) + 1) * size

    case block_end + size <= log.last_index do
      true -> log |> pack_block(packed + 1, block_end) |> pack()
      false -> log
    end
  end

  defp pack_block(%__MODULE__{} = log, from, to) do
    entries = Enum.map(from..to//1, &Map.fetch!(log.recent, &1))
    encoded = Enum.map(entries, &:erlang.term_to_binary/1)
    ends = Enum.scan(encoded, 0, &(byte_size(&1) + &2))
    offset_words = for at <- [0 | ends], do: <<at::32>>
    size_words = for entry <- entries, do: <<Entry.bytes(entry)::32>>
    offsets = :erlang.list_to_binary(offset_words)
    sizes = :erlang.list_to_binary(size_words)
    bytes = :erlang.iolist_to_binary(encoded)
    chunk = %{first: from, last: to, bytes: bytes, offsets: offsets, sizes: sizes}
    block = div(to - 1, log.chunk_entries)
    packed = Enum.to_list(from..to//1)
    recent = Map.drop(log.recent, packed)

    %{log | recent: recent, chunks: Map.put(log.chunks, block, chunk), packed_through: to}
  end

  defp unpack(chunk, index) do
    skip = (index - chunk.first) * 4
    <<_before::binary-size(^skip), start::32, stop::32, _after::binary>> = chunk.offsets
    piece = binary_part(chunk.bytes, start, stop - start)

    :erlang.binary_to_term(piece)
  end

  defp size_at(chunk, index) do
    skip = (index - chunk.first) * 4
    <<_before::binary-size(^skip), size::32, _after::binary>> = chunk.sizes

    size
  end

  # Truncating into packed entries puts the ones that stay back among the recent ones
  defp unpack_from(%__MODULE__{packed_through: packed} = log, index) when index > packed, do: log

  defp unpack_from(%__MODULE__{} = log, index) do
    {cut, kept} = Enum.split_with(log.chunks, fn {_block, chunk} -> chunk.last >= index end)
    firsts = Enum.map(cut, fn {_block, chunk} -> chunk.first end)
    lowest = Enum.min(firsts, fn -> index end)

    pairs =
      for {_block, chunk} <- cut,
          at <- chunk.first..chunk.last//1,
          at >= log.first_index and at < index,
          do: {at, unpack(chunk, at)}

    unpacked = Map.new(pairs)
    restored = Map.merge(log.recent, unpacked)

    %{log | recent: restored, chunks: Map.new(kept), packed_through: lowest - 1}
  end

  # Newest first. An entry that overwrites an index replaces the old one at that index
  defp config_indexes(%__MODULE__{} = log, [first | _rest] = entries) do
    added = for %Entry{kind: :config, index: index} <- entries, do: index
    older = Enum.filter(log.config_indexes, &(&1 < first.index))

    added |> Enum.reverse() |> Enum.concat(older)
  end
end
