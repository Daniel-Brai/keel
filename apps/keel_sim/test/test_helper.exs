{:ok, _credo} = Application.ensure_all_started(:credo)
ExUnit.start(exclude: [:sweep])
