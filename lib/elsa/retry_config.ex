defmodule Elsa.RetryConfig do
  @moduledoc """
  Simple struct for metadata request configuration, with function to provide defaults
  """
  defstruct [
    :tries,
    :dwell_ms
  ]

  @type t :: %__MODULE__{}

  @doc """
  Generate a struct from a keyword list with :tries and :dwell_ms.
  Both keyword list entries are optional, and will be substituded with defaults if missing.
  Default tries: 5
  Default dwell_ms: 100
  """
  @spec new(keyword()) :: t()
  def new(config) when is_list(config) do
    %__MODULE__{
      tries: Keyword.get(config, :tries, 5),
      dwell_ms: Keyword.get(config, :dwell_ms, 100)
    }
  end

  @doc "Return a retry config that doesn't retry at all"
  @spec no_retry() :: t()
  def no_retry do
    %__MODULE__{
      tries: 1,
      dwell_ms: 0
    }
  end

  @doc "Decrement the tries field by 1"
  @spec decrement(t()) :: t()
  def decrement(%__MODULE__{tries: tries, dwell_ms: dwell_ms}) do
    %__MODULE__{tries: tries - 1, dwell_ms: dwell_ms}
  end
end
