defmodule Elsa.Partitioner.Zero do
  @moduledoc """
  Always selects partition 0.
  """

  @behaviour Elsa.Partitioner

  def partition(_count, _key), do: 0
end