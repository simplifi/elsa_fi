defmodule Elsa.Partitioner.Default do
  @moduledoc deprecated: "Use Elsa.Partitioner.Zero instead."

  @behaviour Elsa.Partitioner

  def partition(count, key), do: Elsa.Partitioner.Zero.partition(count, key)
end