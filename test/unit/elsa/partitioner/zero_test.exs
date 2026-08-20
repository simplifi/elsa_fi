defmodule Elsa.Partitioner.ZeroTest do
  use ExUnit.Case

  alias Elsa.Partitioner.Zero

  describe "partition/2" do
    test "always returns partition zero" do
      assert Zero.partition(5, "key") == 0
    end

    test "the legacy default partitioner delegates to zero" do
      assert Elsa.Partitioner.Default.partition(5, "key") == 0
    end
  end
end