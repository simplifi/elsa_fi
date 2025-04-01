defmodule Elsa.Partitioner.Md5Test do
  use ExUnit.Case

  alias Elsa.Partitioner.Md5

  describe "partition/2" do
    test "returns a predictable number based on the hash of the key" do
      assert Md5.partition(2, "key1") == 1
    end
  end
end
