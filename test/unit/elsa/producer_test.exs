defmodule Elsa.ProducerTest do
  use ExUnit.Case
  import TestHelper
  alias Elsa.ElsaRegistry

  describe "produce_sync" do
    setup do
      {:ok, registry} = ElsaRegistry.start_link(name: :elsa_registry_test_client)
      ElsaRegistry.register_name({:elsa_registry_test_client, :brod_client}, self())

      on_exit(fn -> assert_down(registry) end)

      :ok
    end

    test "produce_sync fails with returning what messages did not get sent" do
      :meck.new(:brod_producer)
      :meck.expect(:brod_producer, :produce, 3, :meck.seq([:ok, {:error, :some_reason}, :ok]))
      :meck.expect(:brod_producer, :sync_produce_request, 2, {:ok, 0})

      ElsaRegistry.register_name({:elsa_registry_test_client, :"producer_topic-a_0"}, self())

      messages = Enum.map(1..2_000, fn i -> %{key: "", value: random_string(i)} end)
      {:error, reason, failed} = Elsa.produce(:test_client, "topic-a", messages, connection: :test_client, partition: 0)

      assert reason == "1331 messages succeeded before elsa producer failed midway through due to :some_reason"
      assert 2000 - 1331 == length(failed)
      assert failed == Enum.drop(messages, 1331)

      :meck.unload(:brod_producer)
    end
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.encode64()
    |> binary_part(0, length)
  end
end
