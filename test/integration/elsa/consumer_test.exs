defmodule Elsa.ConsumerTest do
  use ExUnit.Case
  use Divo
  import AsyncAssertion
  require Logger

  @brokers Application.compile_env(:elsa_fi, :brokers)
  # Hack time for brod not working right if you don't give it a moment to initialize
  @brod_init_sleep_ms 500

  test "Elsa.Consumer will hand messages to the handler with state" do
    topic = "consumer-test1"
    Elsa.create_topic(@brokers, topic, partitions: 2)

    start_supervised(
      {Elsa.Supervisor,
       connection: :name1,
       endpoints: @brokers,
       group_consumer: [
         group: "group1",
         topics: [topic],
         handler: Testing.ExampleMessageHandlerWithState,
         handler_init_args: %{pid: self()},
         config: [begin_offset: :earliest]
       ]}
    )

    send_messages(topic, ["message1", "message2"])

    assert_receive {:message, %{topic: ^topic, partition: 0, offset: _, key: "", value: "message1"}}, 5_000
    assert_receive {:message, %{topic: ^topic, partition: 1, offset: _, key: "", value: "message2"}}, 5_000
  end

  test "Elsa.Consumer will hand messages to the handler without state" do
    topic = "consumer-test2"
    Elsa.create_topic(@brokers, topic)

    Agent.start_link(fn -> [] end, name: :test_message_store)

    start_supervised(
      {Elsa.Supervisor,
       connection: :name1,
       endpoints: @brokers,
       group_consumer: [
         topics: [topic],
         group: "group2",
         handler: Testing.ExampleMessageHandlerWithoutState,
         config: [begin_offset: :earliest]
       ]}
    )

    send_messages(topic, ["message2"])

    assert_async 40, 500, fn ->
      messages = Agent.get(:test_message_store, fn s -> s end)
      assert 1 == length(messages)
      assert match?(%{topic: _topic, partition: 0, key: "", value: "message2"}, List.first(messages))
    end
  end

  test "Elsa.Consumer will still send messages after reassignment" do
    topic = "consumer-test3"
    group = "group3"
    Elsa.create_topic(@brokers, topic)

    Agent.start_link(fn -> [] end, name: :test_message_store)

    test_pid = self()

    start_supervised({
      Elsa.Supervisor,
      connection: :name1,
      endpoints: @brokers,
      group_consumer: [
        topics: [topic],
        group: group,
        handler: Testing.ExampleMessageHandlerWithoutState,
        config: [begin_offset: :earliest],
        assignments_revoked_handler: fn ->
          send(test_pid, :assignments_revoked)
          :ok
        end
      ]
    })

    send_messages(topic, ["message3"])

    assert_async 40, 500, fn ->
      messages = Agent.get(:test_message_store, fn s -> s end)
      assert 1 == length(messages)
      assert match?(%{topic: _topic, partition: 0, key: "", value: "message3"}, List.first(messages))
    end

    # Create, then destroy another group subscriber to force a rebalance
    {:ok, dummy_pid} =
      Elsa.Supervisor.start_link(
        connection: :name2,
        endpoints: @brokers,
        group_consumer: [
          topics: [topic],
          group: group,
          handler: Testing.ExampleMessageHandlerWithoutState,
          config: [begin_offset: :latest]
        ]
      )

    assert_receive :assignments_revoked

    Supervisor.stop(dummy_pid)

    assert_receive :assignments_revoked

    send_messages(topic, ["message4"])

    assert_async 40, 500, fn ->
      messages = Agent.get(:test_message_store, fn s -> s end)
      assert 2 == length(messages)
      assert match?(%{topic: _topic, partition: 0, key: "", value: "message4"}, List.last(messages))
    end
  end

  defp send_messages(topic, messages) do
    :brod.start_link_client(@brokers, :test_client)
    :brod.start_producer(:test_client, topic, [])

    on_exit(fn ->
      :brod_client.stop_producer(:test_client, topic)
      :brod.stop_client(:test_client)
    end)

    :timer.sleep(@brod_init_sleep_ms)

    messages
    |> Enum.with_index()
    |> Enum.each(fn {msg, index} ->
      partition = rem(index, 2)
      :brod.produce_sync(:test_client, topic, partition, "", msg)
    end)
  end
end

defmodule Testing.ExampleMessageHandlerWithState do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    {:ok, args}
  end

  def handle_messages(messages, state) do
    Enum.each(messages, &send(state.pid, {:message, &1}))

    {:ack, state}
  end
end

defmodule Testing.ExampleMessageHandlerWithoutState do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages) do
    msgs = Enum.map(messages, &Map.delete(&1, :offset))
    Agent.update(:test_message_store, fn s -> s ++ msgs end)
    :ack
  end
end
