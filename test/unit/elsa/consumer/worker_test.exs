defmodule Elsa.Consumer.WorkerTest do
  use ExUnit.Case

  import Checkov
  import Mock
  import Elsa.Consumer.Worker
  import Elsa.Group.Acknowledger
  import Elsa.Message, only: [kafka_message: 1]

  describe "handle_info/2" do
    setup_with_mocks([
      {Acknowledger, [], [ack: fn _, _, _, _, _ -> :ok end]},
      {:brod_consumer, [], [ack: fn _, _ -> :ok end]}
    ]) do
      init_args = [
        connection: :test_name,
        topic: "test-topic",
        partition: 0,
        generation_id: 5,
        begin_offset: 13,
        handler: Elsa.Consumer.WorkerTest.Handler,
        handler_init_args: [],
        config: []
      ]

      messages =
        kafka_message_set(
          topic: "test-topic",
          partition: 0,
          messages: [
            kafka_message(offset: 13, key: "key1", value: "value1"),
            kafka_message(offset: 14, key: "key2", value: "value2")
          ]
        )

      {:ok, messages: messages, state: create_state(init_args)}
    end

    data_test "handler can specifiy offset to ack", %{messages: messages, state: state} do
      set_handler(fn messages ->
        offset = messages |> List.first() |> Map.get(:offset)
        {ack, offset}
      end)

      Worker.handle_info({:some_pid, messages}, state)

      assert_called(Acknowledger.ack(:test_name, "test-topic", 0, 5, 13))

      where(ack: [:ack, :acknowledge])
    end

    data_test "handler can say #{response}", %{messages: messages, state: state} do
      set_handler(fn _messags -> response end)

      Worker.handle_info({:some_pid, messages}, state)

      assert_not_called(Acknowledger.ack(:test_name, "test-topic", 0, :_, :_))
      where(response: [:no_ack, :noop])
    end

    test "handler can say to continue to consume the ack but not ack consumer group", %{
      messages: messages,
      state: state
    } do
      set_handler(fn _messages -> :continue end)

      Worker.handle_info({:some_pid, messages}, state)

      assert_not_called(Acknowledger.ack(:test_name, "test-topic", 0, :_, :_))
      assert_called(:brod_consumer.ack(:_, 14))
    end

    data_test "acking without a generation_id continues to consume messages", %{
      messages: messages,
      state: state
    } do
      set_handler(fn msgs ->
        offset = msgs |> List.first() |> Map.get(:offset)
        {ack, offset}
      end)

      Worker.handle_info({:some_pid, messages}, Map.put(state, :generation_id, nil))
      assert_not_called(Acknowledger.ack(:test_name, "test-topic", 0, :_, :_))
      assert_called(:brod_consumer.ack(:_, 13))

      where ack: [:ack, :acknowledge]
    end
  end

  defp create_state(init_args) do
    state =
      init_args
      |> Enum.into(%{})
      |> Map.delete(:begin_offset)
      |> Map.put(:offset, 13)

    struct(Worker.State, state)
  end

  defp set_handler(handler) do
    start_supervised(%{id: :agent1, start: {Agent, :start_link, [fn -> handler end, [name: __MODULE__]]}})
  end
end

defmodule Elsa.Consumer.WorkerTest.Handler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages) do
    function = Agent.get(Elsa.Consumer.WorkerTest, fn s -> s end)
    function.(messages)
  end
end
