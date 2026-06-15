defmodule Elsa.Consumer.WorkerTest do
  use ExUnit.Case

  import Checkov
  import Mock
  import ExUnit.CaptureLog
  import Elsa.Consumer.Worker, only: [kafka_message_set: 1, kafka_fetch_error: 1]
  import Elsa.Message, only: [kafka_message: 1]

  alias Elsa.Consumer.Worker
  alias Elsa.Group.Acknowledger

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

  describe "handle_info/2 offset_out_of_range" do
    setup_with_mocks([
      {:brod_consumer, [], [subscribe: fn _, _, _ -> :ok end, ack: fn _, _ -> :ok end]}
    ]) do
      # consumer_pid is a distinct process from the test process so that
      # assertions on subscribe/3's first arg meaningfully pin it (a pid-swap
      # regression in the worker would then be caught).
      consumer_pid = spawn(fn -> :ok end)
      state = build_worker_state(consumer_pid: consumer_pid)

      {:ok, state: state, consumer_pid: consumer_pid}
    end

    test "re-subscribes in place and stays alive, resetting offset to :undefined", %{
      state: state,
      consumer_pid: consumer_pid
    } do
      res = Worker.handle_info({consumer_pid, kafka_fetch_error(error_code: :offset_out_of_range)}, state)
      assert {:noreply, new_state} = res

      assert new_state.offset == :undefined
      # Exactly once — a retry-loop regression (re-subscribing repeatedly) would fail this.
      assert_called_exactly(:brod_consumer.subscribe(consumer_pid, :_, begin_offset: :latest), 1)
    end

    test "re-subscribes using config begin_offset when set", %{
      state: state,
      consumer_pid: consumer_pid
    } do
      state = %{state | config: [begin_offset: :earliest]}

      assert {:noreply, _new_state} =
               Worker.handle_info({consumer_pid, kafka_fetch_error(error_code: :offset_out_of_range)}, state)

      assert_called_exactly(:brod_consumer.subscribe(consumer_pid, :_, begin_offset: :earliest), 1)
    end

    test "other fetch-error codes are logged and the worker stays alive without re-subscribing", %{
      state: state,
      consumer_pid: consumer_pid
    } do
      error = kafka_fetch_error(error_code: :unknown_topic_or_partition, error_desc: "nope")

      log =
        capture_log(fn ->
          assert {:noreply, ^state} = Worker.handle_info({consumer_pid, error}, state)
        end)

      assert log =~ "unknown_topic_or_partition"
      assert_not_called(:brod_consumer.subscribe(:_, :_, :_))
    end

    test "ignores fetch_error casts from a non-matching consumer_pid (guard)", %{state: state} do
      other_pid = spawn(fn -> :ok end)

      # A stale/previous consumer pid must not trigger a re-subscribe of the current
      # consumer. With no matching clause the message is not acted on (it does not
      # call subscribe); the worker raises rather than silently re-subscribing.
      assert_raise FunctionClauseError, fn ->
        Worker.handle_info({other_pid, kafka_fetch_error(error_code: :offset_out_of_range)}, state)
      end

      assert_not_called(:brod_consumer.subscribe(:_, :_, :_))
    end
  end

  describe "handle_info/2 offset_out_of_range fail-loud" do
    setup_with_mocks([
      {:brod_consumer, [], [subscribe: fn _, _, _ -> {:error, :no_leader} end, ack: fn _, _ -> :ok end]}
    ]) do
      consumer_pid = spawn(fn -> :ok end)
      # subscribe_delay: 0 keeps the retry backoff instant instead of mocking
      # Process.sleep/1, so the fail-loud path resolves without the ~4s wait.
      state = build_worker_state(consumer_pid: consumer_pid, config: [subscribe_delay: 0])

      {:ok, state: state, consumer_pid: consumer_pid}
    end

    test "stops with :resubscribe_failed and preserves original state", %{
      state: state,
      consumer_pid: consumer_pid
    } do
      # The stop returns the ORIGINAL state (not the offset: :undefined copy) so
      # supervisors/manager see the offset that failed.
      assert {:stop, {:resubscribe_failed, :failed_subscription}, ^state} =
               Worker.handle_info({consumer_pid, kafka_fetch_error(error_code: :offset_out_of_range)}, state)
    end
  end

  defp build_worker_state(overrides) do
    base = %{
      connection: :test_name,
      topic: "test-topic",
      partition: 0,
      generation_id: 5,
      offset: 13,
      handler: Elsa.Consumer.WorkerTest.Handler,
      handler_init_args: [],
      config: [],
      consumer_pid: nil
    }

    struct(Worker.State, Map.merge(base, Map.new(overrides)))
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
