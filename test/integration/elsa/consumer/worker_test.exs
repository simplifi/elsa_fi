defmodule Elsa.Consumer.WorkerTest do
  use ExUnit.Case
  use Divo

  alias Elsa.ElsaSupervisor

  require Logger

  @endpoints Application.compile_env(:elsa_fi, :brokers)
  # Hack time for brod not working right if you don't give it a moment to initialize
  @brod_init_sleep_ms 500

  test "simply consumes messages from configured topic/partition" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "simple-topic")
      end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, pid} =
      ElsaSupervisor.start_link(
        connection: :test_simple_consumer,
        endpoints: @endpoints,
        consumer: [
          topic: "simple-topic",
          partition: 0,
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    Elsa.produce(@endpoints, "simple-topic", {"key1", "value1"}, partition: 0)
    assert_receive [%Elsa.Message{value: "value1"}], 5_000

    Supervisor.stop(pid)
  end

  test "consumes only from configured partition" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "simple-partitioned-topic", partitions: 3)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, pid} =
      ElsaSupervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "simple-partitioned-topic",
          partition: 1,
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    Elsa.produce(@endpoints, "simple-partitioned-topic", {"0", "zero"}, partition: 0)
    Elsa.produce(@endpoints, "simple-partitioned-topic", {"1", "one"}, partition: 1)
    Elsa.produce(@endpoints, "simple-partitioned-topic", {"2", "two"}, partition: 2)

    assert_receive [%Elsa.Message{value: "one"}], 5_000
    refute_receive [%Elsa.Message{value: "zero"}]
    refute_receive [%Elsa.Message{value: "two"}]

    Supervisor.stop(pid)
  end

  test "can be configured to consume the latest messages only" do
    topic = "latest-only-topic"

    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, topic, partitions: 1)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, topic, {"0", "strike 1"}, partition: 0)
    Elsa.produce(@endpoints, topic, {"1", "strike 2"}, partition: 0)

    # For this test to not fail randomly, we need to make sure the published offsets
    # have registered with the topic before we create the consumer pointed to :latest.
    Patiently.wait_for!(fn ->
      {:ok, offset} = :brod.resolve_offset(@endpoints, topic, 0)
      Logger.info("Current latest offset: #{inspect(offset)}")
      offset == 2
    end)

    connection = :test_simple_consumer_partition

    {:ok, pid} =
      ElsaSupervisor.start_link(
        connection: connection,
        endpoints: @endpoints,
        consumer: [
          topic: topic,
          partition: 0,
          begin_offset: :latest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()],
          config: [offset_reset_policy: :earliest]
        ]
      )

    Patiently.wait_for(fn -> Elsa.Producer.ready?(connection) end)
    :timer.sleep(@brod_init_sleep_ms)

    Elsa.produce(@endpoints, topic, {"2", "homerun"}, partition: 0)

    assert_receive [%Elsa.Message{value: "homerun"}], 5_000
    refute_receive [%Elsa.Message{value: "strike 1"}]
    refute_receive [%Elsa.Message{value: "strike 2"}]

    Supervisor.stop(pid)
  end

  test "can be configured to consume from a specific offset" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "specific-offset", partitions: 1)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, "specific-offset", {"0", "a"}, partition: 0)
    Elsa.produce(@endpoints, "specific-offset", {"1", "b"}, partition: 0)
    Elsa.produce(@endpoints, "specific-offset", {"2", "c"}, partition: 0)

    {:ok, pid} =
      ElsaSupervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "specific-offset",
          partition: 0,
          begin_offset: 1,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    assert_receive [%Elsa.Message{value: "b"}, %Elsa.Message{value: "c"}], 5_000
    refute_receive [%Elsa.Message{value: "a"}]

    Supervisor.stop(pid)
  end

  test "defaults to consuming from all partitions" do
    Patiently.wait_for!(
      fn ->
        :ok = Elsa.create_topic(@endpoints, "all-partitions", partitions: 2)
      end,
      dwell: 1_000,
      max_tries: 30
    )

    Elsa.produce(@endpoints, "all-partitions", {"0", "a"}, partition: 0)
    Elsa.produce(@endpoints, "all-partitions", {"1", "b"}, partition: 1)

    {:ok, pid} =
      ElsaSupervisor.start_link(
        connection: :test_simple_consumer_partition,
        endpoints: @endpoints,
        consumer: [
          topic: "all-partitions",
          begin_offset: :earliest,
          handler: MyMessageHandler,
          handler_init_args: [pid: self()]
        ]
      )

    assert_receive [%Elsa.Message{value: "a"}], 5_000
    assert_receive [%Elsa.Message{value: "b"}], 5_000

    Supervisor.stop(pid)
  end
end

defmodule MyMessageHandler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages, state) do
    send(state[:pid], messages)
    {:ack, state}
  end
end
