defmodule Elsa.ShutdownTest do
  use ExUnit.Case
  use Divo
  import AssertAsync

  @endpoints Application.compile_env(:elsa_fi, :brokers)
  @topic "shutdown-topic"

  test "non direct ACKs duplicate data" do
    setup_topic(@topic)
    test_pid = self()

    options = elsa_options(@topic, test_pid, :earliest)

    {:ok, first_run_pid} = start_supervised({Elsa.Supervisor, options})
    assert_receive {:message, %Elsa.Message{value: "a"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "b"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "c"}}, 5_000

    stop_supervised(Elsa.Supervisor)

    assert_async sleep: 1_000, max_tries: 60 do
      assert false == Process.alive?(first_run_pid)
    end

    {:ok, _second_run_pid} = start_supervised({Elsa.Supervisor, options})
    # give it time to pull in duplicates if they are there
    Process.sleep(10_000)

    Elsa.produce(@endpoints, @topic, ["d", "e", "f"])

    # Check that we receive new messages, but that the old ones are not duplicated.
    assert_receive {:message, %Elsa.Message{value: "d"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "e"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "f"}}, 5_000

    refute_received {:message, %Elsa.Message{value: "a"}}
    refute_received {:message, %Elsa.Message{value: "b"}}
    refute_received {:message, %Elsa.Message{value: "c"}}
  end

  defp setup_topic(topic) do
    Elsa.create_topic(@endpoints, topic)

    assert_async do
      assert Elsa.topic?(@endpoints, topic)
    end

    Elsa.produce(@endpoints, topic, ["a", "b", "c"])
  end

  defp elsa_options(topic, test_pid, begin_offset) do
    [
      name: :"#{topic}_consumer",
      endpoints: @endpoints,
      connection: :"#{topic}",
      group_consumer: [
        group: "test-#{topic}",
        topics: [topic],
        handler: FakeMessageHandler,
        handler_init_args: [test_pid: test_pid],
        config: [
          offset_reset_policy: :reset_to_earliest,
          begin_offset: begin_offset,
          max_bytes: 1_000_000,
          min_bytes: 0,
          max_wait_time: 10_000
        ]
      ]
    ]
  end
end

defmodule FakeMessageHandler do
  use Elsa.Consumer.MessageHandler

  require Logger

  def init(args) do
    {:ok, %{test_pid: Keyword.fetch!(args, :test_pid)}}
  end

  def handle_messages(messages, %{test_pid: test_pid} = state) do
    Logger.info("processing #{inspect(messages)}")

    Enum.each(messages, &send(test_pid, {:message, &1}))
    # pretend we're doing some heavy lifting here
    Process.sleep(5_000)

    Logger.info("acking #{inspect(messages)}")
    {:ack, state}
  end
end
