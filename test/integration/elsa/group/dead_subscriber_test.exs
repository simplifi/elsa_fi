defmodule Elsa.Group.SubscriberDeadTest do
  use ExUnit.Case
  use Divo

  @brokers Application.compile_env(:elsa_fi, :brokers)
  # Hack time for brod not working right if you don't give it a moment to initialize
  @brod_init_sleep_ms 500
  @topic "dead-subscriber-topic"

  test "dead subscriber" do
    Elsa.create_topic(@brokers, "dead-subscriber-topic", partitions: 2)

    {:ok, pid} =
      Elsa.ElsaSupervisor.start_link(
        connection: :name1,
        endpoints: @brokers,
        group_consumer: [
          group: "group_dead_subscriber",
          topics: [@topic],
          handler: Test.BasicHandler,
          handler_init_args: %{pid: self()},
          config: [begin_offset: :earliest]
        ]
      )

    #:timer.sleep(@brod_init_sleep_ms)

    send_messages(0, ["message1"])
    send_messages(1, ["message2"])

    assert_receive {:message, %{value: "message1"}}, 5_000
    assert_receive {:message, %{value: "message2"}}, 5_000
    refute_receive {:message, _message}, 5_000

    kill_worker(0)

    send_messages(0, ["message3"])
    send_messages(1, ["message4"])

    assert_receive {:message, %{value: "message3"}}, 5_000
    assert_receive {:message, %{value: "message4"}}, 5_000
    refute_receive {:message, _message}, 5_000

    kill_worker(1)

    send_messages(0, ["message5"])
    send_messages(1, ["message6"])

    assert_receive {:message, %{value: "message5"}}, 5_000
    assert_receive {:message, %{value: "message6"}}, 5_000
    refute_receive {:message, _message}, 5_000

    Supervisor.stop(pid)
  end

  defp send_messages(partition, messages) do
    :brod.start_link_client(@brokers, :test_client)
    :brod.start_producer(:test_client, @topic, [])

    on_exit(fn ->
      :brod_client.stop_producer(:test_client, @topic)
      :brod.stop_client(:test_client)
    end)

    #:timer.sleep(@brod_init_sleep_ms)

    messages
    |> Enum.each(fn msg ->
      :brod.produce_sync(:test_client, @topic, partition, "", msg)
    end)
  end

  defp kill_worker(partition) do
    worker_pid = Elsa.ElsaRegistry.whereis_name({:elsa_registry_name1, :"worker_#{@topic}_#{partition}"})
    Process.exit(worker_pid, :kill)

    assert false == Process.alive?(worker_pid)
  end
end

defmodule Test.BasicHandler do
  use Elsa.Consumer.MessageHandler

  def handle_messages(messages, state) do
    Enum.each(messages, &send(state.pid, {:message, &1}))
    {:ack, state}
  end
end
