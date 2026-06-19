defmodule Elsa.Consumer.OffsetOutOfRangeTest do
  @moduledoc """
  Forces `offset_out_of_range` for a simple consumer by subscribing at a
  `begin_offset` past the high-water mark of a fresh topic; brod's first fetch
  returns the error, which the worker heals by re-subscribing.
  """
  use ExUnit.Case
  use Divo

  @endpoints Application.compile_env(:elsa_fi, :brokers)

  defmodule ForwardingHandler do
    use Elsa.Consumer.MessageHandler

    def handle_messages(messages, %{pid: pid} = state) do
      Enum.each(messages, &send(pid, {:message, &1}))
      {:ack, state}
    end
  end

  @tag :offset_out_of_range
  test "self-heals an out-of-range begin offset and resumes consuming at :latest" do
    topic = "oor-selfheal-topic"

    Patiently.wait_for!(
      fn -> :ok == Elsa.create_topic(@endpoints, topic, partitions: 1) end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, sup} =
      Elsa.ElsaSupervisor.start_link(
        connection: :oor_selfheal,
        endpoints: @endpoints,
        consumer: [
          topic: topic,
          partition: 0,
          # Past the high-water mark -> offset_out_of_range on the first fetch.
          begin_offset: 1_000_000,
          # Reset target after OOR.
          config: [begin_offset: :latest],
          handler: ForwardingHandler,
          handler_init_args: %{pid: self()}
        ]
      )

    # A :latest reset only delivers messages produced after re-subscribe, so re-produce
    # until one lands (polls for the reset, no sleep).
    assert_eventually_consumes(
      fn -> Elsa.produce(@endpoints, topic, {"k", "after-reset"}, partition: 0) end,
      "after-reset"
    )

    Supervisor.stop(sup)
  end

  # Produce repeatedly until a message with `value` is delivered, or fail.
  defp assert_eventually_consumes(produce_fun, value, attempts \\ 30, delay \\ 500)

  defp assert_eventually_consumes(_produce_fun, value, 0, _delay) do
    flunk("never consumed #{inspect(value)} after re-subscribe")
  end

  defp assert_eventually_consumes(produce_fun, value, attempts, delay) do
    :ok = produce_fun.()

    receive do
      {:message, %Elsa.Message{value: ^value}} -> :ok
    after
      delay -> assert_eventually_consumes(produce_fun, value, attempts - 1, delay)
    end
  end
end
