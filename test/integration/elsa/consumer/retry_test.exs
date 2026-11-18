defmodule Elsa.Consumer.RetryTest do
  @moduledoc """
  Specifies the behaviour of a handler returning `{:retry, offset, state}`: the consumer
  rewinds to `offset` and those messages are delivered again, in order, before anything
  newer. (Contrast `{:no_ack, state}`, which does not redeliver; see
  `Elsa.Consumer.NoAckTest`.)

    * "redelivers from that offset": the handler asks for a retry once, at the offset of
      the first "poison" message in the middle of a batch.
    * "busy partition": the same failure scenario `NoAckTest` uses, but returning
      `{:retry, first_offset_of_batch, state}`, so nothing is lost even though later
      batches keep arriving.
  """
  use ExUnit.Case
  use Divo

  alias Elsa.Test.FailedBatchScenario, as: Scenario

  @endpoints Application.compile_env(:elsa_fi, :brokers)
  @connection :retry_test

  # How long to wait for the first handler call (group join + first fetch).
  @first_event_timeout 30_000
  # Once events start flowing, how long of a gap means "settled".
  @quiet_ms 5_000

  defmodule RetryOnceHandler do
    use Elsa.Consumer.MessageHandler

    def handle_messages(messages, %{pid: pid, once: once} = state) do
      offsets = Enum.map(messages, & &1.offset)
      poison = Enum.find(messages, &(&1.value == "poison"))

      # compare_exchange returns :ok only for the first caller, so we retry exactly once
      # even if the worker is restarted and the handler is re-initialised.
      if poison && :atomics.compare_exchange(once, 1, 0, 1) == :ok do
        send(pid, {:handled, offsets, {:retry, poison.offset}})
        {:retry, poison.offset, state}
      else
        send(pid, {:handled, offsets, :ack})
        {:ack, state}
      end
    end
  end

  @tag :retry
  test "{:retry, offset, state} redelivers from that offset, in order" do
    # Unique per run so a rerun against a broker that kept the old topic still works.
    unique = System.system_time(:millisecond)
    topic = "retry-test-topic-#{unique}"
    group = "retry-test-group-#{unique}"

    Patiently.wait_for!(
      fn -> :ok == Elsa.create_topic(@endpoints, topic, partitions: 1) end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, sup} =
      Elsa.ElsaSupervisor.start_link(
        connection: @connection,
        endpoints: @endpoints,
        group_consumer: [
          group: group,
          topics: [topic],
          handler: RetryOnceHandler,
          handler_init_args: %{pid: self(), once: :atomics.new(1, [])},
          config: [begin_offset: :earliest]
        ]
      )

    worker_name = {:elsa_registry_retry_test, :"worker_#{topic}_0"}

    Patiently.wait_for!(
      fn -> is_pid(Elsa.ElsaRegistry.whereis_name(worker_name)) end,
      dwell: 500,
      max_tries: 60
    )

    # offsets 0..5, "poison" sits at offset 3, in the middle of the batch
    :ok =
      Elsa.produce(
        @endpoints,
        topic,
        for(v <- ["m0", "m1", "m2", "poison", "m4", "m5"], do: {"k", v}),
        partition: 0
      )

    events = collect_events()
    summary = "handler events: #{inspect(events, charlists: :as_lists)}"

    {before_retry, [{retried_offsets, {:retry, 3}} | after_retry]} =
      Enum.split_while(events, fn {_offsets, outcome} -> not match?({:retry, _}, outcome) end)

    assert 3 in retried_offsets, summary

    # 1. The very next delivery after the retry starts at the requested offset...
    assert [{[3 | _], :ack} | _] = after_retry, "expected redelivery to start at offset 3. " <> summary

    # 2. ...nothing before the requested offset is redelivered (a crash/restart would
    #    replay from the last committed offset instead)...
    redelivered = Enum.flat_map(after_retry, fn {offsets, _} -> offsets end)
    assert Enum.all?(redelivered, &(&1 >= 3)), "offsets before 3 were redelivered. " <> summary

    # 3. ...and it is in order, with no gaps or duplicates, through to the end.
    assert redelivered == [3, 4, 5], "expected exactly [3, 4, 5] after the retry. " <> summary

    # 4. Offsets before the retry point were handled once and only once.
    assert Enum.flat_map(before_retry, fn {offsets, _} -> offsets end) ++ Enum.take_while(retried_offsets, &(&1 < 3)) ==
             [0, 1, 2],
           summary

    Supervisor.stop(sup)
  end

  @tag :retry
  test "retrying the failed batch on a busy partition redelivers it and loses nothing" do
    r = Scenario.run(:retry, "retry-busy")

    failed = Scenario.failed_offsets(r.events)
    assert failed != [], "expected one failed batch. " <> r.summary

    # Every offset acked exactly once, in order, the failed batch included, which is
    # redelivered before anything newer (later batches that were already prefetched are
    # dropped and fetched again after it).
    assert Scenario.acked_offsets(r.events) == Enum.to_list(0..(Scenario.total() - 1)), r.summary

    # The failed batch was handled twice: once failing, once succeeding.
    assert Scenario.handled_count(r.events, failed) == 2, r.summary

    # Commit reached the end of the log.
    total = Scenario.total()
    assert r.committed == total, "expected commit #{total}, got #{inspect(r.committed)}. " <> r.summary
  end

  # Collect {offsets, outcome} from the handler until it has been quiet for @quiet_ms.
  defp collect_events do
    receive do
      {:handled, offsets, outcome} -> collect_events([{offsets, outcome}])
    after
      @first_event_timeout -> flunk("handler was never called")
    end
  end

  defp collect_events(acc) do
    receive do
      {:handled, offsets, outcome} -> collect_events([{offsets, outcome} | acc])
    after
      @quiet_ms -> Enum.reverse(acc)
    end
  end
end
