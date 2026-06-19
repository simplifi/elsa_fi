defmodule Elsa.Group.OffsetOutOfRangeTest do
  @moduledoc """
  Group-consumer counterpart to `Elsa.Consumer.OffsetOutOfRangeTest`.

  A group consumer takes its start offset from the committed group offset, not config
  (see `brod_group_coordinator:resolve_begin_offsets/3`), so to trigger
  offset_out_of_range we make the *committed* offset invalid: commit a valid offset,
  grow the log, then advance the partition's log-start past it with `delete_records`
  (retention truncation). The topic is never deleted — that would drop the committed
  offset and the rejoin would start clean, never hitting OOR.
  """
  use ExUnit.Case
  use Divo

  import Record, only: [defrecord: 2, extract: 2]

  alias Elsa.Util, as: ElsaUtil

  @endpoints Application.compile_env(:elsa_fi, :brokers)
  @partition 0
  # Truncate the log front to this offset. Must sit above the committed offset (<= 3)
  # and below the high-water mark (13) so the committed offset becomes out of range.
  @truncate_before 8

  defrecord :kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")

  defmodule ForwardingHandler do
    use Elsa.Consumer.MessageHandler

    def handle_messages(messages, %{pid: pid} = state) do
      Enum.each(messages, &send(pid, {:message, &1}))
      {:ack, state}
    end
  end

  @tag :offset_out_of_range
  test "group consumer self-heals when retention truncates past the committed offset" do
    topic = "oor-group-topic"
    group = "oor-group-#{:erlang.unique_integer([:positive])}"

    create_topic!(topic)

    # Consume, then wait until the group commits. Polling the real offset (not sleeping)
    # also guards a false pass: no commit -> no OOR on rejoin.
    {:ok, sup1} = start_group(:oor_group_first, topic, group, :earliest)

    Enum.each(1..3, fn n -> :ok = Elsa.produce(@endpoints, topic, {"k", "msg#{n}"}, partition: @partition) end)
    assert_receive {:message, %Elsa.Message{value: "msg1"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "msg2"}}, 5_000
    assert_receive {:message, %Elsa.Message{value: "msg3"}}, 5_000

    committed = await_committed_offset(group, topic, @partition)

    assert committed < @truncate_before,
           "committed offset #{committed} must be below the truncation point #{@truncate_before} " <>
             "for it to become out of range"

    Supervisor.stop(sup1)

    # Grow the log, then truncate its front past the committed offset -> the committed
    # offset is now out of range.
    Enum.each(4..13, fn n -> :ok = Elsa.produce(@endpoints, topic, {"k", "fill#{n}"}, partition: @partition) end)
    :ok = truncate_log_before(topic, @partition, @truncate_before)

    # Rejoin the same group. brod fetches the committed offset (below log-start) ->
    # offset_out_of_range. A :latest reset only delivers post-reset messages, so
    # re-produce until one lands.
    {:ok, sup2} = start_group(:oor_group_rejoin, topic, group, :latest)

    assert_eventually_consumes(
      fn -> Elsa.produce(@endpoints, topic, {"k", "after-reset"}, partition: @partition) end,
      "after-reset"
    )

    Supervisor.stop(sup2)
  end

  defp start_group(connection, topic, group, reset_to) do
    Elsa.ElsaSupervisor.start_link(
      connection: connection,
      endpoints: @endpoints,
      group_consumer: [
        group: group,
        topics: [topic],
        handler: ForwardingHandler,
        handler_init_args: %{pid: self()},
        config: [begin_offset: reset_to, offset_commit_interval_seconds: 1]
      ]
    )
  end

  defp create_topic!(topic) do
    Patiently.wait_for!(
      fn -> :ok == Elsa.create_topic(@endpoints, topic, partitions: 1) end,
      dwell: 1_000,
      max_tries: 30
    )
  end

  # Poll until Kafka reports a committed offset for the partition, then return it.
  defp await_committed_offset(group, topic, partition) do
    Patiently.wait_for!(
      fn -> committed_offset(group, topic, partition) >= 0 end,
      dwell: 200,
      max_tries: 50
    )

    committed_offset(group, topic, partition)
  end

  defp committed_offset(group, topic, partition) do
    case :brod.fetch_committed_offsets(@endpoints, [], group) do
      {:ok, topics} ->
        topics
        |> Enum.find(%{}, &(&1.name == topic))
        |> Map.get(:partitions, [])
        |> Enum.find(%{}, &(&1.partition_index == partition))
        |> Map.get(:committed_offset, -1)

      _ ->
        -1
    end
  end

  # Advance the partition's log-start to `before_offset` (retention truncation).
  # delete_records is a partition-leader request; the single-broker cluster makes
  # `:any` the leader. Asserts it took effect.
  defp truncate_log_before(topic, partition, before_offset) do
    ElsaUtil.with_connection(@endpoints, :any, fn connection ->
      vsn = ElsaUtil.get_api_version(connection, :delete_records)

      request =
        :kpro.make_request(:delete_records, vsn, %{
          topics: [%{topic: topic, partitions: [%{partition: partition, offset: before_offset}]}],
          timeout: 5_000
        })

      {:ok, response} = :kpro.request_sync(connection, request, 10_000)

      partition_result =
        kpro_rsp(response, :msg).topics
        |> Enum.find(&(&1.topic == topic))
        |> Map.fetch!(:partitions)
        |> Enum.find(&(&1.partition == partition))

      assert partition_result.error_code == :no_error,
             "delete_records failed: #{inspect(partition_result)}"

      assert partition_result.low_watermark == before_offset,
             "expected log-start #{before_offset}, got #{partition_result.low_watermark}"

      :ok
    end)
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
