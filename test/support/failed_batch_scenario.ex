defmodule Elsa.Test.FailedBatchScenario do
  @moduledoc """
  Shared scenario for the integration tests that exercise what a handler returns for a
  failed batch (`:no_ack` vs `:retry`). Integration tests only: it needs a real Kafka.

  A "busy partition": 6 waves of 3 messages are produced one after another, so each wave
  arrives as its own batch (offsets 0..17). The first batch containing "poison" (in wave 1)
  fails once; the handler returns `on_failure` for it (`:no_ack` or `{:retry, first_offset}`).
  Every other batch is acked, and later batches keep arriving.

  `run/2` returns the ordered handler events (`{offsets, outcome}`) and the offset the
  broker has committed for the group, so tests can assert on what was and wasn't processed.
  """
  import ExUnit.Assertions, only: [flunk: 1]
  import ExUnit.Callbacks, only: [on_exit: 1]

  @waves 6
  @per_wave 3
  @total @waves * @per_wave
  @quiet_ms 5_000
  @first_event_timeout 30_000
  @commit_wait_ms 30_000

  defmodule Handler do
    @moduledoc """
    Acks every batch except the first one containing "poison". For that one it returns
    whatever `on_failure` says (`:no_ack` or `:retry`); nothing else differs.
    """
    use Elsa.Consumer.MessageHandler

    def handle_messages(messages, %{pid: pid, once: once, on_failure: on_failure} = state) do
      offsets = Enum.map(messages, & &1.offset)
      poison? = Enum.any?(messages, &(&1.value == "poison"))

      # compare_exchange returns :ok only for the first caller: fail exactly once, even
      # if the worker is restarted and the handler re-initialised.
      if poison? and :atomics.compare_exchange(once, 1, 0, 1) == :ok do
        send(pid, {:handled, offsets, on_failure})

        case on_failure do
          :no_ack -> {:no_ack, state}
          :retry -> {:retry, hd(offsets), state}
        end
      else
        send(pid, {:handled, offsets, :ack})
        {:ack, state}
      end
    end
  end

  @doc "Total number of messages the scenario produces (offsets 0..total-1)."
  def total, do: @total

  @doc """
  Runs the scenario with the handler returning `on_failure` (`:no_ack` or `:retry`) for the
  failed batch. `tag` namespaces the topic, group and connection.
  """
  @spec run(:no_ack | :retry, String.t()) :: %{events: list, summary: String.t(), committed: term}
  def run(on_failure, tag) do
    endpoints = Application.fetch_env!(:elsa_fi, :brokers)

    # Unique per run so a rerun against a broker that kept the old topic still works.
    unique = System.system_time(:millisecond)
    topic = "#{tag}-topic-#{unique}"
    group = "#{tag}-group-#{unique}"
    connection = String.to_atom("#{tag}-conn")
    worker_name = {String.to_atom("elsa_registry_#{connection}"), :"worker_#{topic}_0"}

    Patiently.wait_for!(
      fn -> :ok == Elsa.create_topic(endpoints, topic, partitions: 1) end,
      dwell: 1_000,
      max_tries: 30
    )

    {:ok, sup} =
      Elsa.ElsaSupervisor.start_link(
        connection: connection,
        endpoints: endpoints,
        group_consumer: [
          group: group,
          topics: [topic],
          handler: Handler,
          handler_init_args: %{pid: self(), once: :atomics.new(1, []), on_failure: on_failure},
          config: [begin_offset: :earliest]
        ]
      )

    # The supervisor is linked to the test process, so it may already be going down when
    # on_exit runs; ignore that race.
    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    Patiently.wait_for!(
      fn -> is_pid(Elsa.ElsaRegistry.whereis_name(worker_name)) end,
      dwell: 500,
      max_tries: 60
    )

    # Wave 0, then wait for it to be handled so the group is really consuming and the
    # later waves arrive as separate batches.
    produce_wave(endpoints, topic, 0)

    # Peek: put the event back so it is still part of the recorded history.
    receive do
      {:handled, _, _} = event -> send(self(), event)
    after
      @first_event_timeout -> flunk("handler was never called")
    end

    for wave <- 1..(@waves - 1) do
      Process.sleep(1_000)
      produce_wave(endpoints, topic, wave)
    end

    events = collect_events([])

    %{
      events: events,
      summary: "handler events: #{inspect(events, charlists: :as_lists)}",
      committed: committed_after_wait(endpoints, group, topic, tag, @total)
    }
  end

  @doc "Offsets of every batch the handler did not ack (the failed batch)."
  def failed_offsets(events) do
    events |> Enum.reject(&match?({_, :ack}, &1)) |> Enum.flat_map(&elem(&1, 0))
  end

  @doc "Offsets of every acked batch, in the order they were acked."
  def acked_offsets(events) do
    events |> Enum.filter(&match?({_, :ack}, &1)) |> Enum.flat_map(&elem(&1, 0))
  end

  @doc "Number of handler calls whose batch included any of `offsets`."
  def handled_count(events, offsets) do
    Enum.count(events, fn {batch, _} -> Enum.any?(batch, &(&1 in offsets)) end)
  end

  # wave 1 carries the poison message in the middle of its batch
  defp produce_wave(endpoints, topic, wave) do
    values = for i <- 0..(@per_wave - 1), do: if(wave == 1 and i == 1, do: "poison", else: "w#{wave}m#{i}")
    :ok = Elsa.produce(endpoints, topic, Enum.map(values, &{"k", &1}), partition: 0)
  end

  # Collect {offsets, outcome} until the handler has been quiet for @quiet_ms.
  defp collect_events(acc) do
    receive do
      {:handled, offsets, outcome} -> collect_events([{offsets, outcome} | acc])
    after
      @quiet_ms -> Enum.reverse(acc)
    end
  end

  # Poll the broker for the group's committed offset until it reaches `min` or we time out.
  defp committed_after_wait(endpoints, group, topic, tag, min) do
    client = String.to_atom("#{tag}-admin")
    {:ok, _} = :brod.start_link_client(endpoints, client)
    on_exit(fn -> :brod.stop_client(client) end)
    wait_committed(client, group, topic, min, System.monotonic_time(:millisecond))
  end

  defp wait_committed(client, group, topic, min, t0) do
    committed = committed(client, group, topic)

    cond do
      is_integer(committed) and committed >= min ->
        committed

      System.monotonic_time(:millisecond) - t0 > @commit_wait_ms ->
        committed

      true ->
        Process.sleep(1_000)
        wait_committed(client, group, topic, min, t0)
    end
  end

  defp committed(client, group, topic) do
    case :brod.fetch_committed_offsets(client, group) do
      {:ok, topics} ->
        List.first(for t <- topics, t.name == topic, p <- t.partitions, p.partition_index == 0, do: p.committed_offset)

      _ ->
        nil
    end
  end
end
