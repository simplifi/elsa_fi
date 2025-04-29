defmodule Elsa.Group.Acknowledger do
  @moduledoc """
  Handles acknowledgement of messages to the
  group coordinator to prevent the group manager
  from queuing up messages for acknowledgement
  when events such as a rebalance occur.
  """
  use GenServer
  import Elsa.ElsaSupervisor, only: [registry: 1]
  alias Elsa.ElsaRegistry
  require Logger

  @doc """
  Trigger acknowledgement of processed messages back to the cluster.
  """
  @spec ack(Elsa.connection(), Elsa.topic(), Elsa.partition(), Elsa.Group.Manager.generation_id(), integer()) :: :ok
  def ack(connection, topic, partition, generation_id, offset) do
    acknowledger = {:via, ElsaRegistry, {registry(connection), __MODULE__}}
    GenServer.cast(acknowledger, {:ack, topic, partition, generation_id, offset})
  end

  @doc """
  Trigger acknowledgement of processed messages back to the cluster.
  """
  @spec ack(Elsa.connection(), %{
          topic: Elsa.topic(),
          partition: Elsa.partition(),
          generation_id: Elsa.Group.Manager.generation_id(),
          offset: integer()
        }) :: :ok
  def ack(connection, %{topic: topic, partition: partition, generation_id: generation_id, offset: offset}) do
    ack(connection, topic, partition, generation_id, offset)
  end

  @doc """
  Sync the group generation ID back to the acknowledger state for validation.
  """
  @spec update_generation_id(GenServer.server(), Elsa.Group.Manager.generation_id()) :: :ok
  def update_generation_id(acknowledger, generation_id) do
    GenServer.cast(acknowledger, {:update_generation, generation_id})
  end

  @doc """
  Retrieve the latest offset for a topic and partition. Primarily used for reinitializing
  consumer workers to the latest unacknowledged offset after a rebalance or other disruption.
  """
  @spec get_latest_offset(GenServer.server(), Elsa.topic(), Elsa.partition()) :: Elsa.Group.Manager.begin_offset() | nil
  def get_latest_offset(acknowledger, topic, partition) do
    GenServer.call(acknowledger, {:get_latest_offset, topic, partition})
  end

  @doc """
  Update the latest offset for a topic and partition. Primarily used when a consumer worker
  gets a partition assignments with an offset, so that future calls to get_latest_offset will
  return an up-to-date result.

  Failing to set this on partition assignment can cause a rewind to occur if the worker
  restarts before acking anything.
  """
  @spec set_latest_offset(GenServer.server(), Elsa.topic(), Elsa.partition(), Elsa.Group.Manager.begin_offset()) :: :ok
  def set_latest_offset(acknowledger, topic, partition, offset) do
    GenServer.call(acknowledger, {:set_latest_offset, topic, partition, offset})
  end

  @doc """
  Instantiate an acknowledger process and register it to the Elsa registry.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(opts) do
    connection = Keyword.fetch!(opts, :connection)
    GenServer.start_link(__MODULE__, opts, name: {:via, ElsaRegistry, {registry(connection), __MODULE__}})
  end

  @impl GenServer
  def init(opts) do
    connection = Keyword.fetch!(opts, :connection)

    state = %{
      connection: connection,
      current_offsets: %{},
      generation_id: nil,
      group_coordinator_pid: nil
    }

    {:ok, state, {:continue, :get_coordinator}}
  end

  @impl GenServer
  @spec handle_continue(:get_coordinator, %{
          :connection => atom() | binary(),
          :group_coordinator_pid => any(),
          optional(any()) => any()
        }) ::
          {:noreply,
           %{:connection => atom() | binary(), :group_coordinator_pid => :undefined | pid(), optional(any()) => any()}}
  def handle_continue(:get_coordinator, state) do
    group_coordinator_pid = ElsaRegistry.whereis_name({registry(state.connection), :brod_group_coordinator})

    {:noreply, %{state | group_coordinator_pid: group_coordinator_pid}}
  end

  @impl GenServer
  def handle_call({:get_latest_offset, topic, partition}, _pid, %{current_offsets: offsets} = state) do
    latest_offset = Map.get(offsets, {topic, partition})

    {:reply, latest_offset, state}
  end

  @impl GenServer
  def handle_call({:set_latest_offset, topic, partition, offset}, _pid, state) do
    new_offsets = Map.update(state.current_offsets, {topic, partition}, offset, fn existing_offset ->
      if offset < existing_offset do
        # If this was called with an older offset, ignore it and log a warning.
        # This should never happen, but if it does this check will avoid causing a rewind.
        Logger.warn(
          "#{__MODULE__} Ignoring :set_latest_offset - \
          it was called for topic #{topic}, partition #{partition} with an offset less than the existing one. \
          (#{offset} < #{existing_offset})")
        existing_offset
      else
        offset
      end
    end)

    {:reply, :ok, %{state | current_offsets: new_offsets}}
  end

  @impl GenServer
  def handle_cast({:update_generation, generation_id}, state) do
    {:noreply, %{state | generation_id: generation_id}}
  end

  @impl GenServer
  def handle_cast({:ack, topic, partition, generation_id, offset}, state) do
    case state.generation_id == generation_id do
      true ->
        :ok = :brod_group_coordinator.ack(state.group_coordinator_pid, generation_id, topic, partition, offset)
        :ok = Elsa.Consumer.ack(state.connection, topic, partition, offset)

        new_offsets = update_offset(state.current_offsets, topic, partition, offset)
        {:noreply, %{state | current_offsets: new_offsets}}

      false ->
        Logger.warn(
          "#{__MODULE__}: Invalid generation_id #{state.generation_id} == #{generation_id}, ignoring ack - topic #{topic} partition #{partition} offset #{offset}"
        )

        {:noreply, state}
    end
  end

  defp update_offset(offsets, topic, partition, offset) do
    Map.put(offsets, {topic, partition}, offset + 1)
  end
end
