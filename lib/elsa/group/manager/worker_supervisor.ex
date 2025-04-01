defmodule Elsa.Group.Manager.WorkerSupervisor do
  @moduledoc """
  Provides functions to encapsulate the management of worker
  processes by the consumer group manager.
  """
  use Supervisor, restart: :transient

  import Record, only: [defrecord: 2, extract: 2]
  import Elsa.Supervisor, only: [registry: 1]

  alias Elsa.Util

  require Logger

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  defmodule WorkerState do
    @moduledoc """
    Tracks the running state of the worker process from the perspective of the group manager.
    """
    defstruct [:pid, :ref, :generation_id, :topic, :partition, :latest_offset]
  end

  @doc """
  Start the main Supervisor.  On init this will start a single DynamicSupervisor as a child.

  Options:
    :connection - Elsa.connection that will identify this supervisor in the Elsa registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    connection = Keyword.fetch!(opts, :connection)
    Supervisor.start_link(__MODULE__, opts, name: {:via, Elsa.Registry, {registry(connection), __MODULE__}})
  end

  @impl true
  @spec init(keyword()) :: {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  def init(opts) do
    connection = Keyword.fetch!(opts, :connection)

    Logger.info("Initializing WorkerSupervisor #{inspect(self())} for connection #{inspect(connection)}")

    children = [
      {DynamicSupervisor,
       [id: :worker_dynamic_supervisor, name: {:via, Elsa.Registry, {registry(connection), :worker_dynamic_supervisor}}]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Retrieve the generation id, used in tracking assignments of workers to topic/partition,
  from the worker state map.
  """
  @spec get_generation_id(map(), Elsa.topic(), Elsa.partition()) :: Elsa.Group.Manager.generation_id()
  def get_generation_id(workers, topic, partition) do
    Map.get(workers, {topic, partition})
    |> Map.get(:generation_id)
  end

  @doc """
  Terminate all workers under the supervisor, giving them a chance to exit gracefully.
  """
  @spec stop_all_workers(Elsa.connection(), map()) :: map()
  def stop_all_workers(connection, workers) do
    # Demonitor all the workers, so that our client doesn't get a bunch of :EXIT signals
    workers
    |> Map.values()
    |> Enum.each(fn worker ->
      Logger.info("Gracefully stopping group worker #{inspect(worker)}")
      Process.demonitor(worker.ref)
    end)

    # Stop the dynamic supervisor in , which will in turn stop all of the children
    Util.with_registry(connection, fn registry ->
      # This is the Supervisor created in start_link, for this specific connection.
      module_supervisor = Elsa.Registry.whereis_name({registry, __MODULE__})
      # This is the DynamicSupervisor created in init
      dynamic_worker_supervisor = Elsa.Registry.whereis_name({registry, :worker_dynamic_supervisor})

      # Synchronously stops the DynamicSupervisor and its children
      DynamicSupervisor.stop(dynamic_worker_supervisor)

      # Restart the dynamic supervisor
      _ = Supervisor.restart_child(module_supervisor, :worker_dynamic_supervisor)
    end)

    %{}
  end

  @doc """
  Restart the specified worker from the manager state. Retrieve the latest recorded
  offset and pass it to the new worker to pick up where the previous left off if it
  has been recorded.
  """
  @spec restart_worker(map(), reference(), struct()) :: map()
  def restart_worker(workers, ref, %Elsa.Group.Manager.State{} = state) do
    worker = get_by_ref(workers, ref)

    latest_offset =
      Elsa.Group.Acknowledger.get_latest_offset(
        {:via, Elsa.Registry, {registry(state.connection), Elsa.Group.Acknowledger}},
        worker.topic,
        worker.partition
      ) || worker.latest_offset

    assignment = brod_received_assignment(topic: worker.topic, partition: worker.partition, begin_offset: latest_offset)

    start_worker(workers, worker.generation_id, assignment, state)
  end

  @doc """
  Construct an argument payload for instantiating a worker process, generate a
  topic/partition assignment and instantiate the worker process with both under
  the dynamic supervisor. Record the manager-relevant information and store in the
  manager state map tracking active worker processes.
  """
  @spec start_worker(map(), integer(), tuple(), struct()) :: map()
  def start_worker(workers, generation_id, assignment, %Elsa.Group.Manager.State{} = state) do
    assignment = Enum.into(brod_received_assignment(assignment), %{})

    init_args = [
      topic: assignment.topic,
      partition: assignment.partition,
      generation_id: generation_id,
      begin_offset: assignment.begin_offset,
      handler: state.handler,
      handler_init_args: state.handler_init_args,
      connection: state.connection,
      config: state.config
    ]

    Logger.info("Starting group consumer worker: #{inspect(init_args)}")

    supervisor = {:via, Elsa.Registry, {registry(state.connection), :worker_dynamic_supervisor}}
    {:ok, worker_pid} = DynamicSupervisor.start_child(supervisor, {Elsa.Consumer.Worker, init_args})
    ref = Process.monitor(worker_pid)

    new_worker = %WorkerState{
      pid: worker_pid,
      ref: ref,
      generation_id: generation_id,
      topic: assignment.topic,
      partition: assignment.partition,
      latest_offset: assignment.begin_offset
    }

    Logger.info("Group consumer worker started: #{inspect(new_worker)}")

    Map.put(workers, {assignment.topic, assignment.partition}, new_worker)
  end

  defp get_by_ref(workers, ref) do
    workers
    |> Map.values()
    |> Enum.find(fn worker -> worker.ref == ref end)
  end
end
