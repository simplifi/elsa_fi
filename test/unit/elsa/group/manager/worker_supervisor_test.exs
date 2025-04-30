defmodule Elsa.Group.Manager.WorkerSupervisorTest do
  use ExUnit.Case

  import Mock
  import Record, only: [defrecord: 2, extract: 2]

  alias Elsa.Consumer.Worker
  alias Elsa.ElsaRegistry
  alias Elsa.ElsaSupervisor
  alias Elsa.Group.Acknowledger
  alias Elsa.Group.Manager.State
  alias Elsa.Group.Manager.WorkerSupervisor

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  @topic "worker_sup_test"
  @connection :worker_sup_test

  defmodule FakeWorker do
    use GenServer

    def start_link(args) do
      GenServer.start_link(__MODULE__, args)
    end

    def init(args) do
      {:ok, args}
    end
  end

  setup_with_mocks([
    {Worker, [],
     [
       child_spec: &FakeWorker.child_spec/1
     ]}
  ]) do
    {:ok, registry} = start_supervised({ElsaRegistry, [name: ElsaSupervisor.registry(@connection)]})

    :yes = ElsaRegistry.register_name({registry, :brod_group_coordinator}, self())

    start_supervised({WorkerSupervisor, [connection: @connection]})

    {:ok, acknowledger} = start_supervised({Acknowledger, [connection: @connection]})

    [acknowledger: acknowledger]
  end

  test "start_worker updates acknowledger immediately with offsets", %{acknowledger: acknowledger} do
    partition = 0
    test_offset = 5

    WorkerSupervisor.start_worker(
      %{},
      partition,
      brod_received_assignment(topic: @topic, partition: partition, begin_offset: test_offset),
      %State{connection: @connection, acknowledger_pid: acknowledger}
    )

    assert test_offset == Acknowledger.get_latest_offset(acknowledger, @topic, partition)
  end

  test "start_worker doesn't set anything on Acknowledger if begin_offset is undefined", %{acknowledger: acknowledger} do
    partition = 0

    WorkerSupervisor.start_worker(
      %{},
      partition,
      brod_received_assignment(topic: @topic, partition: partition, begin_offset: :undefined),
      %State{connection: @connection, acknowledger_pid: acknowledger}
    )

    assert nil == Acknowledger.get_latest_offset(acknowledger, @topic, partition)
  end
end
