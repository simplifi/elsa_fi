defmodule Elsa.Group.LifecycleHooksTest do
  use ExUnit.Case

  import Elsa.Group.Manager, only: [brod_received_assignment: 1]
  import Mock

  alias Elsa.Group.Acknowledger
  alias Elsa.Group.Manager
  alias Elsa.Group.Manager.WorkerManager

  setup_with_mocks([
    {WorkerManager, [],
     [
       start_worker: fn _, _, _, _ -> :workers end,
       stop_all_workers: fn _, _ -> :workers end
     ]}
  ]) do
    test_pid = self()
    Agent.start_link(fn -> test_pid end, name: __MODULE__)

    test_pid = self()

    state = %{
      connection: :fake_test_name,
      workers: :workers,
      group: "group1",
      assignment_received_handler: fn group, topic, partition, generation_id ->
        send(test_pid, {:assignment_received, group, topic, partition, generation_id})
        :ok
      end,
      assignments_revoked_handler: fn ->
        send(test_pid, :assignments_revoked)
        :ok
      end,
      generation_id: :generation_id
    }

    [state: state]
  end

  test "assignments_recieved calls lifecycle hook", %{state: state} do
    with_mocks([
      {Elsa.ElsaRegistry, [], [whereis_name: fn _ -> :ack_pid end]},
      {Acknowledger, [], [update_generation_id: fn _, _ -> :ok end]}
    ]) do
      assignments = [
        brod_received_assignment(topic: "topic1", partition: 0, begin_offset: 0),
        brod_received_assignment(topic: "topic1", partition: 1, begin_offset: 0)
      ]

      {:reply, :ok, ^state} =
        Manager.handle_call({:process_assignments, :member_id, :generation_id, assignments}, self(), state)

      assert_received {:assignment_received, "group1", "topic1", 0, :generation_id}
      assert_received {:assignment_received, "group1", "topic1", 1, :generation_id}
    end
  end

  test "lifecycle handler can stop processing assignments", %{state: state} do
    error_state = %{state | assignment_received_handler: fn _, _, _, _ -> {:error, :some_reason} end}

    assignments = [
      brod_received_assignment(topic: "topic1", partition: 0, begin_offset: 0),
      brod_received_assignment(topic: "topic1", partition: 1, begin_offset: 0)
    ]

    {:stop, :some_reason, {:error, :some_reason}, ^error_state} =
      Manager.handle_call(
        {:process_assignments, :member_id, :generation_id, assignments},
        self(),
        error_state
      )

    assert_not_called(WorkerManager.start_worker(:_, :_, :_, :_))
  end

  test "assignments_revoked calls lifecycle hook", %{state: state} do
    {:reply, :ok, new_state} = Manager.handle_call(:revoke_assignments, self(), state)

    assert new_state == %{state | generation_id: nil}
    assert_received :assignments_revoked
  end
end
