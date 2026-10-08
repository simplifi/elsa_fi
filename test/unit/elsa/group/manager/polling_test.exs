defmodule Elsa.Group.Manager.PollingTest do
  use ExUnit.Case

  import Mock

  alias Elsa.ElsaRegistry
  alias Elsa.Group.Acknowledger
  alias Elsa.Group.Manager
  alias Elsa.Group.Manager.State
  alias Elsa.Util

  @connection :group_polling_test
  @topic "topic"

  defp state(partition_count) do
    %State{
      connection: @connection,
      group: "group",
      topics: [@topic],
      group_coordinator_pid: self(),
      partition_counts: %{@topic => partition_count},
      poll: false
    }
  end

  defp initialization_state do
    %State{
      connection: @connection,
      group: "group",
      topics: [@topic],
      config: [],
      poll: 300_000
    }
  end

  defp metadata_mocks(partition_count) do
    [
      {Elsa.Util, [],
       [
         with_client: fn _registry, function -> function.(:brod_client) end,
         get_endpoints: fn :brod_client -> {:ok, [:endpoint]} end,
         partition_count: fn [:endpoint], @topic, _retry_config -> {:ok, partition_count} end
       ]},
      {:brod_client, [],
       [
         get_metadata: fn :brod_client, @topic -> {:ok, %{}} end
       ]}
    ]
  end

  test "initializes coordinator, acknowledger, partition counts, and polling" do
    mocks =
      metadata_mocks(2) ++
        [
          {:brod_group_coordinator, [],
           [
             start_link: fn connection, group, topics, config, module, _member_pid ->
               assert connection == @connection
               assert group == "group"
               assert topics == [@topic]
               assert config == []
               assert module == Manager
               {:ok, :group_coordinator_pid}
             end
           ]},
          {ElsaRegistry, [],
           [
             register_name: fn {registry, :brod_group_coordinator}, :group_coordinator_pid ->
               assert registry == :elsa_registry_group_polling_test
               :yes
             end
           ]},
          {Acknowledger, [],
           [
             start_link: fn connection: @connection -> {:ok, :acknowledger_pid} end
           ]}
        ]

    with_mocks(mocks) do
      {:noreply, new_state} = Manager.handle_continue(:initialize, initialization_state())

      assert new_state.group_coordinator_pid == :group_coordinator_pid
      assert new_state.acknowledger_pid == :acknowledger_pid
      assert new_state.partition_counts == %{@topic => 2}
    end
  end

  test "does not rebalance when partition counts are unchanged" do
    with_mocks(metadata_mocks(1)) do
      {_reply, new_state} = Manager.handle_info(:poll, state(1))

      refute_received :group_rebalance
      assert new_state.partition_counts == %{@topic => 1}
    end
  end

  test "refreshes metadata and rebalances when a partition count changes" do
    test_pid = self()

    mocks =
      metadata_mocks(2) ++
        [
          {:brod_group_coordinator, [],
           [
             update_topics: fn ^test_pid, [@topic] ->
               send(test_pid, :group_rebalance)
               :ok
             end
           ]}
        ]

    with_mocks(mocks) do
      {_reply, new_state} = Manager.handle_info(:poll, state(1))

      assert_called(Util.with_client(:elsa_registry_group_polling_test, :_))
      assert_called(Util.get_endpoints(:brod_client))
      assert_called(Util.partition_count([:endpoint], @topic, :_))
      assert new_state.partition_counts == %{@topic => 2}
      assert_called(:brod_client.get_metadata(:brod_client, @topic))
      assert_called(:brod_group_coordinator.update_topics(:_, [@topic]))
    end
  end
end
