defmodule Elsa.ElsaRegistryTest do
  use ExUnit.Case
  import TestHelper
  alias Elsa.ElsaRegistry

  @registry :elsa_registry

  defmodule TestServer do
    use GenServer

    def start_link(args) do
      name = Keyword.fetch!(args, :name)
      GenServer.start_link(__MODULE__, args, name: name)
    end

    def init(args) do
      {:ok, Map.new(args)}
    end

    def handle_info(msg, state) do
      send(state.pid, msg)
      {:noreply, state}
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    {:ok, pid} = ElsaRegistry.start_link(name: @registry)

    on_exit(fn -> assert_down(pid) end)

    :ok
  end

  test "gives registered processes a local name and releases it on unregister" do
    {:ok, pid} = Agent.start_link(fn -> :ok end, name: {:via, ElsaRegistry, {@registry, :agent}})

    assert Process.whereis(:"elsa_registry.agent") == pid

    ElsaRegistry.unregister_name({@registry, :agent})
    assert Process.whereis(:"elsa_registry.agent") == nil
  end

  test "does not fail for processes that already have a name" do
    {:ok, pid} = Agent.start_link(fn -> :ok end, name: :already_named)

    assert :yes == ElsaRegistry.register_name({@registry, :named}, pid)
    assert Process.whereis(:already_named) == pid
  end

  test "registers and lookups pids by name" do
    Agent.start_link(fn -> :agent_value end, name: {:via, ElsaRegistry, {@registry, :agent}})

    assert :agent_value == Agent.get({:via, ElsaRegistry, {@registry, :agent}}, fn s -> s end)
  end

  test "pid is automatically removed when process exits" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end, name: {:via, ElsaRegistry, {@registry, :agent}})

    Process.exit(pid, :shutdown)

    Process.sleep(1_000)

    assert false == Process.alive?(pid)
    assert :undefined == ElsaRegistry.whereis_name({@registry, :agent})
  end

  test "pid can be registered by another process" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end)
    ElsaRegistry.register_name({@registry, :agent}, pid)

    assert :agent_value == Agent.get({:via, ElsaRegistry, {@registry, :agent}}, fn s -> s end)
  end

  test "pid can be unregistered" do
    {:ok, pid} = Agent.start_link(fn -> :agent_value end, name: {:via, ElsaRegistry, {@registry, :agent}})

    ElsaRegistry.unregister_name({@registry, :agent})

    assert :undefined == ElsaRegistry.whereis_name({@registry, :agent})
    assert_down(pid)
  end

  test "send will send a message to pid registered by key" do
    {:ok, pid} = TestServer.start_link(pid: self(), name: {:via, ElsaRegistry, {@registry, :test_server}})

    ElsaRegistry.send({@registry, :test_server}, :hello)

    assert_receive :hello
    assert_down(pid)
  end
end
