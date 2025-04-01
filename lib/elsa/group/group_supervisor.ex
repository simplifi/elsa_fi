defmodule Elsa.Group.GroupSupervisor do
  @moduledoc """
  Orchestrates the creation of dynamic supervisor and worker
  processes for per-topic consumer groups, manager processes
  for coordinating topic/partition assignment, and a registry
  for differentiating named processes between consumer groups.
  """
  use Supervisor, restart: :transient

  import Elsa.ElsaSupervisor, only: [registry: 1]

  alias Elsa.ElsaRegistry

  @type init_opts :: [
          connection: Elsa.connection(),
          topics: [Elsa.topic()],
          group: String.t(),
          config: list
        ]

  @spec start_link(init_opts) :: GenServer.on_start()
  def start_link(init_arg \\ []) do
    connection = Keyword.fetch!(init_arg, :connection)
    Supervisor.start_link(__MODULE__, init_arg, name: {:via, ElsaRegistry, {registry(connection), __MODULE__}})
  end

  @impl Supervisor
  def init(init_arg) do
    children =
      [
        {Elsa.Group.Manager.WorkerSupervisor, init_arg},
        {Elsa.Group.Manager, manager_args(init_arg)}
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp manager_args(args) do
    args
    |> Keyword.put(:supervisor_pid, self())
  end
end
