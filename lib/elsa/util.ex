defmodule Elsa.Util do
  @moduledoc """
  Provides functions for simplifying first-class interactions (consuming and
  producing) such as connecting to a cluster and establishing a persistent
  client process for interacting with a cluster.
  """

  require Logger

  alias Elsa.RetryConfig

  @default_max_chunk_size 900_000
  @timestamp_size_in_bytes 10

  @doc """
  Wrap establishing a connection to a cluster for performing an operation.
  """
  @spec with_connection(Elsa.endpoints(), atom(), fun()) :: term()
  def with_connection(endpoints, type \\ :any, fun) when is_function(fun) do
    endpoints
    |> reformat_endpoints()
    |> connect(type)
    |> do_with_connection(fun)
  end

  @doc """
  Retrieves the appropriate registry for the given value and validates it exists.
  Executes the function with the registry name if it successfully locates one.
  """
  @spec with_registry(atom() | String.t(), (atom() -> term())) :: term() | {:error, String.t()}
  def with_registry(connection, function) when is_function(function, 1) do
    registry = Elsa.ElsaSupervisor.registry(connection)

    case Process.whereis(registry) do
      nil -> {:error, "Elsa with connection #{connection} has not been started correctly"}
      _pid -> function.(registry)
    end
  end

  @doc """
  Retrieves the pid of a brod client process if it exists and executes the
  given function against the client.
  """
  @spec with_client(atom(), (pid() -> term())) :: term() | {:error, String.t()}
  def with_client(registry, function) when is_function(function, 1) do
    case Elsa.ElsaRegistry.whereis_name({registry, :brod_client}) do
      :undefined -> {:error, "Unable to find brod_client in registry(#{registry})"}
      pid -> function.(pid)
    end
  end

  @doc """
  Convert supplied cluster endpoints from common keyword list format to
  brod-compatible tuple.
  """
  @spec reformat_endpoints(keyword()) :: [{charlist(), integer()}]
  def reformat_endpoints(endpoints) do
    Enum.map(endpoints, fn {key, value} -> {to_charlist(key), value} end)
  end

  @doc """
  Retrieve the api version of the desired operation supported by the
  connected cluster.
  """
  @spec get_api_version(pid(), atom()) :: non_neg_integer()
  def get_api_version(connection, api) do
    {:ok, api_versions} = :kpro.get_api_versions(connection)
    {_, version} = Map.get(api_versions, api)
    version
  end

  @doc """
  Determines if client pid is alive
  """
  @spec client?(pid() | atom()) :: boolean()
  def client?(pid) when is_pid(pid) do
    Process.alive?(pid)
  end

  def client?(client) when is_atom(client) do
    case Process.whereis(client) do
      pid when is_pid(pid) -> client?(pid)
      nil -> false
    end
  end

  @doc """
  Create a named client connection process for managing interactions
  with the connected cluster.
  """
  @spec start_client(keyword(), atom()) :: :ok | {:error, term()}
  def start_client(endpoints, name, config \\ []) do
    :brod.start_client(endpoints, name, config)
  end

  @spec chunk_by_byte_size(any(), integer()) :: list()
  @doc """
  Process messages into chunks of size up to the size specified by the calling function in bytes,
  and determined by the function argument. If no chunk size is specified the default maximum
  size a chunk will be is approximately 1 megabyte. If no sizing function is provided to construct
  the appropriately sized chunks, the internal function based on Kernel.byte_size/1 is used.
  """
  @spec chunk_by_byte_size(term(), integer(), fun()) :: [term()]
  def chunk_by_byte_size(collection, chunk_byte_size \\ @default_max_chunk_size, byte_size_function \\ &get_byte_size/1) do
    collection
    |> Enum.chunk_while({0, []}, &chunk(&1, &2, chunk_byte_size, byte_size_function), &after_chunk/1)
  end

  @doc """
  Return the number of partitions for a given topic. Bypasses the need for a persistent client
  for lighter weight interactions from one-off calls.

  Note that when passing in a connection/pid for the first argument, the underlying brod calls
  will cache the results, including the non-existence of a topic!  So if this function is being
  used to wait for a recently created topic to be fully online, pass in a list of endpoints
  rather than a connection.  For convenience, the `get_endpoints/1` is provided to extract
  the endpoints from a connection.
  """
  @spec partition_count(list() | Elsa.connection() | pid(), String.t(), RetryConfig.t()) ::
          {:ok, integer()} | {:error, any()}
  def partition_count(connection_or_endpoints, topic, retry_config \\ RetryConfig.no_retry())

  def partition_count(_client_or_endpoints, _topic, %RetryConfig{tries: 0}) do
    {:error, :out_of_tries}
  end

  def partition_count(endpoints, topic, %RetryConfig{} = retry_config) when is_list(endpoints) do
    case :brod.get_metadata(reformat_endpoints(endpoints), [topic]) do
      {:error, reason} -> maybe_retry(reason, endpoints, topic, retry_config)
      {:ok, metadata} -> {:ok, count_partitions(metadata)}
    end
  end

  def partition_count(client, topic, %RetryConfig{} = retry_config) when is_atom(client) or is_pid(client) do
    case :brod_client.get_partitions_count(client, topic) do
      {:error, reason} -> maybe_retry(reason, client, topic, retry_config)
      success -> success
    end
  end

  @doc """
  Like `partition_count/3` but returns only the partition count, and raises on failure
  """
  @spec partition_count!(list() | Elsa.connection() | pid, String.t(), RetryConfig.t()) :: integer()
  def partition_count!(connection_or_endpoints, topic, retry_config \\ RetryConfig.no_retry())

  def partition_count!(connection_or_endpoints, topic, %RetryConfig{} = retry_config) do
    case partition_count(connection_or_endpoints, topic, retry_config) do
      {:ok, partition_count} ->
        partition_count

      {:error, error} ->
        raise "#{__MODULE__}.partition_count! failed: #{error}"
    end
  end

  @spec get_endpoints(Elsa.connection()) :: {:ok, list()} | {:error, any}
  def get_endpoints(connection) do
    case :brod_client.get_bootstrap(connection) do
      {:ok, {endpoints, _}} -> {:ok, endpoints}
      {:ok, endpoints} -> {:ok, endpoints}
      error -> error
    end
  end

  defp maybe_retry(reason, endpoints_or_client, topic, retry_config) do
    Logger.info(
      "#{__MODULE__}: error getting partition metadata for #{topic}: #{reason}. #{retry_config.tries - 1} tries remaining."
    )

    :timer.sleep(retry_config.dwell_ms)
    partition_count(endpoints_or_client, topic, RetryConfig.decrement(retry_config))
  end

  # Handle brod < 3.16
  defp count_partitions(%{topic_metadata: topic_metadatas}) do
    [count | _] = for %{partition_metadata: metadata} <- topic_metadatas, do: Enum.count(metadata)
    count
  end

  # Handle brod 3.16+
  defp count_partitions(%{topics: topics}) do
    [count | _] = for %{partitions: partitions} <- topics, do: Enum.count(partitions)
    count
  end

  defp connect(endpoints, :controller), do: :kpro.connect_controller(endpoints, [])
  defp connect(endpoints, _type), do: :kpro.connect_any(endpoints, [])

  defp do_with_connection({:ok, connection}, fun) do
    fun.(connection)
  after
    :kpro.close_connection(connection)
  end

  defp do_with_connection({:error, reason}, _fun) do
    raise Elsa.ConnectError, message: format_reason(reason)
  end

  defp format_reason(reason) do
    cond do
      is_binary(reason) -> reason
      Exception.exception?(reason) -> Exception.format(:error, reason)
      true -> inspect(reason)
    end
  end

  defp chunk(item, {current_size, current_batch}, chunk_byte_size, byte_size_function) do
    item_size = byte_size_function.(item) + @timestamp_size_in_bytes
    new_total = current_size + item_size

    case new_total < chunk_byte_size do
      true -> add_item_to_batch(new_total, item, current_batch)
      false -> finish_batch(item_size, item, current_batch)
    end
  end

  defp add_item_to_batch(total, item, batch) do
    {:cont, {total, [item | batch]}}
  end

  defp finish_batch(total, item, batch) do
    {:cont, Enum.reverse(batch), {total, [item]}}
  end

  defp after_chunk({_size, []}) do
    {:cont, {0, []}}
  end

  defp after_chunk({_size, current_batch}) do
    finish_batch(0, nil, current_batch)
  end

  defp get_byte_size(%{key: key, value: value} = msg) do
    header_size =
      Map.get(msg, :headers, [])
      |> Enum.map(fn {key, value} -> byte_size(key) + byte_size(value) end)
      |> Enum.sum()

    byte_size(key) + byte_size(value) + header_size
  end
end
