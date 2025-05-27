defmodule Elsa.Producer.Initializer do
  @moduledoc false

  alias Elsa.Util

  @spec init(registry :: atom(), producer_configs :: list(keyword)) :: [Supervisor.child_spec()]
  def init(registry, producer_configs) do
    brod_client = Elsa.ElsaRegistry.whereis_name({registry, :brod_client})

    case Keyword.keyword?(producer_configs) do
      true ->
        child_spec(registry, brod_client, producer_configs)

      false ->
        Enum.map(producer_configs, fn pc ->
          child_spec(registry, brod_client, pc)
        end)
        |> List.flatten()
    end
  end

  defp child_spec(registry, brod_client, producer_config) do
    topic = Keyword.fetch!(producer_config, :topic)
    config = Keyword.get(producer_config, :config, [])
    retry_config = Elsa.RetryConfig.new(Keyword.get(producer_config, :metadata_request_config, []))

    # Use the non-connection based partition_count.
    # This circumvents a behavior in brod that caches topics as non-existent,
    # which would break our ability to retry.
    {:ok, endpoints} = Util.get_endpoints(brod_client)
    partitions = Util.partition_count!(endpoints, topic, retry_config)

    0..(partitions - 1)
    |> Enum.map(fn partition ->
      child_spec(registry, brod_client, topic, partition, config)
    end)
  end

  defp child_spec(registry, brod_client, topic, partition, config) do
    name = :"producer_#{topic}_#{partition}"

    wrapper_args = [
      mfa: {:brod_producer, :start_link, [brod_client, topic, partition, config]},
      register: {registry, name}
    ]

    %{
      id: name,
      start: {Elsa.Wrapper, :start_link, [wrapper_args]}
    }
  end
end
