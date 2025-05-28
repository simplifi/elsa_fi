defmodule Elsa.TopicTest do
  use ExUnit.Case

  import Mock

  alias Elsa.Util

  require Elsa.Topic, as: Topic

  @endpoints Application.compile_env(:elsa_fi, :brokers)

  describe "create_topic/3" do
    test "returns error tuple when topic fails to get created" do
      with_mocks([
        {Util, [:passthrough], [get_api_version: fn _, :create_topics -> :version end]},
        {:kpro_req_lib, [:passthrough], [create_topics: fn _, _, _ -> :topic_request end]},
        {:kpro, [:passthrough],
         [
           request_sync: fn _, _, _ -> {:error, "some failure"} end,
           connect_controller: fn _, _ -> {:ok, "connected"} end
         ]}
      ]) do
        internal_result = Elsa.create_topic(@endpoints, "topic-to-create")

        assert {:error, "some failure"} == internal_result
      end
    end

    test "return error tuple when topic response contains an error" do
      message = %{
        topics: [
          %{
            error_code: :topic_already_exists,
            error_message: "Topic 'elsa-topic' already exists.",
            name: "elsa-topic"
          }
        ]
      }

      kpro_rsp = Topic.kpro_rsp(api: :create_topics, vsn: 2, msg: message)

      with_mocks([
        {Util, [:passthrough], [get_api_version: fn _, :create_topics -> :version end]},
        {:kpro_req_lib, [:passthrough], [create_topics: fn _, _, _ -> :topic_request end]},
        {:kpro, [:passthrough],
         [request_sync: fn _, _, _ -> {:ok, kpro_rsp} end, connect_controller: fn _, _ -> {:ok, "connected"} end]}
      ]) do
        internal_result = Elsa.create_topic(@endpoints, "elsa-topic")

        assert {:error, {:topic_already_exists, "Topic 'elsa-topic' already exists."}} == internal_result
      end
    end
  end

  describe "delete_topic/2" do
    test "return error tuple when topic response contains an error" do
      message = %{
        responses: [
          %{
            error_code: :topic_doesnt_exist,
            name: "elsa-topic"
          }
        ]
      }

      kpro_rsp = Topic.kpro_rsp(api: :delete_topics, vsn: 2, msg: message)

      with_mocks([
        {Util, [:passthrough], [get_api_version: fn _, :delete_topics -> :version end]},
        {:kpro_req_lib, [:passthrough], [delete_topics: fn _, _, _ -> :topic_request end]},
        {:kpro, [:passthrough],
         [request_sync: fn _, _, _ -> {:ok, kpro_rsp} end, connect_controller: fn _, _ -> {:ok, "connected"} end]}
      ]) do
        internal_result = Elsa.delete_topic(@endpoints, "elsa-topic")

        assert {:error, {:topic_doesnt_exist, :delete_topic_error}} == internal_result
      end
    end
  end

  describe "list_topics/1" do
    test "extracts topics and partitions as a list of tuples" do
      metadata = %{
        topics: [
          %{
            partitions: [%{partition: 0}],
            name: "elsa-other-topic"
          },
          %{
            partitions: [%{partition: 0}, %{partition: 1}],
            name: "elsa-topic"
          }
        ]
      }

      with_mock(:brod, get_metadata: fn _, :all -> {:ok, metadata} end) do
        assert Elsa.list_topics(@endpoints) == {:ok, [{"elsa-other-topic", 1}, {"elsa-topic", 2}]}
      end
    end

    test "returns error tuple if error is thrown from brod" do
      with_mock(:brod, get_metadata: fn _, :all -> {:error, "Ops"} end) do
        result = Elsa.list_topics(@endpoints)

        assert {:error, %MatchError{term: {:error, "Ops"}}} == result
      end
    end
  end

  describe "exists?/2" do
    test "returns a boolean identifying the presence of a given topic" do
      metadata = %{
        topics: [
          %{
            partitions: [%{partition: 0}],
            name: "elsa-other-topic"
          },
          %{
            partitions: [%{partition: 0}, %{partition: 1}],
            name: "elsa-topic"
          }
        ]
      }

      with_mock(:brod, get_metadata: fn _, :all -> {:ok, metadata} end) do
        assert Topic.exists?(@endpoints, "elsa-other-topic") == true
        assert Topic.exists?(@endpoints, "missing-topic") == false
      end
    end
  end
end
