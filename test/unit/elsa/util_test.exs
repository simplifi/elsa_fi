defmodule Elsa.UtilTest do
  use ExUnit.Case

  import Checkov
  import Mock

  alias Elsa.Util

  @endpoints Application.compile_env(:elsa_fi, :brokers)

  describe "with_connection/2" do
    test "runs function with connection" do
      with_mock(:kpro,
        connect_any: fn _, _ -> {:ok, :connection} end,
        close_connection: fn _ -> :ok end
      ) do
        result =
          Util.with_connection(@endpoints, fn connection ->
            assert :connection == connection
            :return_value
          end)

        assert :return_value == result
        assert_called_exactly(:kpro.connect_any(Util.reformat_endpoints(@endpoints), []), 1)
        assert_called_exactly(:kpro.close_connection(:connection), 1)
      end
    end

    test "runs function with controller connection" do
      :meck.new(:kpro, [:passthrough])
      :meck.expect(:kpro, :connect_controller, 2, {:ok, :connection})
      :meck.expect(:kpro, :close_connection, 1, :ok)

      result =
        Util.with_connection(@endpoints, :controller, fn connection ->
          assert :connection == connection
          :return_value
        end)

      assert :return_value == result
      assert_called_exactly(:kpro.connect_controller(Util.reformat_endpoints(@endpoints), []), 1)
      assert_called_exactly(:kpro.close_connection(:connection), 1)

      :meck.unload(:kpro)
    end

    test "calls close_connection when fun raises an error" do
      with_mock(:kpro,
        connect_any: fn _, _ -> {:ok, :connection} end,
        close_connection: fn _ -> :ok end
      ) do
        try do
          Util.with_connection(@endpoints, fn _connection ->
            raise "some error"
          end)

          flunk("Should have raised error")
        rescue
          e in RuntimeError -> assert Exception.message(e) == "some error"
        end

        assert_called_exactly(:kpro.close_connection(:connection), 1)
      end
    end

    data_test "raises exception when unable to create connection" do
      with_mock(:kpro, connect_any: fn _, _ -> {:error, reason} end) do
        assert_raise(Elsa.ConnectError, message, fn ->
          Util.with_connection(@endpoints, fn _connection -> nil end)
        end)

        assert_not_called(:kpro.close_connection(:_))
      end

      where([
        [:reason, :message],
        ["unable to connect", "unable to connect"],
        [{:econnrefused, [1, 2]}, inspect({:econnrefused, [1, 2]})],
        [RuntimeError.exception("jerks"), Exception.format(:error, RuntimeError.exception("jerks"))]
      ])
    end
  end

  describe "chunk_by_byte_size" do
    test "will create chunks that are less then supplied chunk_byte_size" do
      chunks =
        ?a..?z
        |> Enum.map(&to_message/1)
        |> Util.chunk_by_byte_size(10 * 10)

      assert length(chunks) == 3
      assert Enum.at(chunks, 0) == ?a..?i |> Enum.map(&to_message/1)
      assert Enum.at(chunks, 1) == ?j..?r |> Enum.map(&to_message/1)
      assert Enum.at(chunks, 2) == ?s..?z |> Enum.map(&to_message/1)
    end

    test "will create chunks of for {key, value} pairs" do
      chunks =
        ?a..?z
        |> Enum.map(&to_message(&1, key: true))
        |> Util.chunk_by_byte_size(20 + 10 * 10)

      assert length(chunks) == 3
      assert Enum.at(chunks, 0) == ?a..?i |> Enum.map(&to_message(&1, key: true))
      assert Enum.at(chunks, 1) == ?j..?r |> Enum.map(&to_message(&1, key: true))
      assert Enum.at(chunks, 2) == ?s..?z |> Enum.map(&to_message(&1, key: true))
    end
  end

  defp to_message(char, opts \\ []) do
    string = List.to_string([char])

    key =
      case Keyword.get(opts, :key, false) do
        true -> string
        false -> ""
      end

    %{key: key, value: string}
  end
end
