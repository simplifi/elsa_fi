defmodule Elsa.Consumer.MessageHandler do
  @moduledoc """
  Define the behaviour and default implementations of functions
  for creating message handlers that will be called by Elsa worker
  processes.

  ## Return values

    * `:ack` / `:acknowledge` - commit the last offset of the batch (or the offset
      given with `{:ack, offset}`) and keep consuming.
    * `:no_ack` / `:noop` - do not ack, and do NOT redeliver. Consumption continues
      with later batches, and a later ack commits past this batch, so it will be
      skipped unless you ack it yourself.
    * `{:retry, offset}` - do not ack; rewind the consumer to `offset` and deliver
      from there again, in order, before anything newer. Use the first offset of the
      batch to redeliver all of it. Retrying repeatedly holds the partition at that
      offset, so bound the attempts (and back off) in your handler state.
    * `:continue` - keep consuming without committing to the consumer group.
  """
  @callback init(term()) :: {:ok, term()}

  @callback handle_messages(term(), term()) ::
              {:acknowledge, term()}
              | {:acknowledge, term(), term()}
              | {:ack, term()}
              | {:ack, term(), term()}
              | {:no_ack, term()}
              | {:noop, term()}
              | {:retry, non_neg_integer(), term()}
              | {:continue, term()}

  @callback handle_messages(term()) ::
              :ack
              | :acknowledge
              | {:ack, term()}
              | {:acknowledge, term()}
              | :no_ack
              | :noop
              | {:retry, non_neg_integer()}
              | :continue

  @doc """
  Defines the macro for implementing the message handler behaviour
  in an application. Default implementations allow injecting of
  configuration into the worker process, persisting state between runs
  of the message handler function, or alternatively, basic processing
  and acknowlegement of messages.
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour Elsa.Consumer.MessageHandler

      @impl Elsa.Consumer.MessageHandler
      def init(args) do
        {:ok, args}
      end

      @impl Elsa.Consumer.MessageHandler
      def handle_messages(messages, state) do
        case handle_messages(messages) do
          :ack -> {:ack, state}
          :acknowledge -> {:acknowledge, state}
          {:ack, offset} -> {:ack, offset, state}
          {:acknowledge, offset} -> {:acknowledge, offset, state}
          :no_ack -> {:no_ack, state}
          :noop -> {:noop, state}
          {:retry, offset} -> {:retry, offset, state}
          :continue -> {:continue, state}
        end
      end

      @impl Elsa.Consumer.MessageHandler
      def handle_messages(messages) do
        :ack
      end

      def topic do
        Process.get(:elsa_topic)
      end

      def partition do
        Process.get(:elsa_partition)
      end

      def generation_id do
        Process.get(:elsa_generation_id)
      end

      def connection do
        Process.get(:elsa_connection)
      end

      defoverridable Elsa.Consumer.MessageHandler
    end
  end
end
