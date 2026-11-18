defmodule Elsa.Consumer.NoAckTest do
  @moduledoc """
  Pins down what `{:no_ack, state}` does; it does not
  stop at the failed batch and it does not redeliver it.

  Scenario (a "busy partition"): 6 waves of 3 messages are produced one after another, so
  each wave arrives as its own batch (offsets 0..17). The first batch containing "poison"
  (in wave 1) fails once and the handler returns `{:no_ack, state}` for it. Later batches
  keep arriving and are acked.

  `:no_ack` only means "I'm not acking this one yet". brod keeps delivering later message
  sets, and acking a later offset commits everything below it, so the failed batch is
  skipped for good. This helps to show what happens with :no_ack, compared to :retry.
  """
  use ExUnit.Case
  use Divo

  alias Elsa.Test.FailedBatchScenario, as: Scenario

  @tag :no_ack
  test "{:no_ack, state} does not redeliver the failed batch; later acks commit past it" do
    r = Scenario.run(:no_ack, "no-ack")

    failed = Scenario.failed_offsets(r.events)
    assert failed != [], "expected one failed batch. " <> r.summary

    # Everything except the failed batch was acked, in order, and the failed offsets were
    # never acked at all...
    assert Scenario.acked_offsets(r.events) == Enum.to_list(0..(Scenario.total() - 1)) -- failed, r.summary

    # ...the failed batch reached the handler exactly once and was never redelivered...
    assert Scenario.handled_count(r.events, failed) == 1, r.summary

    # ...and the broker's committed offset moved PAST it, so even a restarted group would
    # not replay it.
    assert r.committed > Enum.max(failed),
           "expected commit past #{Enum.max(failed)}, got #{inspect(r.committed)}. " <> r.summary
  end
end
