defmodule ExPTY.OutputCompletionTest do
  use ExUnit.Case, async: false

  @moduletag skip: not match?({:unix, _}, :os.type())

  defmodule ReadClosedCallback do
    def on_read_closed(module, pty, result) do
      send(Process.whereis(:expty_read_closed_test), {module, pty, result})
    end
  end

  test "read completion follows every byte even when the process exits immediately" do
    owner = self()
    # More than the native reader's 1024-byte buffer requires multiple callbacks.
    expected = String.duplicate("x", 8192)

    {:ok, pty} =
      ExPTY.spawn("/bin/sh", ["-c", "printf '%8192s' '' | tr ' ' x"],
        on_data: fn _, _, data -> send(owner, {:output, data}) end,
        on_exit: fn _, _, code, signal -> send(owner, {:leader_exit, code, signal}) end,
        on_read_closed: fn _, _, result -> send(owner, {:read_closed, result}) end
      )

    on_exit(fn -> if Process.alive?(pty), do: GenServer.stop(pty) end)

    assert collect_output([]) == expected
    assert_receive {:leader_exit, 0, 0}, 5_000
    refute_received {:output, _}
    refute_received {:read_closed, _}
  end

  test "a command with no output still reports clean stream completion" do
    owner = self()

    {:ok, pty} =
      ExPTY.spawn("/bin/sh", ["-c", "exit 0"],
        on_read_closed: fn _, _, result -> send(owner, {:read_closed, result}) end
      )

    on_exit(fn -> if Process.alive?(pty), do: GenServer.stop(pty) end)
    assert_receive {:read_closed, :eof}, 5_000
  end

  test "preserves a module read-completion callback passed during spawn" do
    Process.register(self(), :expty_read_closed_test)

    {:ok, pty} =
      ExPTY.spawn("/bin/sh", ["-c", "exit 0"], on_read_closed: ReadClosedCallback)

    on_exit(fn -> if Process.alive?(pty), do: GenServer.stop(pty) end)
    assert_receive {ExPTY, ^pty, :eof}, 5_000
  end

  defp collect_output(chunks) do
    receive do
      {:output, data} -> collect_output([data | chunks])
      {:read_closed, :eof} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:read_closed, failure} -> flunk("unexpected reader failure: #{inspect(failure)}")
    after
      5_000 -> flunk("reader did not report output completion")
    end
  end
end
