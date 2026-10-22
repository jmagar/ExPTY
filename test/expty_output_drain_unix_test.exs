defmodule ExPTY.OutputDrainUnixTest do
  @moduledoc """
  Protects final output delivery from real Unix PTYs.

  The native reader supplies closure and the ExPTY server owns callback order.
  These tests cover empty output, multiple native reads, cancellation, and
  inherited terminals; callback-order unit tests cover both event interleavings.
  """
  use ExUnit.Case, async: false

  @moduletag skip: not match?({:unix, _}, :os.type())
  @command_timeout 5_000
  @terminal_holder """
  trap '' HUP
  (
    exec 3<"$1"
    printf ready
    cat <&3
  ) &
  IFS= read -r command
  printf final
  exit 23
  """

  test "drained exit follows all output, including empty and multi-chunk output" do
    for expected <- ["", "final output", String.duplicate("x", 16_384)] do
      {_pty, ref} = spawn_drained(["-c", "printf %s \"$1\"", "output-test", expected])
      assert {^expected, 0, 0} = receive_exit(ref)
    end
  end

  test "cancellation closes the reader and delivers the process signal" do
    {pty, ref} = spawn_drained(["-c", "printf ready; exec sleep 60"])
    assert_output(ref, "ready")

    assert :ok = ExPTY.kill(pty, 15)
    assert {"", 0, 15} = receive_exit(ref)
  end

  test "live output larger than the exit budget remains complete" do
    {_pty, ref} =
      spawn_drained(["-c", "dd if=/dev/zero bs=1024 count=1024 2>/dev/null; printf final"])

    expected = :binary.copy(<<0>>, 1_048_576) <> "final"
    assert {^expected, 0, 0} = receive_exit(ref)
  end

  @tag :tmp_dir
  @tag skip: not match?({:unix, :linux}, :os.type())
  test "drains an inherited terminal released within the bound", %{
    tmp_dir: tmp_dir
  } do
    fifo = Path.join(tmp_dir, "release")
    assert {_, 0} = System.cmd("mkfifo", [fifo], stderr_to_stdout: true)
    assert {:ok, release} = File.open(fifo, [:read, :write])

    on_exit(fn ->
      File.close(release)
      File.rm(fifo)
    end)

    {pty, ref} = spawn_drained(["-c", @terminal_holder, "terminal-holder", fifo])
    assert_output(ref, "ready")

    assert :ok = ExPTY.write(pty, "exit\n")
    assert_output(ref, "final")
    refute_received {^ref, :exit, _, _}

    assert :ok = File.close(release)
    assert {"", 23, 0} = receive_exit(ref)
  end

  @tag :tmp_dir
  @tag skip: not match?({:unix, :linux}, :os.type())
  test "quiet retained slave settles unsuccessfully within one second", %{tmp_dir: tmp_dir} do
    {pty, ref, release} = retained_slave(tmp_dir, "cat <&3")
    started = System.monotonic_time(:millisecond)
    assert :ok = ExPTY.write(pty, "exit\n")
    assert {"final", 1, 0} = receive_exit(ref)
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed >= 900
    assert elapsed < 2_500
    File.close(release)
    refute_receive {^ref, :data, _}, 50
    refute_receive {^ref, :exit, _, _}, 0
  end

  @tag :tmp_dir
  @tag skip: not match?({:unix, :linux}, :os.type())
  test "continuous retained output stops at the post-exit byte bound", %{tmp_dir: tmp_dir} do
    # The descendant cannot write until the direct child has printed its final
    # marker and exited. All bulk output is therefore post-exit drain traffic.
    {pty, ref, release} = retained_slave(tmp_dir, "read token <&3; exec yes x")
    child = :sys.get_state(pty).pid
    assert :ok = ExPTY.write(pty, "exit\n")
    assert_output(ref, "final")

    assert Enum.any?(1..100, fn _ ->
             Process.sleep(5)

             case File.read("/proc/#{child}/stat") do
               {:error, :enoent} -> true
               {:ok, stat} -> Regex.match?(~r/\) Z /, stat)
             end
           end),
           "direct child did not exit before descendant output"

    IO.write(release, "go\n")
    {output, 1, 0} = receive_exit(ref)
    assert byte_size(output) == 512_000
    refute File.exists?("/proc/#{child}"), "original child was not finally reaped"
    File.close(release)
    refute_receive {^ref, :data, _}, 50
  end

  @tag :tmp_dir
  @tag skip: not match?({:unix, :linux}, :os.type())
  test "native launch and drain errors retain child cleanup and unsuccessful status", %{
    tmp_dir: tmp_dir
  } do
    app = Path.join(tmp_dir, "expty")
    File.mkdir_p!(Path.join(app, "priv"))
    File.cp_r!(Path.join(to_string(:code.lib_dir(:expty)), "ebin"), Path.join(app, "ebin"))
    File.cp!(ExPTY.Nif.helper_path(), Path.join(app, "priv/spawn-helper"))

    include =
      Path.join([to_string(:code.root_dir()), "erts-#{:erlang.system_info(:version)}", "include"])

    assert {_, 0} =
             System.cmd(
               "c++",
               [
                 "-shared",
                 "-fPIC",
                 "-std=c++14",
                 "-pthread",
                 "-I#{include}",
                 "-Ic_src",
                 "test/fixtures/native_fault.cpp",
                 "-o",
                 Path.join(app, "priv/expty.so")
               ],
               stderr_to_stdout: true
             )

    script = ~S'''
    Process.flag(:trap_exit, true)
    {:ok, _} = ExPTY.Application.start(:normal, [])
    owner = self()
    mode = System.fetch_env!("EXPTY_NATIVE_FAULT")
    fds_before = length(File.ls!("/proc/self/fd"))
    {args, release} = if mode in ["short-read", "custody", "straddle"] do
      fifo = System.fetch_env!("EXPTY_FAULT_PID")
      {_, 0} = System.cmd("mkfifo", [fifo])
      {:ok, release} = File.open(fifo, [:read, :write])
      command = if mode in ["short-read", "straddle"],
        do: "trap '' HUP; (read token < \"$1\"; exec yes x) & exit 0",
        else: "trap '' HUP; (exec 3< \"$1\"; printf ready; cat <&3) & exit 0"
      command = if mode == "straddle" do
        {_, 0} = System.cmd("mkfifo", [fifo <> ".exit"])
        {:ok, exit_release} = File.open(fifo <> ".exit", [:read, :write])
        Process.put(:exit_release, exit_release)
        "trap '' HUP; (read token < \"$1\"; exec yes x) & printf ready; read token < \"$2\"; exit 0"
      else
        command
      end
      {["-c", command, "drain-control", fifo, fifo <> ".exit"], release}
    else
      {["-c", "exec sleep 0.1"], nil}
    end
    result = ExPTY.spawn("/bin/sh", args,
      drain_on_exit: true,
      on_data: fn _, _, data ->
        Process.put(:bytes, Process.get(:bytes, 0) + byte_size(data))
        send(owner, {:data, data})
      end,
      on_exit: fn _, _, c, s -> send(owner, {:done, c, s, Process.get(:bytes, 0)}) end)
    if mode in ["reader", "waiter"] do
      pid = File.read!(System.fetch_env!("EXPTY_FAULT_PID"))
      {_, alive} = System.cmd("kill", ["-0", pid], stderr_to_stdout: true)
      unless match?({:error, _}, result) and alive != 0, do: System.halt(31)
      unless length(File.ls!("/proc/self/fd")) == fds_before, do: System.halt(34)
    else
      if mode == "straddle" do
        {:ok, pty} = result
        child = :sys.get_state(pty).pid
        path = System.fetch_env!("EXPTY_FAULT_PID")
        unless Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          File.exists?(path <> ".reading")
        end), do: System.halt(47)
        IO.write(Process.get(:exit_release), "exit\n")
        unless Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          case File.read("/proc/#{child}/stat") do
            {:error, :enoent} -> true
            {:ok, stat} -> Regex.match?(~r/\) Z /, stat)
          end
        end), do: System.halt(48)
        IO.write(release, "go\n")
        File.write!(path <> ".continue", "read after exit")
      end
      if mode == "inflight" do
        {:ok, pty} = result
        child = :sys.get_state(pty).pid
        path = System.fetch_env!("EXPTY_FAULT_PID")
        killer = Task.async(fn -> ExPTY.kill(pty, 10) end)
        unless Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          File.exists?(path <> ".signaling")
        end), do: System.halt(45)
        unless Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          case File.read("/proc/#{child}/stat") do
            {:error, :enoent} -> true
            {:ok, stat} -> Regex.match?(~r/\) Z /, stat)
          end
        end), do: System.halt(46)
        early_reap = Enum.any?(1..100, fn _ ->
          Process.sleep(2)
          File.exists?(path <> ".reaped")
        end)
        File.write!(path <> ".continue", "release signal")
        :ok = Task.await(killer, 3_000)
        if early_reap, do: IO.puts("reaped while original signal was still in flight")
        Process.put(:early_reap, early_reap)
      end
      if mode == "custody" do
        {:ok, pty} = result
        path = System.fetch_env!("EXPTY_FAULT_PID")
        receive do
          {:data, "ready"} -> :ok
        after
          3_000 -> System.halt(41)
        end
        unless Enum.any?(1..500, fn _ ->
          Process.sleep(5)
          File.exists?(path <> ".reaped")
        end), do: System.halt(42)
        :ok = ExPTY.kill(pty, 10)
        :ok = File.close(release)
        unless Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          :sys.get_state(pty).output_closed
        end), do: System.halt(43)
        File.write!(path <> ".continue", "release")
      end
      if mode == "short-read" do
        {:ok, pty} = result
        child = :sys.get_state(pty).pid
        exited = Enum.any?(1..100, fn _ ->
          Process.sleep(5)
          case File.read("/proc/#{child}/stat") do
            {:error, :enoent} -> true
            {:ok, stat} -> Regex.match?(~r/\) Z /, stat)
          end
        end)
        unless exited, do: System.halt(35)
        IO.write(release, "go\n")
      end
      receive do
        {:done, 0, 0, 0} when mode == "inflight" -> :ok
        {:done, 1, 0, bytes} when mode != "inflight" and (mode not in ["short-read", "straddle"] or bytes == 512_000) -> :ok
        {:done, _, _, _} = other ->
          IO.inspect(other)
          path = System.fetch_env!("EXPTY_FAULT_PID") <> ".signals"
          if File.exists?(path), do: IO.puts(File.read!(path))
          System.halt(32)
      after
        3_000 -> System.halt(33)
      end
      if release, do: File.close(release)
      if Process.get(:exit_release), do: File.close(Process.get(:exit_release))
      if mode in ["short-read", "custody", "straddle"] do
        pty = elem(result, 1)
        :ok = ExPTY.kill(pty, 10)
        if File.exists?("/proc/#{:sys.get_state(pty).pid}"), do: System.halt(37)
      end
    end
    signals = System.fetch_env!("EXPTY_FAULT_PID") <> ".signals"
    if File.exists?(signals), do: (IO.puts(File.read!(signals)); System.halt(36))
    if Process.get(:early_reap), do: System.halt(38)
    rewaits = System.fetch_env!("EXPTY_FAULT_PID") <> ".rewaits"
    if File.exists?(rewaits), do: (IO.puts(File.read!(rewaits)); System.halt(39))
    IO.puts("fault-contract-ok")
    '''

    results =
      for fault <- [
            "reader",
            "waiter",
            "waitpid",
            "select",
            "read",
            "short-read",
            "custody",
            "inflight",
            "waitid",
            "straddle"
          ] do
        {output, status} =
          System.cmd(
            "timeout",
            ["--kill-after=1", "8", "elixir", "-pa", Path.join(app, "ebin"), "-e", script],
            env: [
              {"EXPTY_NATIVE_FAULT", fault},
              {"EXPTY_FAULT_PID", Path.join(tmp_dir, fault)},
              {"ERL_FLAGS", "+S 2:2"}
            ],
            stderr_to_stdout: true
          )

        {fault, status, output}
      end

    assert Enum.all?(results, fn {_, status, output} ->
             status == 0 and output =~ "fault-contract-ok"
           end),
           inspect(results, limit: :infinity)
  end

  defp retained_slave(tmp_dir, action) do
    fifo = Path.join(tmp_dir, "release")
    assert {_, 0} = System.cmd("mkfifo", [fifo], stderr_to_stdout: true)
    assert {:ok, release} = File.open(fifo, [:read, :write])
    on_exit(fn -> File.close(release) end)
    holder = String.replace(@terminal_holder, "cat <&3", action)
    {pty, ref} = spawn_drained(["-c", holder, "terminal-holder", fifo])
    assert_output(ref, "ready")
    {pty, ref, release}
  end

  defp spawn_drained(args) do
    owner = self()
    ref = make_ref()

    assert {:ok, pty} =
             ExPTY.spawn("/bin/sh", args,
               drain_on_exit: true,
               on_data: fn _, _, data -> send(owner, {ref, :data, data}) end,
               on_exit: fn _, _, code, signal -> send(owner, {ref, :exit, code, signal}) end
             )

    on_exit(fn ->
      unless :sys.get_state(pty).exit_status == :delivered, do: ExPTY.kill(pty, 9)
      GenServer.stop(pty)
    end)

    {pty, ref}
  end

  defp assert_output(ref, expected, received \\ "")
  defp assert_output(_ref, expected, expected), do: :ok

  defp assert_output(ref, expected, received) do
    assert_receive {^ref, :data, data}, @command_timeout
    received = received <> data
    assert String.starts_with?(expected, received)
    assert_output(ref, expected, received)
  end

  defp receive_exit(ref) do
    receive_exit(ref, [], System.monotonic_time(:millisecond) + @command_timeout)
  end

  defp receive_exit(ref, chunks, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^ref, :data, data} -> receive_exit(ref, [data | chunks], deadline)
      {^ref, :exit, code, signal} -> {IO.iodata_to_binary(Enum.reverse(chunks)), code, signal}
    after
      timeout -> flunk("timed out waiting for drained process exit")
    end
  end
end
