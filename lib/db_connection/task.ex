defmodule DBConnection.Task do
  @moduledoc false
  @name __MODULE__

  def start_link() do
    Task.Supervisor.start_link(name: @name)
  end

  def start_link(_, _) do
    raise ArgumentError, "can not start the DBConnection.Task pool"
  end

  def child_spec(_, _, _) do
    raise ArgumentError,
      "can not create a child spec for the DBConnection.Task pool"
  end

  def run_child(mod, fun, state, opts) do
    ref = make_ref()
    arg = [mod, fun, ref, self(), state, opts]
    {:ok, pid} = Task.Supervisor.start_child(__MODULE__, __MODULE__, :init, arg)
    mon = Process.monitor(pid)
    send(pid, {:go, ref, mon})
    {pid, mon}
  end

  def init(mod, fun, ref, conn, state, opts) do
    try do
      Process.link(conn)
    catch
      :error, :noproc ->
        exit({:shutdown, :noproc})
    end
    receive do
      {:go, ^ref, mon} ->
        Process.unlink(conn)
        pool = {:via, __MODULE__, {{conn, mon}, mod, state}}
        _ = DBConnection.run(pool, make_fun(fun), [holder: __MODULE__] ++ opts)
        exit(:normal)
    end
  end

  def checkout({:via, __MODULE__, {info, mod, state}}, _) do
    {:ok, info, mod, state}
  end

  defdelegate checkin(info, state), to: DBConnection.Connection
  defdelegate disconnect(info, err, state), to: DBConnection.Connection
  defdelegate stop(info, err, state), to: DBConnection.Connection

  defp make_fun(fun) when is_function(fun, 1) do
    fun
  end
  defp make_fun(mfargs) do
    fn(conn) ->
      {mod, fun, args} = mfargs
      apply(mod, fun, [conn | args])
    end
  end
end
