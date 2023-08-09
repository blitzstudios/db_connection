defmodule DBConnection.ConnectionPool do
  @moduledoc false

  use GenServer
  alias DBConnection.ConnectionPool.PoolSupervisor
  alias DBConnection.Holder

  @queue_target 50
  @queue_interval 1000
  @idle_interval 1000
  @time_unit 1000

  ## child_spec API

  @doc false
  def child_spec(args) do
    Supervisor.Spec.worker(__MODULE__, [args])
  end

  @doc false
  def start_link({mod, opts}) do
    GenServer.start_link(__MODULE__, {mod, opts}, start_opts(opts))
  end

  def get_metrics(pid) do
    GenServer.call(pid, :get_metrics)
  end

  ## GenServer api

  def init({mod, opts}) do
    queue = :ets.new(__MODULE__.Queue, [:protected, :ordered_set])
    {:ok, _} = PoolSupervisor.start_pool(queue, mod, opts)
    target = Keyword.get(opts, :queue_target, @queue_target)
    interval = Keyword.get(opts, :queue_interval, @queue_interval)
    idle_interval = Keyword.get(opts, :idle_interval, @idle_interval)
    now = System.monotonic_time(@time_unit)
    codel = %{target: target, interval: interval, delay: 0, slow: false,
              next: now, poll: nil, idle_interval: idle_interval, idle: nil}
    codel = start_idle(now, now, start_poll(now, now, codel))
    {:ok, {:busy, queue, codel, %{
      active_connections: 0,
      waiting_connections: 0
    }}}
  end

  def handle_call(:get_metrics, _from, {_, _, _, metrics} = state) do
    {:reply, metrics, state}
  end

  def handle_info({:db_connection, from, {:checkout, _caller, now, queue?}}, {:busy, queue, codel, metrics} = busy) do
    case queue? do
      true ->
        :ets.insert(queue, {{now, System.unique_integer(), from}})
        {:noreply, {:busy, queue, codel, %{metrics | waiting_connections: metrics.waiting_connections + 1}}}
      false ->
        message = "connection not available and queuing is disabled"
        err = DBConnection.ConnectionError.exception(message)
        Holder.reply_error(from, err)
        {:noreply, busy}
    end
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, _queue?}} = checkout, ready) do
    {:ready, queue, _codel, metrics} = ready
    case :ets.first(queue) do
      {_time, holder} = key ->
        Holder.handle_checkout(holder, from, queue) and :ets.delete(queue, key)
        {:noreply, {:ready, queue, _codel, %{metrics | active_connections: metrics.active_connections + 1}}}
      :"$end_of_table" ->
        handle_info(checkout, put_elem(ready, 0, :busy))
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, pid, queue}, {_, queue, _, _metrics} = data) do
    message = "client #{inspect pid} exited"
    err = DBConnection.ConnectionError.exception(message)
    Holder.handle_disconnect(holder, err)
    {:noreply, data}
  end

  def handle_info({:"ETS-TRANSFER", holder, _, {msg, queue, extra}}, {_, queue, _, _metrics} = data) do
    case msg do
      :checkin ->
        owner = self()
        case :ets.info(holder, :owner) do
          ^owner ->
            handle_checkin(holder, extra, data)
          :undefined ->
            {:noreply, data}
        end
      :disconnect ->
        Holder.handle_disconnect(holder, extra)
        {:noreply, data}
      :stop ->
        Holder.handle_stop(holder, extra)
        {:noreply, data}
    end
  end

  def handle_info({:timeout, deadline, {queue, holder, pid, len}}, {_, queue, _, _metrics} = data) do
    # Check that timeout refers to current holder (and not previous)
    if Holder.handle_deadline(holder, deadline) do
      message = "client #{inspect pid} timed out because " <>
        "it queued and checked out the connection for longer than #{len}ms"
      err = DBConnection.ConnectionError.exception(message)
      Holder.handle_disconnect(holder, err)
    end
    {:noreply, data}
  end

  def handle_info({:timeout, poll, {time, last_sent}}, {_, _, %{poll: poll}, metrics} = data) do
    {status, queue, codel, metrics} = data

    # If no queue progress since last poll check queue
    case :ets.first(queue) do
      {sent, _, _} when sent <= last_sent and status == :busy ->
        delay = time - sent
        timeout(delay, time, queue, start_poll(time, sent, codel), metrics)
      {sent, _, _} ->
        {:noreply, {status, queue, start_poll(time, sent, codel), metrics}}
      _ ->
        {:noreply, {status, queue, start_poll(time, time, codel), metrics}}
    end
  end

  def handle_info({:timeout, idle, {time, last_sent}}, {_, _, %{idle: idle}, _metrics} = data) do
    {status, queue, codel, metrics} = data

    # If no queue progress since last idle check oldest connection
    case :ets.first(queue) do
      {sent, holder} = key when sent <= last_sent and status == :ready ->
        :ets.delete(queue, key)
        ping(holder, queue, start_idle(time, last_sent, codel), metrics)
      {sent, _} ->
        {:noreply, {status, queue, start_idle(time, sent, codel), metrics}}
      _ ->
        {:noreply, {status, queue, start_idle(time, time, codel), metrics}}
    end
  end

  defp timeout(delay, time, queue, codel, metrics) do
    case codel do
      %{delay: min_delay, next: next, target: target, interval: interval}
          when time >= next and min_delay > target ->
        codel = %{codel | slow: true, delay: delay, next: time + interval}
        drop_slow(time, target * 2, queue)
        {:noreply, {:busy, queue, codel, %{metrics | waiting_connections: metrics.waiting_connections - 1}}}
      %{next: next, interval: interval} when time >= next ->
        codel = %{codel | slow: false, delay: delay, next: time + interval}
        {:noreply, {:busy, queue, codel, metrics}}
      _ ->
        {:noreply, {:busy, queue, codel, metrics}}
    end
  end

  defp drop_slow(time, timeout, queue) do
    min_sent = time - timeout
    match = {{:"$1", :_, :"$2"}}
    guards = [{:<, :"$1", min_sent}]
    select_slow = [{match, guards, [{{:"$1", :"$2"}}]}]
    for {sent, from} <- :ets.select(queue, select_slow) do
      drop(time - sent, from)
    end
    :ets.select_delete(queue, [{match, guards, [true]}])
  end

  defp ping(holder, queue, codel, metrics) do
    Holder.handle_ping(holder)
    {:noreply, {:ready, queue, codel, metrics}}
  end

  defp handle_checkin(holder, now, {:ready, queue, codel, metrics} = _data) do
    :ets.insert(queue, {{now, holder}})
    {:noreply, {:ready, queue, codel, metrics}}
  end

  defp handle_checkin(holder, now, {:busy, queue, codel, metrics}) do
    # we're checking back in, but the queue is still busy, so we don't we've freed a connection
    # so we can -1 active
    dequeue(now, holder, queue, codel, metrics)
  end


  defp dequeue(time, holder, queue, codel, metrics) do
    # the we're selecting a thing off the queue, so add one to the waiting
    case codel do
      %{next: next, delay: delay, target: target} when time >= next  ->
        dequeue_first(time, delay > target, holder, queue, codel, metrics)
      %{slow: false} ->
        dequeue_fast(time, holder, queue, codel, metrics)
      %{slow: true, target: target} ->
        dequeue_slow(time, target * 2, holder, queue, codel, metrics)
    end
  end

  defp dequeue_first(time, slow?, holder, queue, codel, metrics) do
    %{interval: interval} = codel
    next = time + interval
    case :ets.first(queue) do
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        delay = time - sent
        codel =  %{codel | next: next, delay: delay, slow: slow?}
        go(delay, from, time, holder, queue, codel, metrics)
      :"$end_of_table" ->
        codel = %{codel | next: next, delay: 0, slow: slow?}
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, codel, %{metrics | waiting_connections: metrics.waiting_connections + 1}}}
    end
  end

  defp dequeue_fast(time, holder, queue, codel, metrics) do
    case :ets.first(queue) do
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        go(time - sent, from, time, holder, queue, codel, metrics)
      :"$end_of_table" ->
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, %{codel | delay: 0}, %{metrics | waiting_connections: metrics.waiting_connections + 1}}}
    end
  end

  defp dequeue_slow(time, timeout, holder, queue, codel, metrics) do
    case :ets.first(queue) do
      {sent, _, from} = key when time - sent > timeout ->
        :ets.delete(queue, key)
        drop(time - sent, from)
        dequeue_slow(time, timeout, holder, queue, codel, metrics)
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        go(time - sent, from, time, holder, queue, codel, metrics)
      :"$end_of_table" ->
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, %{codel | delay: 0}, %{metrics | waiting_connections: metrics.waiting_connections + 1}}}
    end
  end

  defp go(delay, from, time, holder, queue, %{delay: min} = codel, metrics) do
    # when we successfully checked out a node, decrement the waiting and increment the active connections
    case Holder.handle_checkout(holder, from, queue) do
      true when delay < min ->
        {:noreply, {:busy, queue, %{codel | delay: delay}, %{metrics | active_connections: metrics.active_connections + 1, waiting_connections: metrics.waiting_connections - 1}}}
      true ->
        {:noreply, {:busy, queue, %{codel | delay: delay}, %{metrics | active_connections: metrics.active_connections + 1, waiting_connections: metrics.waiting_connections - 1}}}
      false ->
        dequeue(time, holder, queue, codel, metrics)
    end
  end

  defp drop(delay, from) do
    message =
      "connection not available and request was dropped from queue after #{delay}ms. " <>
        "You can configure how long requests wait in the queue using :queue_target and " <>
        ":queue_interval. See DBConnection.start_link/2 for more information"
    # drop one from waiting
    err = DBConnection.ConnectionError.exception(message)
    Holder.reply_error(from, err)
  end

  defp start_opts(opts) do
    Keyword.take(opts, [:name, :spawn_opt])
  end

  defp start_poll(now, last_sent, %{interval: interval} = codel) do
    timeout = now + interval
    poll = :erlang.start_timer(timeout, self(), {timeout, last_sent}, [abs: true])
    %{codel | poll: poll}
  end

  defp start_idle(now, last_sent, %{idle_interval: interval} = codel) do
    timeout = now + interval
    idle = :erlang.start_timer(timeout, self(), {timeout, last_sent}, [abs: true])
    %{codel | idle: idle}
  end
end
