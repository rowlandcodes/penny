defmodule Penny do
  import Plug.Conn

  @type result :: term
  @type success_handler :: (result :: result, assigns :: Plug.Conn.assigns() -> Plug.Conn.body())
  @type timeout_handler :: (assigns :: Plug.Conn.assigns() -> Plug.Conn.body())
  @type error_handler :: (assigns :: Plug.Conn.assigns() -> Plug.Conn.body())

  @type marked_option ::
          {:timeout, timeout}
          | {:success_handler, success_handler}
          | {:timeout_handler, timeout_handler}
          | {:error_handler, error_handler}

  @type marked_options :: [marked_option]

  @type send_option ::
          {:timeout, timeout}
          | {:max_concurrency, pos_integer()}

  @type send_options :: [send_option]

  @opaque marked_async :: Penny.MarkedAsync.t()

  @spec send_chunk_stream(
          Plug.Conn.t(),
          Enumerable.t(Plug.Conn.body() | marked_async),
          send_options
        ) ::
          Plug.Conn.t()
  def send_chunk_stream(conn, chunks, options \\ []) do
    chunk_stream = async_stream(chunks, options)
    send_chunk_stream_no_async(conn, chunk_stream)
  end

  @spec send_chunk_stream_no_async(Plug.Conn.t(), Enumerable.t(Plug.Conn.body())) :: Plug.Conn.t()
  def send_chunk_stream_no_async(conn, chunks) do
    conn = send_chunked(conn, 200)

    chunks
    |> Enum.reduce_while(conn, fn chunk, conn ->
      result = handle_output(chunk, conn.assigns)

      case Plug.Conn.chunk(conn, result) do
        {:ok, conn} ->
          {:cont, conn}

        {:error, :closed} ->
          {:halt, conn}
      end
    end)
  end

  @spec mark_async((() -> result), marked_options) :: marked_async
  def mark_async(fun, options \\ []) when is_function(fun, 0) do
    mark_async(:erlang, :apply, [fun, []], options)
  end

  @spec mark_async(module, atom, [term], marked_options) :: marked_async
  def mark_async(module, function_name, args, options \\ [])
      when is_atom(module) and is_atom(function_name) and is_list(args) do
    timeout = Keyword.get(options, :timeout, 5000)
    success_handler = Keyword.get(options, :success_handler, &default_success_handler/2)
    error_handler = Keyword.get(options, :error_handler, &default_error_handler/1)
    timeout_handler = Keyword.get(options, :timeout_handler, &default_timeout_handler/1)

    Penny.MarkedAsync.create(
      {module, function_name, args},
      timeout,
      success_handler,
      error_handler,
      timeout_handler
    )
  end

  defp async_stream(enumerable, options)
       when is_list(options) do
    build_stream(enumerable, options)
  end

  defp build_stream(enumerable, options) do
    fn acc, acc_fun ->
      owner = get_owner(self())

      Penny.Supervised.stream(enumerable, acc, acc_fun, get_callers(self()), options, fn ->
        {:ok, pid} = Penny.Supervised.start(owner)
        {:ok, :nolink, pid}
      end)
    end
  end

  # Returns a tuple with the node where this is executed and either the
  # registered name of the given PID or the PID of where this is executed. Used
  # when exiting from tasks to print out from where the task was started.
  defp get_owner(pid) do
    self_or_name =
      case Process.info(pid, :registered_name) do
        {:registered_name, name} when is_atom(name) -> name
        _ -> pid
      end

    {node(), self_or_name, pid}
  end

  defp get_callers(owner) do
    case :erlang.get(:"$callers") do
      [_ | _] = list -> [owner | list]
      _ -> [owner]
    end
  end

  defp handle_output({:timeout, fun}, assigns) when is_function(fun, 1),
    do: fun.(assigns)

  defp handle_output({:error, fun}, assigns) when is_function(fun, 1),
    do: fun.(assigns)

  defp handle_output({:ok, result, fun}, assigns) when is_function(fun, 2),
    do: fun.(result, assigns)

  defp handle_output(value, _assigns),
    do: value

  defp default_success_handler(value, _assigns) when is_list(value), do: value
  defp default_success_handler(value, _assigns) when is_binary(value), do: value
  defp default_success_handler(value, _assigns), do: to_string(value)

  defp default_error_handler(_), do: ""

  defp default_timeout_handler(_), do: ""
end
