defmodule Penny do
  import Plug.Conn

  def handle_output({:timeout, fun}, assigns) when is_function(fun, 1),
    do: fun.(assigns)

  def handle_output({:error, fun}, assigns) when is_function(fun, 1),
    do: fun.(assigns)

  def handle_output({:ok, result, fun}, assigns) when is_function(fun, 2),
    do: fun.(result, assigns)

  def handle_output(value, _assigns),
    do: value

  @spec send_chunks(Plug.Conn.t(), Enumerable.t()) :: Plug.Conn.t()
  def send_chunks(conn, chunks, options \\ []) do
    conn = send_chunked(conn, 200)

    chunks
    |> async_stream(options)
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

  def mark_async(fun, options \\ []) do
    struct(Penny.MarkedAsync, options ++ [mfa: fun])
  end

  def async_stream(enumerable, options \\ [])
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
end
