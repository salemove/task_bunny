defmodule TaskBunny do
  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  @json_library Application.compile_env(:task_bunny, :json_library, Poison)

  use Application

  @spec start(atom, term) :: {:ok, pid} | {:ok, pid, any} | {:error, term}
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    # Define workers and child supervisors to be supervised
    children = [
      TaskBunny.Supervisor
    ]

    opts = [strategy: :one_for_one, name: TaskBunny]
    Supervisor.start_link(children, opts)
  end

  def json_library, do: @json_library
end
